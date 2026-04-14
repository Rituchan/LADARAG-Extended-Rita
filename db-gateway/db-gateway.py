from flask import Flask, request, jsonify
from pymongo import MongoClient
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct
from bson import ObjectId
from bson.json_util import dumps
from cheroot.wsgi import Server as WSGIServer
import multiprocessing
import uuid
import os
import sys
import logging
import bson
import json
import signal
import re

from sentence_transformers import SentenceTransformer, CrossEncoder


def handle_sigterm(*args):
    sys.exit(0)

signal.signal(signal.SIGTERM, handle_sigterm)

log_file_path = "test.txt"
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file_path, mode='w', encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("app")

app = Flask(__name__)

MONGO_USER = os.environ.get("MONGO_USER", "admin")
MONGO_PASS = os.environ.get("MONGO_PASS", "admin")
MONGO_HOST = os.environ.get("MONGO_HOST", "catalog-data")
MONGO_PORT = os.environ.get("MONGO_PORT", "27017")
MONGO_DB   = os.environ.get("MONGO_DB", "microcks")
MONGO_URI  = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/"

QDRANT_HOST             = os.environ.get("QDRANT_HOST", "catalog-vector")
QDRANT_PORT             = os.environ.get("QDRANT_PORT", "6333")
QDRANT_COLLECTION       = os.environ.get("QDRANT_COLLECTION", "services")
QDRANT_COLLECTION_INDEX = os.environ.get("QDRANT_COLLECTION_INDEX", "services_index")
QDRANT_URI              = f"http://{QDRANT_HOST}:{QDRANT_PORT}"

mongo_client = MongoClient(MONGO_URI)
qdrant_client = QdrantClient(QDRANT_URI)

db         = mongo_client[MONGO_DB]
collection = db["services"]

is_server_ready = False
embedding_model = None
reranker_model  = None
tokenizer       = None


def load_model():
    global embedding_model, reranker_model, tokenizer
    logger.info("Loading models...")
    embedding_model = SentenceTransformer(
        model_name_or_path='Qwen/Qwen3-Embedding-0.6B',
        device='cpu',
        trust_remote_code=True
    )
    tokenizer = embedding_model.tokenizer
    logger.info("Embedding model loaded.")
    reranker_model = CrossEncoder(
        model_name_or_path='cross-encoder/ms-marco-MiniLM-L-6-v2',
        device='cpu',
        trust_remote_code=True
    )
    logger.info("Reranker model loaded.")


def clean_doc(doc):
    doc["_id"] = str(doc["_id"])
    return doc


def embed(input):
    embedding = embedding_model.encode(
        f"query: {input}", convert_to_tensor=False, normalize_embeddings=True
    )
    return embedding.tolist()


def count_tokens(text):
    tokens = tokenizer.encode(text, add_special_tokens=True)
    return len(tokens)


# ─────────────────────────────────────────────────────────────────────────────
# ENRICHED RERANK TEXT
#
# Il CrossEncoder ha scarso overlap lessicale tra query utente e capability text
# perché le capability descrivono comportamenti REST, non concetti del dominio.
# Arricchire il testo con i parametri e i campi del response schema espone
# termini di dominio (es. "temperature", "Celsius", "sensorType") che il
# CrossEncoder può confrontare direttamente con la query.
#
# Il testo arricchito è usato SOLO per il reranking — non viene salvato in
# Qdrant né inviato all'LLM. Il contesto LLM riceve sempre la capability
# originale (più leggibile e concisa).
# ─────────────────────────────────────────────────────────────────────────────

def _build_enriched_text(
    http_op: str,
    capabilities: dict,
    parameters: dict,
    response_schemas: dict,
    request_schemas: dict,
) -> str:
    """
    Costruisce il testo arricchito per il reranking CrossEncoder combinando:
      - capability text (descrizione funzionale dell'endpoint)
      - parameters (nomi e valori enum dei query param)
      - response schema fields (nomi dei campi restituiti)
      - request schema fields (nomi dei campi in input, per i POST/PUT)

    Esempio output per GET /sensor:
      "Retrieve environmental sensors... | parameters: zoneId(Z-CENTRO,...),
       sensorType(temperature,humidity,air_quality,...) | response fields:
       zoneId, sensorType, lastReading, readingUnit, threshold, alertActive"
    """
    parts = [capabilities.get(http_op, "")]

    # ── Parameters ────────────────────────────────────────────────────────────
    param_str = parameters.get(http_op, "") or ""
    if param_str:
        parts.append(f"| parameters: {param_str}")

    # ── Response schema fields ────────────────────────────────────────────────
    resp_str = response_schemas.get(http_op, "") or ""
    if resp_str:
        # Estrae solo i nomi dei campi dallo skeleton "{field:type, ...}" o "[{...}]"
        fields = re.findall(r'(\w+):', resp_str)
        if fields:
            parts.append(f"| response fields: {', '.join(fields)}")

    # ── Request schema fields (solo per metodi con body) ──────────────────────
    req_str = request_schemas.get(http_op, "") or ""
    if req_str and not http_op.startswith("GET"):
        fields = re.findall(r'(\w+):', req_str)
        if fields:
            parts.append(f"| request fields: {', '.join(fields)}")

    return " ".join(parts)



# ─────────────────────────────────────────────────────────────────── parallel
def init_model():
    global model
    model = SentenceTransformer('Qwen/Qwen3-Embedding-0.6B', device='cpu')


def embed_item(args):
    doc_id, key, text = args
    vector    = model.encode(f"query: {text}", normalize_embeddings=True)
    vector_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, text))
    return PointStruct(
        id=vector_id,
        vector=vector.tolist(),
        payload={"mongo_id": doc_id, "http_operation": key}
    )
# ─────────────────────────────────────────────────────────────────── parallel


def create_vector_collection():
    existing_collections = {col.name for col in qdrant_client.get_collections().collections}

    if QDRANT_COLLECTION not in existing_collections:
        qdrant_client.create_collection(
            collection_name=QDRANT_COLLECTION,
            vectors_config=VectorParams(size=1024, distance=Distance.COSINE)
        )
        logger.info(f"Collection '{QDRANT_COLLECTION}' created")
    else:
        logger.info(f"Collection '{QDRANT_COLLECTION}' already exists")

    if QDRANT_COLLECTION_INDEX not in existing_collections:
        qdrant_client.create_collection(
            collection_name=QDRANT_COLLECTION_INDEX,
            vectors_config=VectorParams(size=1024, distance=Distance.COSINE)
        )
        logger.info(f"Collection '{QDRANT_COLLECTION_INDEX}' created")
    else:
        logger.info(f"Collection '{QDRANT_COLLECTION_INDEX}' already exists")


@app.route("/health")
def index():
    if is_server_ready is True:
        return jsonify({"status": "ok", "message": "Gateway Server is ready", "model_loaded": True}), 200
    else:
        logger.error("Model not yet loaded or broken")
        return jsonify({"status": "error", "message": "Model not yet loaded or broken", "model_loaded": False}), 500


@app.route("/index/search", methods=["POST"])
def vector_search():
    """
    Two-stage retrieval pipeline:

    Stage 1 — Bi-encoder su descriptions (services_index):
      Recupera i top-K servizi per similarità semantica sulla description.
      La description cattura il contesto cross-domain del servizio.
      → alta recall: trova i servizi giusti anche per query composte

    Stage 2 — Cross-encoder su capabilities (reranker BAAI/bge-reranker-v2-m3):
      Per ogni servizio recuperato nel Stage 1, carica TUTTI i suoi endpoint
      da MongoDB e li rerankerizza con il CrossEncoder.
      Mantiene sempre i top-N endpoint per score (senza soglia minima).
      → bilanciamento recall/precision costante per ogni servizio selezionato

    Scoring composito (NUOVO):
      Combina s1_score (rilevanza description) e ep_score normalizzato
      (rilevanza endpoint) in un unico punteggio per ordinamento e filtraggio.
      Evita che servizi con ep_score vicino a zero ma s1_score alto
      vengano scartati anche quando sono semanticamente necessari.

    Token budget con trimming graceful (NUOVO):
      Invece di scartare un intero servizio quando non entra nel budget,
      scala progressivamente il numero di endpoint mantenendo sempre
      i top-N per ep_score fino a trovare la configurazione minima che entra.
    """
    data = request.get_json()
    if not data or "query" not in data:
        return jsonify({"error": "Missing 'query' field"}), 400

    query_text      = data["query"]
    query_embedding = embed(query_text)

    # ── Parametri two-stage ───────────────────────────────────────────────────
    STAGE1_K                  = 5
    TOP_ENDPOINTS_PER_SERVICE = 5
    INTELLIGENCE_ID           = "smart-city-intelligence-mock"
    MIN_SCORE_INTELLIGENCE    = 0.60

    # ── Parametri scoring composito ──────────────────────────────────────────
    # ALPHA: peso del s1_score (rilevanza semantica della description)
    # BETA:  peso dell'ep_score normalizzato (rilevanza dell'endpoint migliore)
    # MIN_COMBINED_SCORE: soglia sotto cui un servizio viene scartato.
    #   Calibrata sui dati reali: sensors (necessario) ha combined ~0.19,
    #   buildings (falso positivo) ha combined ~0.15. Soglia a 0.16 taglia
    #   i falsi positivi puri mantenendo i servizi semanticamente rilevanti.
    ALPHA              = 0.3
    BETA               = 0.7
    MIN_COMBINED_SCORE = 0.16

    # ════════════════════════════════════════════════════════════════════════
    # STAGE 1: recupero servizi per similarità sulla description
    # ════════════════════════════════════════════════════════════════════════
    stage1_results = qdrant_client.search(
        collection_name=QDRANT_COLLECTION_INDEX,
        query_vector=query_embedding,
        limit=STAGE1_K
    )

    if not stage1_results:
        logger.warning("[SEARCH] Stage 1: nessun servizio trovato in services_index")
        return jsonify({"results": []}), 200

    stage1_scores = {
        r.payload["mongo_id"]: r.score
        for r in stage1_results
    }

    logger.info(f"[STAGE 1] {len(stage1_scores)} servizi recuperati: "
                f"{list(stage1_scores.keys())}")

    # ════════════════════════════════════════════════════════════════════════
    # STAGE 2: reranking degli endpoint — single-batch per tutti i servizi
    #
    # Invece di chiamare reranker_model.predict() una volta per servizio
    # (N chiamate con ~6 coppie ciascuna), carichiamo prima tutti i servizi
    # da MongoDB, costruiamo tutte le coppie (query, enriched_text) in una
    # lista unica, e facciamo UNA SOLA chiamata a predict().
    # Questo elimina l'overhead di inizializzazione per ogni batch e permette
    # al modello di processare tutte le coppie in modo efficiente.
    # ════════════════════════════════════════════════════════════════════════
    merged: dict = {}

    # ── Fase 2a: carica tutti i servizi da MongoDB ───────────────────────────
    services_data = {}
    for doc_id, s1_score in stage1_scores.items():

        if doc_id == INTELLIGENCE_ID and s1_score < MIN_SCORE_INTELLIGENCE:
            logger.info(f"[STAGE 1] {doc_id} escluso: score {s1_score:.3f} < {MIN_SCORE_INTELLIGENCE}")
            continue

        retrieved = collection.find_one({"_id": doc_id})
        if not retrieved:
            logger.warning(f"[STAGE 2] Servizio '{doc_id}' non trovato in MongoDB")
            continue
        retrieved = bson.json_util.loads(dumps(retrieved))

        capabilities     = retrieved.get("capabilities", {}) or {}
        endpoints        = retrieved.get("endpoints", {}) or {}
        response_schemas = retrieved.get("response_schemas", {}) or {}
        request_schemas  = retrieved.get("request_schemas", {}) or {}
        parameters       = retrieved.get("parameters", {}) or {}

        ops = [op for op in capabilities.keys() if op != "POST /register"]
        if not ops:
            continue

        services_data[doc_id] = {
            "s1_score":       s1_score,
            "retrieved":      retrieved,
            "capabilities":   capabilities,
            "endpoints":      endpoints,
            "response_schemas": response_schemas,
            "request_schemas":  request_schemas,
            "parameters":     parameters,
            "ops":            ops,
        }

    if not services_data:
        return jsonify({"results": []}), 200

    # ── Fase 2b: costruisci tutte le coppie in una lista unica ───────────────
    # pair_map: lista ordinata di (doc_id, op) per ricostruire i risultati
    all_pairs = []
    pair_map  = []  # (doc_id, op) per ogni elemento di all_pairs

    for doc_id, sdata in services_data.items():
        for op in sdata["ops"]:
            enriched = _build_enriched_text(
                op,
                sdata["capabilities"],
                sdata["parameters"],
                sdata["response_schemas"],
                sdata["request_schemas"],
            )
            all_pairs.append((query_text, enriched))
            pair_map.append((doc_id, op))

    logger.info(f"[STAGE 2] Single-batch reranking: {len(all_pairs)} coppie "
                f"da {len(services_data)} servizi")

    # ── Fase 2c: unica chiamata a predict() per tutte le coppie ─────────────
    all_scores = reranker_model.predict(all_pairs)

    # ── Fase 2d: ricostruisci i risultati per servizio ───────────────────────
    # Aggrega gli score per doc_id
    scores_by_service: dict = {doc_id: {} for doc_id in services_data}
    for (doc_id, op), score in zip(pair_map, all_scores):
        scores_by_service[doc_id][op] = float(score)

    for doc_id, sdata in services_data.items():
        s1_score     = sdata["s1_score"]
        capabilities = sdata["capabilities"]
        endpoints    = sdata["endpoints"]
        response_schemas = sdata["response_schemas"]
        request_schemas  = sdata["request_schemas"]
        parameters   = sdata["parameters"]
        ops          = sdata["ops"]

        scored_ops = sorted(
            [(op, scores_by_service[doc_id][op]) for op in ops],
            key=lambda x: x[1],
            reverse=True
        )

        relevant_ops        = scored_ops[:TOP_ENDPOINTS_PER_SERVICE]
        best_endpoint_score = relevant_ops[0][1]

        merged[doc_id] = {
            "_id":              doc_id,
            "name":             sdata["retrieved"].get("name"),
            "description":      sdata["retrieved"].get("description"),
            "capabilities":     {op: capabilities[op] for op, _ in relevant_ops},
            "endpoints":        {op: endpoints.get(op) for op, _ in relevant_ops},
            "response_schemas": {op: response_schemas.get(op) for op, _ in relevant_ops},
            "request_schemas":  {op: request_schemas.get(op) for op, _ in relevant_ops},
            "parameters":       {op: parameters.get(op) for op, _ in relevant_ops},
            "_stage1_score":    s1_score,
            "_best_ep_score":   best_endpoint_score,
        }

        ep_rows = "\n            ".join(
            f"{op:<40} score={score:.4f}" for op, score in relevant_ops
        )
        discarded_ops = [op for op, _ in scored_ops[TOP_ENDPOINTS_PER_SERVICE:]]
        disc_str = f"\n          SCARTATI : {discarded_ops}" if discarded_ops else ""
        logger.info(
            f"[STAGE 2] {doc_id}\n"
            f"          s1={s1_score:.4f} | best_ep={best_endpoint_score:.4f}\n"
            f"          SELEZIONATI ({len(relevant_ops)}/{len(ops)}):\n"
            f"            {ep_rows}"
            f"{disc_str}"
        )

    if not merged:
        return jsonify({"results": []}), 200

    # ════════════════════════════════════════════════════════════════════════
    # SCORING COMPOSITO: combina s1_score e ep_score normalizzato
    #
    # Motivazione: ep_score grezzo non è calibrato in modo assoluto.
    # Per query cross-domain, un servizio può avere s1_score alto (description
    # semanticamente rilevante) ma ep_score vicino a zero (il CrossEncoder
    # non trova match diretto tra query e capability text). Normalizzare
    # ep_score rispetto al best globale e combinarlo con s1_score produce
    # un ranking più stabile che preserva i servizi necessari anche quando
    # il loro endpoint migliore ha score assoluto basso.
    # ════════════════════════════════════════════════════════════════════════
    best_global_ep = max(
        (v["_best_ep_score"] for v in merged.values()), default=0.0
    )
    min_global_ep  = min(
        (v["_best_ep_score"] for v in merged.values()), default=0.0
    )
    ep_range = best_global_ep - min_global_ep

    for s in merged.values():
        # Min-max normalization: funziona anche con logit negativi (ms-marco)
        # Il servizio migliore ottiene sempre ep_norm=1.0, il peggiore 0.0
        ep_norm = (s["_best_ep_score"] - min_global_ep) / ep_range if ep_range > 0 else 0.0
        s["_combined_score"] = ALPHA * s["_stage1_score"] + BETA * ep_norm

    # Filtra servizi sotto la soglia combinata
    pre_filter_count = len(merged)
    merged_with_all  = dict(merged)  # copia pre-filtraggio per il log
    filtered_out     = {k: f"{v['_combined_score']:.3f}" for k, v in merged.items()
                        if v["_combined_score"] < MIN_COMBINED_SCORE}
    merged = {k: v for k, v in merged.items() if v["_combined_score"] >= MIN_COMBINED_SCORE}

    combined_rows = "\n    ".join(
        f"  {sid:<50} s1={v['_stage1_score']:.4f}  ep={v['_best_ep_score']:.4f}  ep_norm={(v['_best_ep_score']-min_global_ep)/ep_range if ep_range>0 else 0:.4f}"
        f"  combined={v['_combined_score']:.4f}  "
        f"[{'PASSA' if v['_combined_score'] >= MIN_COMBINED_SCORE else 'SCARTATO'}]"
        for sid, v in sorted(merged_with_all.items(), key=lambda x: x[1]['_combined_score'], reverse=True)
    )
    logger.info(
        f"[COMBINED_SCORE] formula: {ALPHA}*s1 + {BETA}*ep_norm(min-max) | best_ep={best_global_ep:.4f} | min_ep={min_global_ep:.4f} | soglia={MIN_COMBINED_SCORE}\n"
        f"    {combined_rows}\n"
        f"    → filtrati {pre_filter_count - len(merged)}/{pre_filter_count}: {list(filtered_out.keys())}"
    )

    if not merged:
        return jsonify({"results": []}), 200

    # Ordina per combined_score decrescente
    ordered_services = sorted(
        merged.values(),
        key=lambda x: x["_combined_score"],
        reverse=True
    )
    for s in ordered_services:
        s.pop("_stage1_score", None)
        s.pop("_best_ep_score", None)
        s.pop("_combined_score", None)

    # ════════════════════════════════════════════════════════════════════════
    # TOKEN BUDGET con trimming graceful
    #
    # Per ogni servizio si tenta prima l'inserimento con tutti gli endpoint.
    # Se non ci sta nel budget residuo, si scala progressivamente il numero
    # di endpoint (top-N per ep_score, già ordinati) fino a trovare la
    # configurazione minima che entra. Solo se nemmeno il singolo endpoint
    # migliore entra nel budget, il servizio viene scartato e loggato.
    #
    # Questo garantisce che nessun servizio venga eliminato per intero
    # solo perché il budget era quasi esaurito: almeno il suo endpoint
    # più rilevante arriva sempre al contesto dell'LLM.
    # ════════════════════════════════════════════════════════════════════════
    _DICT_FIELDS = ("capabilities", "endpoints", "response_schemas",
                    "request_schemas", "parameters")

    def _trim_service(s: dict, keep_ops: list) -> dict:
        """Restituisce una copia del servizio con solo gli endpoint in keep_ops."""
        trimmed = {}
        for k, v in s.items():
            if k in _DICT_FIELDS and isinstance(v, dict):
                trimmed[k] = {op: v[op] for op in keep_ops if op in v}
            else:
                trimmed[k] = v
        return trimmed

    max_tokens     = 3500
    current_tokens = 0
    top_results    = []
    budget_log     = []  # (service_id, n_endpoints_inseriti, n_endpoints_totali, nomi_endpoint)

    for s in ordered_services:
        ops = list(s.get("capabilities", {}).keys())

        inserted = False
        for n in range(len(ops), 0, -1):
            candidate     = _trim_service(s, ops[:n])
            candidate_tok = count_tokens(json.dumps(candidate))

            if current_tokens + candidate_tok <= max_tokens:
                top_results.append(candidate)
                current_tokens += candidate_tok
                budget_log.append((s["_id"], n, len(ops), ops[:n]))
                inserted = True
                break

        if not inserted:
            logger.info(
                f"[BUDGET] {s['_id']} escluso: nemmeno il top-1 endpoint "
                f"({count_tokens(json.dumps(_trim_service(s, ops[:1])))} tok) "
                f"entra nel budget residuo ({max_tokens - current_tokens} tok)"
            )

    budget_rows = "\n    ".join(
        f"  {sid:<50} ({n}/{tot} ep) → {ep_names}"
        for sid, n, tot, ep_names in budget_log
    )
    logger.info(
        f"[SEARCH] Stage1={len(stage1_scores)} → Stage2={pre_filter_count} → "
        f"combined_filter={pre_filter_count - len(merged)} scartati → "
        f"{len(top_results)} nel budget | token usati: {current_tokens}/{max_tokens}\n"
        f"    {budget_rows}"
    )
    return jsonify({"results": top_results}), 200


@app.route("/service", methods=["POST"])
def create_or_update_service_old():
    data = request.get_json()
    if not data or "id" not in data:
        return jsonify({"error": "Missing 'id' field"}), 400

    doc_id = data["id"]
    data["_id"] = doc_id
    data.pop("id", None)

    # ── Stage 2 index: 1 vettore per endpoint (capability text) ──────────────
    capabilities = data.get("capabilities", {})
    for http_op, capability in capabilities.items():
        embedding = embed(capability)
        vector_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, capability))
        qdrant_client.upsert(
            collection_name=QDRANT_COLLECTION,
            points=[
                PointStruct(
                    id=vector_id,
                    vector=embedding,
                    payload={"mongo_id": doc_id, "http_operation": http_op}
                )
            ]
        )

    # ── Stage 1 index: 1 vettore per servizio (description text) ─────────────
    description = data.get("description", "")
    if description:
        desc_vector_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"desc:{doc_id}"))
        desc_embedding = embed(description)
        qdrant_client.upsert(
            collection_name=QDRANT_COLLECTION_INDEX,
            points=[
                PointStruct(
                    id=desc_vector_id,
                    vector=desc_embedding,
                    payload={"mongo_id": doc_id}
                )
            ]
        )
        logger.info(f"[INDEX] Service '{doc_id}' indexed in services_index")

    collection.replace_one({"_id": doc_id}, data, upsert=True)
    return jsonify({"status": "ok", "id": doc_id}), 200


# ─────────────────────────────────────────────────────────────────── parallel
@app.route("/service/old", methods=["POST"])
def create_or_update_service():
    data = request.get_json()
    if not data or "id" not in data:
        return jsonify({"error": "Missing 'id' field"}), 400

    doc_id = data["id"]
    data["_id"] = doc_id
    data.pop("id", None)

    capabilities = data.get("capabilities")
    input_data   = [(doc_id, k, v) for k, v in capabilities.items()]

    try:
        with multiprocessing.Pool(initializer=init_model) as pool:
            points = pool.map(embed_item, input_data)
    except Exception as e:
        logger.exception("Embedding failed")
        return jsonify({"error": "Embedding failed", "details": str(e)}), 500

    qdrant_client.upsert(collection_name=QDRANT_COLLECTION, points=points)
    collection.replace_one({"_id": doc_id}, data, upsert=True)
    return jsonify({"status": "ok", "id": doc_id}), 200
# ─────────────────────────────────────────────────────────────────── parallel


@app.route("/services", methods=["GET"])
def list_services():
    docs = list(collection.find())
    return dumps(docs), 200


@app.route("/services/<string:service_id>", methods=["GET"])
def get_service(service_id):
    doc = collection.find_one({"_id": service_id})
    if not doc:
        return jsonify({"error": "Service not found"}), 404
    return dumps(doc), 200


@app.route("/services/<string:service_id>", methods=["DELETE"])
def delete_service(service_id):
    result = collection.delete_one({"_id": service_id})
    if result.deleted_count == 0:
        return jsonify({"error": "Service not found"}), 404
    return jsonify({"status": "deleted", "id": service_id}), 200


@app.route("/index/reindex", methods=["POST"])
def reindex_descriptions():
    """
    Reindicizza le description di tutti i servizi in MongoDB nella collezione
    services_index (Stage 1). Necessario dopo il primo deploy o dopo aver
    aggiunto nuovi servizi quando services_index era vuota.
    """
    docs    = list(collection.find())
    indexed = 0
    skipped = 0
    for doc in docs:
        doc_id      = doc.get("_id")
        description = doc.get("description", "")
        if not description:
            logger.warning(f"[REINDEX] {doc_id}: description vuota, saltato")
            skipped += 1
            continue
        try:
            desc_vector_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"desc:{doc_id}"))
            desc_embedding = embed(description)
            qdrant_client.upsert(
                collection_name=QDRANT_COLLECTION_INDEX,
                points=[
                    PointStruct(
                        id=desc_vector_id,
                        vector=desc_embedding,
                        payload={"mongo_id": doc_id}
                    )
                ]
            )
            logger.info(f"[REINDEX] {doc_id} indicizzato")
            indexed += 1
        except Exception as e:
            logger.error(f"[REINDEX] {doc_id} failed: {e}")
            skipped += 1

    logger.info(f"[REINDEX] Completato: {indexed} indicizzati, {skipped} saltati")
    return jsonify({"indexed": indexed, "skipped": skipped}), 200


@app.route("/services/<string:service_id>/schemas", methods=["PATCH"])
def update_service_schemas(service_id):
    """
    Aggiorna response_schemas, request_schemas e/o parameters di un servizio.
    Chiamato dall'api-importer dopo aver estratto gli schema con prance.
    """
    data = request.get_json()
    update_fields = {}
    if "response_schemas" in data:
        update_fields["response_schemas"] = data["response_schemas"]
    if "request_schemas" in data:
        update_fields["request_schemas"] = data["request_schemas"]
    if "parameters" in data:
        update_fields["parameters"] = data["parameters"]

    if not update_fields:
        return jsonify({"error": "No valid schema fields provided"}), 400

    result = collection.update_one(
        {"_id": service_id},
        {"$set": update_fields}
    )

    if result.matched_count == 0:
        return jsonify({"error": f"Service '{service_id}' not found"}), 404

    logger.info(f"[SCHEMAS] Updated {list(update_fields.keys())} for {service_id}")
    return jsonify({"status": "ok", "id": service_id}), 200


if __name__ == "__main__":
    try:
        with app.app_context():
            logger.info("🛠️ Creating Qdrant collection...")
            create_vector_collection()
            logger.info("📦 Loading embedding model...")
            load_model()
            is_server_ready = True
            logger.info("✅ Server is ready.")
    except Exception as e:
        logger.exception("❌ Failed to initialize application")
        sys.exit(1)

    server = WSGIServer(('0.0.0.0', 5000), app)
    try:
        print("🚀 Starting Flask app with Cheroot on http://0.0.0.0:5000")
        server.start()
    except KeyboardInterrupt:
        print("🛑 Shutting down server...")
        server.stop()