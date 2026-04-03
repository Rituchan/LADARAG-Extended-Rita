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

QDRANT_HOST       = os.environ.get("QDRANT_HOST", "catalog-vector")
QDRANT_PORT       = os.environ.get("QDRANT_PORT", "6333")
QDRANT_COLLECTION = os.environ.get("QDRANT_COLLECTION", "services")
QDRANT_URI        = f"http://{QDRANT_HOST}:{QDRANT_PORT}"

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
    collection_name      = QDRANT_COLLECTION
    existing_collections = qdrant_client.get_collections().collections
    if collection_name not in [col.name for col in existing_collections]:
        qdrant_client.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(size=1024, distance=Distance.COSINE)
        )
        return jsonify({"status": f"Collection '{collection_name}' created"}), 200
    else:
        return jsonify({"status": f"Collection '{collection_name}' already exists"}), 200


@app.route("/health")
def index():
    if is_server_ready is True:
        return jsonify({"status": "ok", "message": "Gateway Server is ready", "model_loaded": True}), 200
    else:
        logger.error("Model not yet loaded or broken")
        return jsonify({"status": "error", "message": "Model not yet loaded or broken", "model_loaded": False}), 500


@app.route("/index/search", methods=["POST"])
def vector_search():
    data = request.get_json()
    if not data or "query" not in data:
        return jsonify({"error": "Missing 'query' field"}), 400

    query_text      = data["query"]
    query_embedding = embed(query_text)

    results = qdrant_client.search(
        collection_name=QDRANT_COLLECTION,
        query_vector=query_embedding,
        limit=20
    )

    services     = []
    rerank_texts = []

    for result in results:
        doc_id         = result.payload["mongo_id"]
        http_operation = result.payload["http_operation"]

        retrieved = collection.find_one({"_id": doc_id})
        retrieved = bson.json_util.loads(dumps(retrieved))
        try:
            name         = retrieved.get("name")
            description  = retrieved.get("description")
            capabilities = retrieved.get("capabilities")
            capability   = capabilities.get(http_operation)
            endpoints    = retrieved.get("endpoints")
            endpoint     = endpoints.get(http_operation)

            response_schemas = retrieved.get("response_schemas", {})
            resp_schema      = response_schemas.get(http_operation)

            request_schemas = retrieved.get("request_schemas", {})
            req_schema      = request_schemas.get(http_operation)

            all_parameters  = retrieved.get("parameters", {})
            endpoint_params = all_parameters.get(http_operation)

            service = {
                "_id":         doc_id,
                "name":        name,
                "description": description,
                "capabilities": {
                    http_operation: capability
                },
                "endpoints": {
                    http_operation: endpoint
                },
                "response_schemas": {
                    http_operation: resp_schema
                },
                "request_schemas": {
                    http_operation: req_schema
                },
                "parameters": {
                    http_operation: endpoint_params
                },
            }

            rerank_texts.append(capability)
            services.append(service)

        except Exception as e:
            logger.error(f"Error processing doc_id: {doc_id}, operation: {http_operation} - {str(e)}")

    rerank_inputs = [(query_text, cap_text) for cap_text in rerank_texts]
    scores        = reranker_model.predict(rerank_inputs)
    reranked      = sorted(zip(services, scores), key=lambda x: x[1], reverse=True)

    # ── Merge: un oggetto per servizio, preservando l'ordine del best score ──
    # Qdrant restituisce 1 vettore per endpoint: lo stesso servizio può comparire
    # N volte, ciascuna con 1 capability diversa. Il merge li riunisce in un
    # unico documento con tutti gli endpoint rilevanti, ordinati per rilevanza.
    merged: dict = {}
    for service, score in reranked:
        sid = service["_id"]
        if sid not in merged:
            merged[sid] = {
                "_id":              sid,
                "name":             service["name"],
                "description":      service["description"],
                "capabilities":     {},
                "endpoints":        {},
                "response_schemas": {},
                "request_schemas":  {},
                "parameters":       {},
                "_best_score":      score,   # score del primo match = il più rilevante
            }
        merged[sid]["capabilities"].update(service.get("capabilities", {}))
        merged[sid]["endpoints"].update(service.get("endpoints", {}))
        merged[sid]["response_schemas"].update(service.get("response_schemas", {}))
        merged[sid]["request_schemas"].update(service.get("request_schemas", {}))
        merged[sid]["parameters"].update(service.get("parameters", {}))

    # Ordina i servizi per best score e rimuove il campo interno
    ordered_services = sorted(merged.values(), key=lambda x: x["_best_score"], reverse=True)
    for s in ordered_services:
        s.pop("_best_score", None)

    # ── Token budget: include i servizi completi finché non si supera il limite ──
    max_tokens     = 7600
    current_tokens = 0
    top_results    = []

    for s in ordered_services:
        serialized = json.dumps(s)
        n_tokens   = count_tokens(serialized)
        if current_tokens + n_tokens <= max_tokens:
            top_results.append(s)
            current_tokens += n_tokens
        else:
            break

    logger.info(
    f"[SEARCH] {len(results)} vettori → {len(merged)} servizi unici → "
    f"{len(top_results)} nel budget | token usati: {current_tokens}/{max_tokens}")
    return jsonify({"results": top_results}), 200


@app.route("/service", methods=["POST"])
def create_or_update_service_old():
    data = request.get_json()
    if not data or "id" not in data:
        return jsonify({"error": "Missing 'id' field"}), 400

    doc_id = data["id"]
    data["_id"] = doc_id
    data.pop("id", None)

    capabilities = data.get("capabilities")
    for http_op, capability in capabilities.items():
        embedding = embed(capability)
        print(f"EMBEDDING DIM: {len(embedding)}")
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