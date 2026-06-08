#!/usr/bin/env python3
"""
Test Runner — Valutazione basata su dataset con metriche RestGPT.

Questo modulo si occupa di caricare i dataset di benchmark, inviare le query 
al Control Unit ed elaborare le metriche di valutazione definite nel paper RestGPT.

Metriche calcolate:
    - Success Rate (S%)        : Percentuale di query in cui tutti i task hanno successo.
    - Correct Path Rate (CP%)  : Percentuale di query in cui la sequenza di task (piano di esecuzione)
                                 corrisponde esattamente all'oracolo.
    - Delta Solution Length    : Differenza media assoluta tra la lunghezza del piano generato
      (ΔSL)                      e quella dell'oracolo (|planned| - |oracle|).
    - Latency (sec)            : Tempo di esecuzione della fase di planning.

Struttura delle directory attesa/generata:
    results/
      test_runs/
        <dataset>_<model>_<run_id>/
          responses.json       — Risposte complete per ogni query (JSON).
          metrics.json         — Metriche aggregate a livello di dataset (JSON).
          summary.txt          — Report leggibile e riassuntivo (Text).
          per_query.csv        — Dettaglio tabellare delle metriche per singola query (CSV).

Esempi di utilizzo:
    # Esegui contro il Control Unit in esecuzione (default http://localhost:5500):
    python benchmark/test_runner.py

    # Specifica dataset particolari e l'URL del control-unit:
    python benchmark/test_runner.py --datasets tmdb spotify --control-url http://localhost:5500

    # Specifica una directory di output personalizzata:
    python benchmark/test_runner.py --output-dir results/my_run
"""

import argparse
import csv
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATASETS_DIR = PROJECT_ROOT / "datasets"
RESULTS_DIR  = PROJECT_ROOT / "results" / "test_runs"

BACKEND_TYPES = {
    "ollama": "Ollama",
    "llamacpp": "llama.cpp",
}


def parse_args():
    """Analizza gli argomenti passati da riga di comando.

    Returns:
        argparse.Namespace: Un oggetto contenente gli argomenti parsati.
    """
    p = argparse.ArgumentParser(description="Test Runner — Valutazione basata su dataset con metriche RestGpt")
    p.add_argument(
        "--datasets", nargs="+", default=None,
        help="Dataset da valutare (default: tutti i file JSON in datasets/)"
    )
    p.add_argument(
        "--control-url", default="http://localhost:5500",
        help="URL di base del Control Unit (default: http://localhost:5500)"
    )
    p.add_argument(
        "--output-dir", default=None,
        help="Directory di output (default: results/test_runs/<dataset>_<model>_<timestamp>)"
    )
    p.add_argument(
        "--model", default=None,
        help="Identificativo del modello per nominare la directory (default: da var d'ambiente LLM_MODEL o 'unknown')"
    )
    p.add_argument(
        "--run-id", default=None,
        help="Identificativo personalizzato per l'esecuzione (default: timestamp generato automaticamente)"
    )
    p.add_argument(
        "--max-queries", type=int, default=None,
        help="Limite massimo di query per dataset (utile per test rapidi di smoke)"
    )
    return p.parse_args()


def discover_datasets(selected=None):
    """Carica in memoria i dataset in formato JSON presenti nella directory dedicata.

    Args:
        selected (list[str], opzionale): Lista di nomi di dataset da includere (senza estensione .json).
            Se None, vengono caricati tutti i dataset disponibili.

    Returns:
        dict: Un dizionario dove le chiavi sono i nomi dei dataset e i valori sono 
            le liste di query/oracoli caricate dal JSON.
    """
    datasets = {}
    for fpath in sorted(DATASETS_DIR.glob("*.json")):
        stem = fpath.stem
        if selected and stem not in selected:
            continue
        with open(fpath, "r", encoding="utf-8") as f:
            datasets[stem] = json.load(f)
    return datasets


def invoke_control_unit(query, control_url):
    """Invia una singola query al Control Unit via richiesta POST.

    Args:
        query (str): Il testo della query da sottoporre in linguaggio naturale.
        control_url (str): L'URL di base del Control Unit.

    Returns:
        tuple: Una tupla contenente:
            - dict: Dati della risposta in formato JSON. Se si verifica un errore, 
              contiene i dettagli dell'errore.
            - float: Tempo impiegato (latenza) in secondi.
            - int: Codice di stato HTTP della risposta.
    """
    payload = {"input": query}
    t0 = time.perf_counter()
    resp = requests.post(
        f"{control_url}/api/control/invoke",
        json=payload,
        timeout=None,
    )
    elapsed = time.perf_counter() - t0
    http_status = resp.status_code
    try:
        data = resp.json()
    except Exception:
        data = {"error": f"non-json response (HTTP {http_status})", "raw_text": resp.text[:500]}
    return data, elapsed, http_status


# =====================================================================
# SEZIONE CORRISPONDENZA PATH / STEP
# =====================================================================
# Questa sezione implementa il confronto tra il piano d'esecuzione generato 
# e l'oracolo, con tre correzioni (FIX) rispetto allo standard base:
#
#   FIX 1 (Placeholder): Maschera i placeholder (es. {{...}}) prima del parsing
#         dell'URL (urlparse), evitando che un filtro JMESPath incorporato 
#         come {{t.items[?name=='X'] | [0].id}} venga spezzato in corrispondenza del '?'.
#
#   FIX 2 (Suffisso del Servizio): Rimuove esplicitamente i prefissi REST noti
#         (es. /rest/<svc>/<ver>) garantendo un confronto esatto del numero di 
#         segmenti del path per evitare falsi positivi.
#
#   FIX 3 (Estensione Lineare DAG): Considera un "Correct Path" valido anche 
#         quando è una legittima estensione lineare del DAG (Grafo Aciclico Diretto) 
#         dell'oracolo. Le richieste GET indipendenti (che non dipendono da ID di task 
#         precedenti) possono essere riordinate senza invalidare il piano.
#
# NOTA: I GET intermedi ridondanti (redundant intermediate GET) non sono perdonati 
#       da questa logica e vanno analizzati a parte.
# =====================================================================

_PLACEHOLDER_RE    = re.compile(r"\{\{.*?\}\}")          # Trova i placeholder, non-greedy
_SERVICE_PREFIX_RE = re.compile(r"^/rest/[^/]+/[^/]+")   # Trova prefissi tipo /rest/Spotify/1.0

# Parametri path considerati "LITERAL" (es. {type}, {region}). Non generano dipendenze 
# temporali o d'ordine tra i task. Tutto ciò che non è qui è trattato come dipendente.
_LITERAL_PATH_PARAMS = {"type", "media_type", "time_window", "language", "region", "sort_by"}


def _is_literal_param(name, literal=frozenset()):
    """Verifica se un parametro fa parte di un set letterale definito o se è un numero (enum)."""
    return name in _LITERAL_PATH_PARAMS or name in literal or name.endswith("_number")


def derive_literal_path_params(spec):
    """Estrae i nomi dei parametri di percorso definiti come 'enum' nelle spec OpenAPI.

    Args:
        spec (dict): Le specifiche OpenAPI del servizio.

    Returns:
        set: Un insieme di nomi dei parametri di path che hanno valori predefiniti (letterali) 
             e non dipendono dagli ID generati da chiamate precedenti.
    """
    out = set()
    for _p, item in (spec.get("paths") or {}).items():
        for _m, op in (item or {}).items():
            if not isinstance(op, dict):
                continue
            for prm in op.get("parameters", []) or []:
                if prm.get("in") == "path" and (prm.get("schema") or {}).get("enum"):
                    out.add(prm.get("name"))
    return out


def _clean_path(url):
    """Pulisce e normalizza l'URL di un task generato per il confronto (Applica FIX 1 e FIX 2)."""
    if not url:
        return ""
    masked = _PLACEHOLDER_RE.sub("{ph}", url)            # FIX 1
    parsed = urlparse(masked)
    path = parsed.path.rstrip("/")
    if not path and " " in url:
        path = url.split(" ")[-1].rstrip("/")
    path = _SERVICE_PREFIX_RE.sub("", path)              # FIX 2
    return path


def extract_operation_path(task):
    """Estrae il metodo HTTP e il percorso normalizzato da un singolo task del piano."""
    operation = task.get("operation", "GET").upper()
    url = task.get("url") or task.get("endpoint") or ""
    return operation, _clean_path(url)


def normalize_oracle_path(path):
    """Rimuove gli spazi e i trailing slash dal path di un oracolo."""
    return path.strip().rstrip("/")


def _is_wildcard_segment(segment):
    """Determina se il segmento di URL è una wildcard o un placeholder dinamico."""
    return bool(re.fullmatch(r"\{[^/{}]+\}|\{ph\}", segment)) or segment.startswith("{{")


def _segments_match(planned_seg, oracle_seg):
    """Verifica la corrispondenza tra un singolo segmento generato e uno dell'oracolo."""
    if _is_wildcard_segment(oracle_seg) or _is_wildcard_segment(planned_seg):
        return True
    return planned_seg == oracle_seg


def _step_matches(planned_step, oracle_step):
    """Confronta un intero passo generato con uno dell'oracolo.

    Args:
        planned_step (tuple): Tupla (Metodo_HTTP, Path_Pulito) del task generato.
        oracle_step (tuple): Tupla (Metodo_HTTP, Path_Oracolo).

    Returns:
        bool: True se combaciano esattamente per metodo e logica dei segmenti, False altrimenti.
    """
    p_op, p_path = planned_step
    o_op, o_path = oracle_step
    if p_op != o_op:
        return False
    p_seg = [s for s in p_path.split("/") if s]
    o_seg = [s for s in o_path.split("/") if s]
    if len(p_seg) != len(o_seg):                         # FIX 2: Previene fallback impropri
        return False
    return all(_segments_match(ps, os) for ps, os in zip(p_seg, o_seg))


def _is_source(op, path, literal=frozenset()):
    """Verifica se un task rappresenta una SOURCE indipendente (non necessita di output precedenti)."""
    if op != "GET":
        return False
    for seg in path.split("/"):
        mm = re.fullmatch(r"\{([^/{}]+)\}", seg)
        if mm and not _is_literal_param(mm.group(1), literal):
            return False
    return True


def _build_partial_order(oracle, literal=frozenset()):
    """Costruisce i vincoli di ordinamento (DAG) a partire dalla sequenza oracolo.

    Regole implementate:
    - SOURCE -> DEPENDENT: Una dipendenza deve sempre seguire la sua sorgente.
    - DEPENDENT -> DEPENDENT: L'ordine tra dipendenze va mantenuto rigido.
    Le SOURCE tra di loro non hanno vincoli, e possono variare in ordine.

    Returns:
        set: Un insieme di tuple (i, j) indicanti i vincoli (il task `i` deve precedere `j`).
    """
    is_src = [_is_source(op, p, literal) for (op, p) in oracle]
    edges = set()
    for i, src in enumerate(is_src):
        if src:
            for j in range(i + 1, len(oracle)):
                if not is_src[j]:
                    edges.add((i, j))
    deps = [i for i, s in enumerate(is_src) if not s]
    for a, b in zip(deps, deps[1:]):
        edges.add((a, b))
    return edges


def _assign_respecting_order(planned_steps, oracle_norm, literal=frozenset()):
    """Tenta un matching (assegnamento) iniettivo valido rispettando i vincoli d'ordine.

    Returns:
        dict | None: Un dizionario {indice_oracolo: indice_pianificato} se il piano 
                     è un'estensione lineare valida del DAG. Altrimenti, None.
    """
    cand = []
    for o in oracle_norm:
        c = [pi for pi, p in enumerate(planned_steps) if _step_matches(p, o)]
        if not c:
            return None
        cand.append(c)
    edges = _build_partial_order(oracle_norm, literal)
    used, assign = set(), {}

    def bt(k):
        if k == len(oracle_norm):
            return True
        for pi in cand[k]:
            if pi in used:
                continue
            ok = True
            for (i, j) in edges:
                if i == k and j in assign and pi > assign[j]:
                    ok = False; break
                if j == k and i in assign and assign[i] > pi:
                    ok = False; break
            if not ok:
                continue
            used.add(pi); assign[k] = pi
            if bt(k + 1):
                return True
            used.discard(pi); del assign[k]
        return False

    return dict(assign) if bt(0) else None


def compare_plan_with_oracle(tasks, oracle_steps, backend_mode="MOCK", literal=frozenset()):
    """Esegue un confronto analitico approfondito tra piano generato e passi oracolo.

    Args:
        tasks (list[dict]): Lista di task estratti dal piano di esecuzione generato.
        oracle_steps (list[str]): I task definiti come soluzione corretta.
        backend_mode (str): Modalità di utilizzo (attualmente mock/default).
        literal (frozenset): Set di parametri passati in input riconosciuti come costanti.

    Returns:
        dict: Dizionario contenente match, mismatch, metriche di F1 e booleano `is_correct_path`.
    """
    planned_steps = [extract_operation_path(t) for t in tasks]

    oracle_normalized = []
    for step in oracle_steps:
        parts = step.strip().split(maxsplit=1)
        if len(parts) == 2:
            oracle_normalized.append((parts[0].strip().upper(),
                                      normalize_oracle_path(parts[1])))

    # Verifica se il percorso è corretto rispettando il DAG
    assignment = _assign_respecting_order(planned_steps, oracle_normalized, literal)
    is_correct_path = assignment is not None

    if assignment is not None:
        matched_idx = set(assignment.values())
        matches = [{"planned": planned_steps[assignment[i]], "oracle": o}
                   for i, o in enumerate(oracle_normalized)]
        oracle_missed = []
    else:
        matched_idx, matches, oracle_missed = set(), [], []
        for o in oracle_normalized:
            hit = next((pi for pi, p in enumerate(planned_steps)
                        if pi not in matched_idx and _step_matches(p, o)), None)
            if hit is None:
                oracle_missed.append({"oracle": o, "planned": None})
            else:
                matched_idx.add(hit)
                matches.append({"planned": planned_steps[hit], "oracle": o})
    mismatches = [{"planned": p, "oracle": None}
                  for pi, p in enumerate(planned_steps) if pi not in matched_idx]

    # Diagnostica (Recall/Precision/F1) indipendente dall'ordine
    used, step_hits, step_missed = set(), 0, []
    for o in oracle_normalized:
        hit = next((pi for pi, p in enumerate(planned_steps)
                    if pi not in used and _step_matches(p, o)), None)
        if hit is None:
            step_missed.append({"oracle": o, "planned": None})
        else:
            used.add(hit); step_hits += 1

    num_planned, num_oracle = len(planned_steps), len(oracle_normalized)
    step_recall = 1.0 if num_oracle == 0 else step_hits / num_oracle
    step_precision = 0.0 if num_planned == 0 else step_hits / num_planned
    step_f1 = (0.0 if step_precision + step_recall == 0
               else 2 * step_precision * step_recall / (step_precision + step_recall))

    return {
        "planned_steps": planned_steps,
        "oracle_steps": oracle_normalized,
        "matches": matches,
        "mismatches": mismatches,
        "oracle_missed": oracle_missed,
        "is_correct_path": is_correct_path,
        "num_planned": num_planned,
        "num_oracle": num_oracle,
        "step_hits": step_hits,
        "step_missed": step_missed,
        "step_recall": round(step_recall, 4),
        "step_precision": round(step_precision, 4),
        "step_f1": round(step_f1, 4),
    }


def is_all_successful(execution_results):
    """Verifica se tutti i task contenuti nei risultati hanno riportato stato 'SUCCESS'."""
    for r in execution_results:
        if r.get("status") != "SUCCESS":
            return False
    return True


def compute_delta_sl(planned_count, oracle_count):
    """Calcola la Delta Solution Length assoluta (|pianificati| - |oracolo|)."""
    return abs(planned_count - oracle_count)


def evaluate_dataset(dataset_name, queries, control_url, max_queries=None, output_dir=None):
    """Orchestra la valutazione di tutte le query di un dataset specifico.

    Args:
        dataset_name (str): Nome del dataset in esecuzione.
        queries (list[dict]): Lista di query presenti nel dataset.
        control_url (str): Endpoint da chiamare per l'inferenza del plan.
        max_queries (int, opzionale): Limita l'esecuzione ai primi N risultati.
        output_dir (Path, opzionale): Cartella dove salvare il progresso temporaneo.

    Returns:
        list[dict]: Array con i risultati dettagliati per ogni query sottomessa.
    """
    if max_queries:
        queries = queries[:max_queries]

    total = len(queries)
    results = []

    for idx, entry in enumerate(queries):
        query = entry["query"]
        oracle = entry.get("solution", [])

        print(f"[{idx+1}/{total}] {dataset_name}: {query[:80]}{'...' if len(query) > 80 else ''}")

        try:
            response_data, total_latency, http_status = invoke_control_unit(query, control_url)
        except requests.exceptions.RequestException as e:
            print(f"  ERROR: HTTP request failed — {e}")
            response_data = {"error": str(e)}
            total_latency = 0.0
            http_status = 0

        execution_plan = response_data.get("execution_plan", {})
        execution_results = response_data.get("execution_results", [])
        planning_latency = response_data.get("planning_latency_s", 0.0) or 0.0
        error = response_data.get("error", "")

        tasks = execution_plan.get("tasks", [])
        comparison = compare_plan_with_oracle(tasks, oracle)

        tasks_successful = is_all_successful(execution_results) if execution_results else False
        delta_sl = compute_delta_sl(comparison["num_planned"], comparison["num_oracle"])

        per_query = {
            "query_index": idx + 1,
            "query": query,
            "oracle": oracle,
            "error": error,
            "http_status": http_status,
            "planning_latency_s": round(planning_latency, 3),
            "total_latency_s": round(total_latency, 3),
            "planned_count": comparison["num_planned"],
            "oracle_count": comparison["num_oracle"],
            "delta_sl": delta_sl,
            "is_correct_path": comparison["is_correct_path"],
            "step_recall": comparison["step_recall"],
            "step_precision": comparison["step_precision"],
            "step_f1": comparison["step_f1"],
            "step_missed": comparison["step_missed"],
            "tasks_successful": tasks_successful,
            "matches": comparison["matches"],
            "mismatches": comparison["mismatches"],
            "oracle_missed": comparison["oracle_missed"],
            "execution_plan": execution_plan,
            "execution_results": [
                {
                    "task_name": r.get("task_name"),
                    "operation": r.get("operation"),
                    "status": r.get("status"),
                    "status_code": r.get("status_code"),
                }
                for r in (execution_results or [])
            ],
        }
        results.append(per_query)

        status = "OK" if comparison["is_correct_path"] and tasks_successful else "MISMATCH"
        print(f"  → {status}  planned={comparison['num_planned']} oracle={comparison['num_oracle']} "
              f"ΔSL={delta_sl} stepF1={comparison['step_f1']:.2f} latency={planning_latency:.2f}s")

        if output_dir:
            _save_incremental(results, dataset_name, output_dir)

    return results


def _save_incremental(results, dataset_name, output_dir):
    """Salva su disco in formato temporaneo i risultati, permettendo resilienza.

    Ogni iterazione aggiorna atomicamente (tramite replace):
      - responses.json    : Contiene tutto il payload (request/response/metriche).
      - per_query.csv     : Metriche tabellari sintetiche.
      - metrics.json      : Calcoli e aggregazioni aggiornate.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    tmp = output_dir / ".responses.tmp"
    final = output_dir / "responses.json"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    tmp.replace(final)

    tmp = output_dir / ".per_query.tmp"
    final = output_dir / "per_query.csv"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "query_index", "query", "oracle_count", "planned_count",
            "delta_sl", "is_correct_path", "step_recall", "step_precision",
            "step_f1", "tasks_successful",
            "planning_latency_s", "total_latency_s", "http_status", "error",
        ])
        for r in results:
            writer.writerow([
                r["query_index"], r["query"], r["oracle_count"],
                r["planned_count"], r["delta_sl"], r["is_correct_path"],
                r.get("step_recall", ""), r.get("step_precision", ""),
                r.get("step_f1", ""), r["tasks_successful"], r["planning_latency_s"],
                r["total_latency_s"], r.get("http_status", ""), r.get("error", ""),
            ])
    tmp.replace(final)

    metrics = compute_aggregate_metrics(results, dataset_name)
    tmp = output_dir / ".metrics.tmp"
    final = output_dir / "metrics.json"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=False)
    tmp.replace(final)


def compute_aggregate_metrics(results, dataset_name):
    """Calcola le metriche medie e aggregate (RestGPT) partendo dall'array dei risultati singoli.

    Args:
        results (list[dict]): Lista prodotta da `evaluate_dataset`.
        dataset_name (str): Nome associato all'aggregazione (es: 'tmdb').

    Returns:
        dict: Dizionario riassuntivo contenente P50, P95, media per Recall, Precision, Latency.
    """
    total = len(results)
    if total == 0:
        return {
            "dataset": dataset_name,
            "total_queries": 0,
            "success_rate": 0.0,
            "correct_path_rate": 0.0,
            "avg_delta_sl": 0.0,
            "avg_step_recall": 0.0,
            "avg_step_precision": 0.0,
            "avg_step_f1": 0.0,
            "avg_planning_latency_s": 0.0,
            "avg_total_latency_s": 0.0,
            "p50_planning_latency_s": 0.0,
            "p95_planning_latency_s": 0.0,
        }

    successful = sum(1 for r in results if r["tasks_successful"])
    correct_path = sum(1 for r in results if r["is_correct_path"])
    total_delta_sl = sum(r["delta_sl"] for r in results)
    total_step_recall = sum(r.get("step_recall", 0.0) for r in results)
    total_step_precision = sum(r.get("step_precision", 0.0) for r in results)
    total_step_f1 = sum(r.get("step_f1", 0.0) for r in results)
    planning_latencies = [r["planning_latency_s"] for r in results]
    total_latencies = [r["total_latency_s"] for r in results]
    sorted_planning = sorted(planning_latencies)
    sorted_total = sorted(total_latencies)

    n = len(sorted_planning)
    p50_planning = sorted_planning[n // 2]
    p95_idx = min(int(n * 0.95), n - 1)
    p95_planning = sorted_planning[p95_idx]

    # Calcolo della distribuzione dei codici HTTP
    status_counts = {}
    http_errors = 0
    for r in results:
        s = r.get("http_status", 0)
        status_counts[s] = status_counts.get(s, 0) + 1
        if s and s >= 400:
            http_errors += 1

    return {
        "dataset": dataset_name,
        "total_queries": total,
        "success_rate_s": round(successful / total * 100, 2),
        "correct_path_rate_cp": round(correct_path / total * 100, 2),
        "avg_delta_sl": round(total_delta_sl / total, 2),
        "avg_step_recall": round(total_step_recall / total, 4),
        "avg_step_precision": round(total_step_precision / total, 4),
        "avg_step_f1": round(total_step_f1 / total, 4),
        "avg_planning_latency_s": round(sum(planning_latencies) / total, 3),
        "avg_total_latency_s": round(sum(total_latencies) / total, 3),
        "p50_planning_latency_s": round(p50_planning, 3),
        "p95_planning_latency_s": round(p95_planning, 3),
        "successful_queries": successful,
        "correct_path_queries": correct_path,
        "http_status_distribution": {str(k): v for k, v in sorted(status_counts.items())},
        "http_error_count": http_errors,
    }


def sanitize_path_component(value):
    """Pulisce una stringa per garantire che sia sicura da usare nei percorsi del filesystem."""
    sanitized = re.sub(r'[<>:"/\\|?*\n\r\t]+', '_', str(value))
    sanitized = sanitized.strip(' .')
    return sanitized or 'unknown'


def write_responses(results, output_dir):
    """Persiste le risposte globali elaborate in un file JSON finale."""
    path = output_dir / "responses.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"  Responses: {path}")


def write_metrics(metrics, output_dir):
    """Persiste le metriche aggregate calcolate nel file JSON finale."""
    path = output_dir / "metrics.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=False)
    print(f"  Metrics:   {path}")


def write_per_query_csv(results, output_dir):
    """Converte e salva i dati per-query in formato CSV standard."""
    path = output_dir / "per_query.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "query_index", "query", "oracle_count", "planned_count",
            "delta_sl", "is_correct_path", "step_recall", "step_precision",
            "step_f1", "tasks_successful",
            "planning_latency_s", "total_latency_s", "http_status", "error",
        ])
        for r in results:
            writer.writerow([
                r["query_index"], r["query"], r["oracle_count"],
                r["planned_count"], r["delta_sl"], r["is_correct_path"],
                r.get("step_recall", ""), r.get("step_precision", ""),
                r.get("step_f1", ""), r["tasks_successful"], r["planning_latency_s"],
                r["total_latency_s"], r.get("http_status", ""), r.get("error", ""),
            ])
    print(f"  Per-query: {path}")


def write_summary(metrics, output_dir, run_label):
    """Genera e scrive un file di log testuale riepilogativo per la lettura rapida."""
    path = output_dir / "summary.txt"
    lines = [
        "=" * 64,
        f"  POLLEN Test Runner — Summary",
        f"  Run: {run_label}",
        f"  Dataset: {metrics['dataset']}",
        "=" * 64,
        "",
        f"  Total queries:       {metrics['total_queries']}",
        f"  Successful (S):      {metrics['successful_queries']}",
        f"  Correct path (CP):   {metrics['correct_path_queries']}",
        f"  HTTP errors (4xx/5xx): {metrics.get('http_error_count', 0)}",
        "",
        f"  Success Rate (S%):         {metrics['success_rate_s']:.2f}%",
        f"  Correct Path Rate (CP%):   {metrics['correct_path_rate_cp']:.2f}%",
        f"  Avg Δ Solution Length:     {metrics['avg_delta_sl']:.2f}",
        "",
        "  Step-level (diagnostic, non-canonical):",
        f"    Avg Step Recall:    {metrics.get('avg_step_recall', 0.0):.3f}",
        f"    Avg Step Precision: {metrics.get('avg_step_precision', 0.0):.3f}",
        f"    Avg Step F1:        {metrics.get('avg_step_f1', 0.0):.3f}",
        f"  HTTP Status Distribution:  {metrics.get('http_status_distribution', {})}",
        "",
        "  Latency (planning):",
        f"    Average:  {metrics['avg_planning_latency_s']:.3f} s",
        f"    P50:      {metrics['p50_planning_latency_s']:.3f} s",
        f"    P95:      {metrics['p95_planning_latency_s']:.3f} s",
        "",
        f"  Latency (total):",
        f"    Average:  {metrics['avg_total_latency_s']:.3f} s",
        "",
    ]
    text = "\n".join(lines)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"  Summary:  {path}")


def main():
    """Funzione principale. Inizializza l'esecuzione dei test di benchmark."""
    args = parse_args()

    datasets = discover_datasets(args.datasets)
    if not datasets:
        print("Nessun dataset trovato. Verifica la cartella 'datasets/'.")
        sys.exit(1)

    print(f"Dataset trovati: {list(datasets.keys())}")
    model = sanitize_path_component(args.model or os.environ.get("LLM_MODEL", "unknown"))
    run_id = sanitize_path_component(args.run_id or datetime.now().strftime("%Y%m%d_%H%M%S"))

    for dataset_name, queries in datasets.items():
        print(f"\n{'=' * 64}")
        print(f"  Dataset: {dataset_name} ({len(queries)} query totali)")
        print(f"{'=' * 64}")

        run_label = f"{dataset_name}_{model}_{run_id}"
        if args.output_dir:
            output_dir = Path(args.output_dir)
        else:
            output_dir = RESULTS_DIR / run_label

        # 1. Eseguiamo il dataset contro l'endpoint del CU
        results = evaluate_dataset(
            dataset_name, queries, args.control_url, args.max_queries, output_dir
        )

        # 2. Aggreghiamo le metriche totali (RestGPT) e arricchiamole con metadati
        metrics = compute_aggregate_metrics(results, dataset_name)
        metrics["model"] = model
        metrics["run_id"] = run_id
        metrics["control_url"] = args.control_url

        # 3. Finalizziamo la scrittura su disco
        write_responses(results, output_dir)
        write_metrics(metrics, output_dir)
        write_per_query_csv(results, output_dir)
        write_summary(metrics, output_dir, run_label)

        print(f"\n  Risultati salvati correttamente in: {output_dir}")

    print("\nBenchmark completato.")


if __name__ == "__main__":
    main()