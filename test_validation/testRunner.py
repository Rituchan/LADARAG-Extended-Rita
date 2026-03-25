"""
testRunner.py  (v2)
====================
Test runner per LADARAG-Extended con oracle multi-dimensionale e multiple run.

Oracle a 4 layer:
  L1 — Plan:      METHOD + path corretti
  L2 — Execution: tutti i task hanno restituito 2xx
  L3 — Chaining:  placeholder risolti con valori non vuoti
  L4 — Schema:    body POST/PUT contengono campi con valori non vuoti

Metriche di affidabilità su N run:
  Consistency@N  — % di run che producono piano identico
  Reliability@N  — % di run in cui tutti i task sono SUCCESS
  Pass@1         — successo alla prima run
  Pass@K         — successo in almeno una delle K run

Run per categoria (default adattivo):
  single-get / single-delete  → 1 run
  single-post / single-put    → 2 run
  chaining / cross-service    → 3 run
  three-step                  → 3 run

Uso:
    python testRunner.py
    python testRunner.py --csv smart_city_test_requests.csv
    python testRunner.py --runs 5
    python testRunner.py --only-categories chaining cross-service
    python testRunner.py --fail-fast
    python testRunner.py --control-url http://localhost:5500
"""

import sys
import csv
import json
import re
import time
import argparse
import unicodedata
import statistics
import io
import contextlib
import os
from collections import Counter, defaultdict
from urllib.parse import urlparse
from datetime import datetime


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAZIONE
# ─────────────────────────────────────────────────────────────────────────────

DEFAULTS = {
    "control_url": "http://localhost:5500/api/control/invoke",
    "csv_file":    "C:\\Universita\\Tesi\\LADARAG-Extended-Rita\\test_validation\\smart_city_test_requests.csv",
    "output_dir":  ".",
    "timeout":     90,
    "delay":       0.3,
    "runs":        None,   # None = adattivo per categoria
}

RUNS_BY_CATEGORY = {
    "single-get":        1,
    "single-delete":     1,
    "single-post":       2,
    "single-put":        2,
    "single-op":         2,
    "get-then-get":      2,
    "get-post":          2,
    "chaining-find-put": 3,
    "two-step":          2,
    "three-step":        3,
    "cross-service":     3,
}
DEFAULT_RUNS = 2


# ─────────────────────────────────────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def normalize(text):
    text = text.strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = re.sub(r"[''´`]", "'", text)
    text = re.sub(r"[""]", '"', text)
    text = re.sub(r"\s+", " ", text)
    return text


def parse_oracle(oracle_raw):
    cleaned = oracle_raw.replace('"', '').replace('\\n', '\n').replace('\\r', '')
    result  = []
    for line in cleaned.strip().splitlines():
        parts = line.strip().split(maxsplit=1)
        if len(parts) == 2:
            method = parts[0].strip().upper()
            path   = parts[1].strip().rstrip('/')
            result.append((method, path))
    return result


def extract_from_task(task):
    method   = task.get("operation", "").upper()
    endpoint = task.get("url", "") or task.get("endpoint", "")
    path     = urlparse(str(endpoint)).path.rstrip('/')
    return method, path


def infer_category(oracle_steps, query):
    n       = len(oracle_steps)
    methods = [m for m, _ in oracle_steps]
    paths   = [p for _, p in oracle_steps]
    
    # Se i path puntano a root diverse (es. /api e /v1), è sempre un cross-service!
    resources = {p.split('/')[1] for p in paths if '/' in p and len(p.split('/')) > 1}
    if len(resources) > 1:
        return "cross-service"

    if n == 1:
        m = methods[0]
        return {"GET": "single-get", "POST": "single-post",
                "PUT": "single-put", "DELETE": "single-delete"}.get(m, "single-op")
    if n == 2:
        if methods == ["GET", "GET"]:
            return "get-then-get"
        if set(methods) == {"GET", "POST"}:
            return "get-post"
        if set(methods) in ({"GET", "PUT"}, {"GET", "DELETE"}):
            return "chaining-find-put"
        return "two-step"
    
    return "three-step"


def canonical_plan(plan):
    tasks = plan.get("tasks", [])
    parts = []
    for t in tasks:
        method = t.get("operation", "").upper()
        path   = urlparse(str(t.get("url", ""))).path.rstrip('/')
        parts.append(f"{method} {path}")
    return " | ".join(parts)


# ─────────────────────────────────────────────────────────────────────────────
# ORACLE MULTI-DIMENSIONALE — 4 LAYER
# ─────────────────────────────────────────────────────────────────────────────

def check_layer1_plan(tasks, oracle_steps):
    """
    L1 — Plan: METHOD + path corretti (matching greedy, path suffix).
    Certifica che il piano abbia chiamato gli endpoint giusti nel numero giusto.
    """
    matched_oracle = set()
    matches        = []
    mismatches     = []

    for task in tasks:
        t_method, t_path = extract_from_task(task)
        found = False
        for idx, (o_method, o_path) in enumerate(oracle_steps):
            if idx in matched_oracle:
                continue
            if t_method == o_method and t_path.endswith(o_path):
                matches.append({"task": (t_method, t_path), "oracle": (o_method, o_path)})
                matched_oracle.add(idx)
                found = True
                break
        if not found:
            mismatches.append({"task": (t_method, t_path)})

    tp = len(matches)
    fp = len(mismatches)
    fn = len(oracle_steps) - tp

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall    = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

    if tp == len(oracle_steps) and fp == 0:
        verdict = "PASS"
    elif tp > 0:
        verdict = "PARTIAL"
    else:
        verdict = "FAIL"

    return {
        "verdict": verdict,
        "tp": tp, "fp": fp, "fn": fn,
        "precision": precision, "recall": recall, "f1": f1,
        "matches": matches, "mismatches": mismatches,
    }


def check_layer2_execution(execution_results):
    """
    L2 — Execution: tutti i task hanno restituito status SUCCESS (HTTP 2xx).
    Cattura fallimenti reali dell'API che L1 non vede (piano giusto, body sbagliato).
    """
    if not execution_results:
        return {"verdict": "SKIP", "details": "Nessun risultato di esecuzione", "failed": [], "total": 0, "passed": 0}

    failed = []
    for r in execution_results:
        status      = r.get("status", "")
        status_code = r.get("status_code", 0)
        task_name   = r.get("task_name", "unnamed")
        if status != "SUCCESS" or not (200 <= int(status_code) < 300):
            failed.append(f"{task_name}: {status} HTTP {status_code}")

    return {
        "verdict": "PASS" if not failed else "FAIL",
        "failed":  failed,
        "total":   len(execution_results),
        "passed":  len(execution_results) - len(failed),
    }


def check_layer3_chaining(tasks, execution_results):
    """
    L3 — Chaining: i placeholder nell'URL sono stati risolti con valori non vuoti.
    Cattura {{now}} risolto a "", ID non trovati da FIND, indici fuori range.
    """
    issues = []

    for task in tasks:
        url       = str(task.get("url", ""))
        task_name = task.get("task_name", "unnamed")

        # Placeholder non risolti rimasti nell'URL
        if re.search(r'\{\{.*?\}\}', url):
            issues.append(f"{task_name}: placeholder non risolto in URL")
            continue

        # Segmenti vuoti (placeholder risolto a "")
        path = urlparse(url).path
        if re.search(r'//+', path):
            issues.append(f"{task_name}: segmento vuoto nell'URL (placeholder risolto a '')")

    return {
        "verdict": "PASS" if not issues else "FAIL",
        "issues":  issues,
    }


def check_layer4_schema(tasks, execution_results):
    """
    L4 — Schema: i body POST/PUT contengono campi con valori non vuoti.
    Cattura campi required omessi o risolti a stringa vuota (es. {{now}}).
    """
    issues = []

    for task in tasks:
        operation = task.get("operation", "").upper()
        if operation not in ("POST", "PUT"):
            continue

        task_name = task.get("task_name", "unnamed")
        body      = task.get("input", {})

        if not body or body == "":
            issues.append(f"{task_name}: body {operation} vuoto o assente")
            continue

        if isinstance(body, dict):
            empty_fields = [k for k, v in body.items() if v == "" or v is None]
            if empty_fields:
                issues.append(f"{task_name}: campi vuoti/null nel body: {empty_fields}")

    return {
        "verdict": "PASS" if not issues else "FAIL",
        "issues":  issues,
    }


def evaluate_all_layers(data, oracle_steps, tasks):
    """
    Esegue tutti e 4 i layer e calcola il verdetto finale.
    CORRECT   = tutti i layer PASS (o SKIP)
    PARTIAL   = L1 almeno PARTIAL ma qualche layer FAIL
    INCORRECT = L1 FAIL
    """
    exec_results = data.get("execution_results", [])
    if isinstance(exec_results, dict):
        exec_results = []

    l1 = check_layer1_plan(tasks, oracle_steps)
    l2 = check_layer2_execution(exec_results)
    l3 = check_layer3_chaining(tasks, exec_results)
    l4 = check_layer4_schema(tasks, exec_results)

    layers = {"L1_plan": l1, "L2_execution": l2, "L3_chaining": l3, "L4_schema": l4}

    if l1["verdict"] == "FAIL":
        final = "INCORRECT"
    elif all(l["verdict"] in ("PASS", "SKIP") for l in layers.values()):
        final = "CORRECT"
    else:
        final = "PARTIAL"

    return {
        "final": final,
        "layers": layers,
        "f1": l1["f1"],
        "tp": l1["tp"], "fp": l1["fp"], "fn": l1["fn"],
    }


# ─────────────────────────────────────────────────────────────────────────────
# SINGOLA CHIAMATA HTTP
# ─────────────────────────────────────────────────────────────────────────────

def run_query(question, control_url, timeout):
    import requests as req
    try:
        t0       = time.perf_counter()
        response = req.post(control_url, json={"input": question}, timeout=timeout)
        latency  = time.perf_counter() - t0
        return {"ok": True, "data": response.json(), "latency": latency, "http_status": response.status_code}
    except req.exceptions.Timeout:
        return {"ok": False, "error": f"Timeout dopo {timeout}s", "latency": timeout}
    except req.exceptions.ConnectionError as e:
        return {"ok": False, "error": f"Connessione fallita: {e}", "latency": 0}
    except Exception as e:
        return {"ok": False, "error": str(e), "latency": 0}


# ─────────────────────────────────────────────────────────────────────────────
# CORE TEST LOOP
# ─────────────────────────────────────────────────────────────────────────────

def run_tests(args):
    import requests as req

    print(f"Verifica connettività verso {args.control_url}...")
    try:
        req.get(args.control_url.replace("/invoke", ""), timeout=5)
        print("  OK\n")
    except Exception:
        print("  [WARN] Il control-unit potrebbe non essere raggiungibile.\n")

    rows = []
    with open(args.csv_file, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            q_key = next((k for k in row if 'question' in k.lower()), None)
            o_key = next((k for k in row if 'oracle'   in k.lower()), None)
            if q_key and o_key:
                n_key = next((k for k in row if 'noise' in k.lower()), None)
                rows.append({
                    "question":   row[q_key].strip(),
                    "oracle":     row[o_key].strip(),
                    "noise_type": row[n_key].strip() if n_key and row.get(n_key) else None,
                })

    print(f"Caricate {len(rows)} query da '{args.csv_file}'")

    if args.only_categories:
        rows = [
            r for r in rows
            if any(c in infer_category(parse_oracle(r["oracle"]), r["question"])
                   for c in args.only_categories)
        ]
        print(f"Filtrate a {len(rows)} query per categorie: {args.only_categories}")

    if getattr(args, "only_noise", None):
        rows = [
            r for r in rows
            if r.get("noise_type") and any(n in r["noise_type"] for n in args.only_noise)
        ]
        print(f"Filtrate a {len(rows)} query per noise type: {args.only_noise}")

    results        = []
    all_latencies  = []
    status_counter = Counter()
    category_stats = defaultdict(lambda: {
        "correct": 0, "partial": 0, "incorrect": 0, "error": 0,
        "total": 0, "pass_at_1": 0, "pass_at_k": 0, "consistency_sum": 0.0,
    })

    total = len(rows)
    print(f"\n{'='*72}")
    print(f"Avvio — {total} query — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Oracle: 4 layer (L1 plan · L2 execution · L3 chaining · L4 schema)")
    print(f"{'='*72}\n")

    # noise_type stats: {noise_type: {correct,partial,incorrect,error,total}}
    noise_stats = defaultdict(lambda: {
        "correct": 0, "partial": 0, "incorrect": 0, "error": 0, "total": 0,
    })

    for idx, row in enumerate(rows, 1):
        question     = row["question"]
        oracle_steps = parse_oracle(row["oracle"])
        category     = infer_category(oracle_steps, question)
        noise_type   = row.get("noise_type")  # None per CSV senza colonna
        n_runs       = args.runs if args.runs else RUNS_BY_CATEGORY.get(category, DEFAULT_RUNS)

        noise_label = f"[{noise_type}]" if noise_type else ""
        print(f"[{idx:>3}/{total}] {category:<22} (×{n_runs}) {noise_label:<22} {question[:50]}")

        run_records     = []
        plan_signatures = []

        for run_i in range(n_runs):
            outcome = run_query(question, args.control_url, args.timeout)
            all_latencies.append(outcome["latency"])

            if not outcome["ok"]:
                print(f"         run {run_i+1}: ❌ RETE: {outcome['error']}")
                run_records.append({"network_error": outcome["error"], "latency": outcome["latency"]})
                continue

            data = outcome["data"]
            if "error" in data and "execution_plan" not in data:
                print(f"         run {run_i+1}: ❌ APP:  {data['error']}")
                run_records.append({"app_error": data["error"], "latency": outcome["latency"]})
                continue

            plan  = data.get("execution_plan", {})
            tasks = plan.get("tasks", [])

            ev    = evaluate_all_layers(data, oracle_steps, tasks)
            final = ev["final"]
            l1, l2, l3, l4 = (ev["layers"][k] for k in
                               ("L1_plan", "L2_execution", "L3_chaining", "L4_schema"))

            sig = canonical_plan(plan)
            plan_signatures.append(sig)

            icon   = {"CORRECT": "✅", "PARTIAL": "🟡", "INCORRECT": "❌"}.get(final, "❓")
            icons  = {k: ("✅" if v["verdict"] in ("PASS","SKIP") else "❌")
                      for k, v in ev["layers"].items()}

            print(
                f"         run {run_i+1}: {icon} {final:<10} "
                f"L1{icons['L1_plan']} L2{icons['L2_execution']} "
                f"L3{icons['L3_chaining']} L4{icons['L4_schema']}  "
                f"F1={l1['f1']:.2f} ⏱{outcome['latency']:.1f}s"
            )

            # Dettagli errori
            for mm in l1.get("mismatches", []):
                print(f"              L1 extra:   {mm['task'][0]} {mm['task'][1]}")
            matched_idxs = {
                i for i, (om, op) in enumerate(oracle_steps)
                if any(m["oracle"] == (om, op) for m in l1.get("matches", []))
            }
            for i, (om, op) in enumerate(oracle_steps):
                if i not in matched_idxs:
                    print(f"              L1 missing: {om} {op}")
            for item in l2.get("failed", []):
                print(f"              L2 failed:  {item}")
            for item in l3.get("issues", []):
                print(f"              L3 issue:   {item}")
            for item in l4.get("issues", []):
                print(f"              L4 issue:   {item}")

            run_records.append({
                "run": run_i + 1, "final": final, "eval": ev,
                "latency": outcome["latency"], "plan": plan,
                "tasks_count": len(tasks), "signature": sig,
            })

            time.sleep(args.delay)

        # ── Aggregazione ──────────────────────────────────────────────────
        valid_runs = [r for r in run_records if "final" in r]
        n_valid    = len(valid_runs)

        pass_at_1   = valid_runs[0]["final"] == "CORRECT" if valid_runs else False
        pass_at_k   = any(r["final"] == "CORRECT" for r in valid_runs)
        consistency = Counter(plan_signatures).most_common(1)[0][1] / len(plan_signatures) \
                      if plan_signatures else 0.0

        if n_valid == 0:
            agg_final = "ERROR"
        elif all(r["final"] == "CORRECT" for r in valid_runs):
            agg_final = "CORRECT"
        elif any(r["final"] == "INCORRECT" for r in valid_runs):
            agg_final = "INCORRECT"
        else:
            agg_final = "PARTIAL"

        if n_valid > 1:
            c_icon = "🔵" if consistency == 1.0 else ("🟡" if consistency >= 0.6 else "🔴")
            print(
                f"         aggregato: {agg_final:<10} "
                f"Pass@1={'✅' if pass_at_1 else '❌'}  "
                f"Pass@K={'✅' if pass_at_k else '❌'}  "
                f"Consistency={consistency:.0%}{c_icon}"
            )

        status_counter[agg_final] += 1
        cs = category_stats[category]
        cs["total"] += 1
        cs[agg_final.lower()] = cs.get(agg_final.lower(), 0) + 1
        cs["pass_at_1"]       += int(pass_at_1)
        cs["pass_at_k"]       += int(pass_at_k)
        cs["consistency_sum"] += consistency

        # Track noise_type stats
        if noise_type:
            ns = noise_stats[noise_type]
            ns["total"] += 1
            ns[agg_final.lower()] = ns.get(agg_final.lower(), 0) + 1

        results.append({
            "idx": idx, "question": question, "oracle": oracle_steps,
            "category": category, "noise_type": noise_type,
            "n_runs": n_runs, "n_valid": n_valid,
            "agg_final": agg_final, "pass_at_1": pass_at_1,
            "pass_at_k": pass_at_k, "consistency": consistency,
            "run_records": run_records,
        })

        if args.fail_fast and agg_final in ("ERROR", "INCORRECT"):
            print("\n[FAIL FAST] Interruzione.")
            break

    return {
        "results": results, "latencies": all_latencies,
        "status_counter": status_counter, "category_stats": category_stats,
        "noise_stats": noise_stats,
        "total": total, "ran": len(results),
    }


# ─────────────────────────────────────────────────────────────────────────────
# REPORT
# ─────────────────────────────────────────────────────────────────────────────

def print_summary(data):
    results        = data["results"]
    latencies      = data["latencies"]
    status_counter = data["status_counter"]
    category_stats = data["category_stats"]
    ran            = data["ran"]

    if not results:
        print("Nessun risultato.")
        return

    first_runs = [
        r["run_records"][0]
        for r in results
        if r.get("run_records") and "eval" in r["run_records"][0]
    ]
    all_tp = sum(r["eval"]["tp"] for r in first_runs)
    all_fp = sum(r["eval"]["fp"] for r in first_runs)
    all_fn = sum(r["eval"]["fn"] for r in first_runs)

    micro_p  = all_tp / (all_tp + all_fp) if (all_tp + all_fp) > 0 else 0
    micro_r  = all_tp / (all_tp + all_fn) if (all_tp + all_fn) > 0 else 0
    micro_f1 = 2 * micro_p * micro_r / (micro_p + micro_r) if (micro_p + micro_r) > 0 else 0
    macro_f1 = statistics.mean(r["eval"]["f1"] for r in first_runs) if first_runs else 0

    pass_at_1 = sum(1 for r in results if r.get("pass_at_1")) / ran
    pass_at_k = sum(1 for r in results if r.get("pass_at_k")) / ran
    avg_cons  = statistics.mean(r.get("consistency", 0) for r in results)

    avg_lat    = statistics.mean(latencies)   if latencies else 0
    median_lat = statistics.median(latencies) if latencies else 0
    max_lat    = max(latencies)               if latencies else 0

    print(f"\n{'='*72}")
    print("RIEPILOGO FINALE")
    print(f"{'='*72}")
    print(f"Query eseguite: {ran}")
    print(f"  ✅ CORRECT:   {status_counter.get('CORRECT',0):>4}  ({status_counter.get('CORRECT',0)/ran*100:.1f}%)")
    print(f"  🟡 PARTIAL:   {status_counter.get('PARTIAL',0):>4}  ({status_counter.get('PARTIAL',0)/ran*100:.1f}%)")
    print(f"  ❌ INCORRECT: {status_counter.get('INCORRECT',0):>4}  ({status_counter.get('INCORRECT',0)/ran*100:.1f}%)")
    print(f"  ⚠️  ERROR:     {status_counter.get('ERROR',0):>4}  ({status_counter.get('ERROR',0)/ran*100:.1f}%)")

    print(f"\nAffidabilità:")
    print(f"  Pass@1 (prima run corretta):      {pass_at_1:.1%}")
    print(f"  Pass@K (almeno una run corretta): {pass_at_k:.1%}")
    print(f"  Consistency media (piano stabile):{avg_cons:.1%}")

    print(f"\nMetriche endpoint L1 (prima run, micro):")
    print(f"  TP={all_tp}  FP={all_fp}  FN={all_fn}")
    print(f"  Micro F1={micro_f1:.3f}   Macro F1={macro_f1:.3f}")

    print(f"\nLatenza: Media={avg_lat:.1f}s  Mediana={median_lat:.1f}s  Max={max_lat:.1f}s")

    print(f"\nPer categoria:")
    hdr = f"  {'Categoria':<24}{'Tot':>4}{'OK':>5}{'Part':>6}{'Wrong':>7}{'Err':>5}  {'P@1':>5}{'P@K':>5}{'Cons':>6}{'Acc%':>6}"
    print(hdr)
    print(f"  {'-'*73}")
    for cat in sorted(category_stats):
        s = category_stats[cat]
        if s["total"] == 0:
            continue
        tot  = s["total"]
        acc  = s.get("correct", 0) / tot * 100
        pa1  = s.get("pass_at_1", 0) / tot * 100
        pak  = s.get("pass_at_k", 0) / tot * 100
        cons = s.get("consistency_sum", 0) / tot * 100
        print(
            f"  {cat:<24}{tot:>4}{s.get('correct',0):>5}{s.get('partial',0):>6}"
            f"{s.get('incorrect',0):>7}{s.get('error',0):>5}  "
            f"{pa1:>4.0f}% {pak:>4.0f}% {cons:>4.0f}% {acc:>4.0f}%"
        )

    # Layer failure breakdown
    l2_fails = l3_fails = l4_fails = 0
    total_valid_runs = 0
    for r in results:
        for run in r.get("run_records", []):
            if "eval" not in run:
                continue
            total_valid_runs += 1
            layers = run["eval"].get("layers", {})
            if layers.get("L2_execution", {}).get("verdict") == "FAIL":
                l2_fails += 1
            if layers.get("L3_chaining", {}).get("verdict") == "FAIL":
                l3_fails += 1
            if layers.get("L4_schema",   {}).get("verdict") == "FAIL":
                l4_fails += 1

    if total_valid_runs > 0:
        print(f"\nFailure per layer (su {total_valid_runs} run valide):")
        print(f"  L2 Execution: {l2_fails:>4}  ({l2_fails/total_valid_runs:.1%})")
        print(f"  L3 Chaining:  {l3_fails:>4}  ({l3_fails/total_valid_runs:.1%})")
        print(f"  L4 Schema:    {l4_fails:>4}  ({l4_fails/total_valid_runs:.1%})")

    # ── Robustezza per tipo di rumore ────────────────────────────────────────
    noise_stats = data.get("noise_stats", {})
    if noise_stats:
        print(f"\nRobustezza per tipo di rumore:")
        hdr2 = f"  {'Noise type':<26}{'Tot':>4}{'OK':>5}{'Part':>6}{'Wrong':>7}{'Err':>5}  {'Acc%':>6}"
        print(hdr2)
        print(f"  {'-'*58}")
        for noise_type in sorted(noise_stats):
            ns  = noise_stats[noise_type]
            tot = ns["total"]
            if tot == 0:
                continue
            acc = ns.get("correct", 0) / tot * 100
            # Resilience icon: green>=80%, yellow>=50%, red<50%
            icon = "🟢" if acc >= 80 else ("🟡" if acc >= 50 else "🔴")
            print(
                f"  {noise_type:<26}{tot:>4}{ns.get('correct',0):>5}"
                f"{ns.get('partial',0):>6}{ns.get('incorrect',0):>7}"
                f"{ns.get('error',0):>5}  {acc:>5.0f}% {icon}"
            )

        # Totale robustezza (solo righe con noise_type definito)
        total_noise = sum(ns["total"] for ns in noise_stats.values())
        total_ok    = sum(ns.get("correct", 0) for ns in noise_stats.values())
        if total_noise > 0:
            rob = total_ok / total_noise * 100
            rob_icon = "🟢" if rob >= 80 else ("🟡" if rob >= 50 else "🔴")
            print(f"\n  Resilienza complessiva: {rob:.1f}% {rob_icon}  "
                  f"({total_ok}/{total_noise} query rumorose corrette)")


def write_reports(data, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
    detail  = os.path.join(output_dir, f"test_detail_{ts}.txt")
    summary = os.path.join(output_dir, f"test_summary_{ts}.txt")
    plans   = os.path.join(output_dir, f"execution_plans_{ts}.json")

    with open(detail, 'w', encoding='utf-8') as f:
        for r in data["results"]:
            f.write(f"--- Query #{r['idx']} [{r['category']}] ---\n")
            f.write(f"Q: {r['question']}\n")
            f.write(f"Oracle: {r['oracle']}\n")
            noise_label = f"  Noise: {r['noise_type']}" if r.get("noise_type") else ""
            f.write(f"Aggregato: {r['agg_final']}  "
                    f"Pass@1={r['pass_at_1']}  Pass@K={r['pass_at_k']}  "
                    f"Consistency={r['consistency']:.0%}{noise_label}\n")
            for run in r.get("run_records", []):
                f.write(f"\n  Run {run.get('run','?')}:\n")
                if "network_error" in run:
                    f.write(f"    NETWORK ERROR: {run['network_error']}\n")
                elif "app_error" in run:
                    f.write(f"    APP ERROR: {run['app_error']}\n")
                elif "eval" in run:
                    ev = run["eval"]
                    f.write(f"    Final: {ev['final']}  "
                            f"TP={ev['tp']} FP={ev['fp']} FN={ev['fn']} F1={ev['f1']:.3f}\n")
                    for lname, ldata in ev.get("layers", {}).items():
                        f.write(f"    {lname}: {ldata['verdict']}\n")
                        for key in ("failed", "issues", "mismatches"):
                            for item in ldata.get(key, []):
                                f.write(f"      - {item}\n")
                    f.write(f"    Latency: {run.get('latency',0):.2f}s\n")
            f.write("\n")

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        print_summary(data)
    with open(summary, 'w', encoding='utf-8') as f:
        f.write(buf.getvalue())

    # JSON compatibile con plan_validator.py esistente
    plan_log = []
    for r in data["results"]:
        for run in r.get("run_records", []):
            if "plan" in run:
                plan_log.append({
                    "question_index": r["idx"],
                    "run":            run.get("run", 1),
                    "question":       r["question"],
                    "execution_plan": run["plan"],
                })
    with open(plans, 'w', encoding='utf-8') as f:
        json.dump(plan_log, f, indent=2, ensure_ascii=False)

    print(f"\nReport scritti:")
    print(f"  Dettaglio:  {detail}")
    print(f"  Sommario:   {summary}")
    print(f"  Piani JSON: {plans}  (compatibile con plan_validator.py)")


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Test runner v2 per LADARAG-Extended")
    p.add_argument("--control-url",     default=DEFAULTS["control_url"])
    p.add_argument("--csv",             default=DEFAULTS["csv_file"], dest="csv_file")
    p.add_argument("--output-dir",      default=DEFAULTS["output_dir"])
    p.add_argument("--timeout",         default=DEFAULTS["timeout"], type=int)
    p.add_argument("--delay",           default=DEFAULTS["delay"], type=float)
    p.add_argument("--runs",            default=DEFAULTS["runs"], type=int,
                   help="Forza N run per tutte le query (default: adattivo per categoria)")
    p.add_argument("--only-categories", nargs="+", default=None, metavar="CAT")
    p.add_argument("--only-noise",       nargs="+", default=None, metavar="NOISE",
                   help="Esegui solo query con i tipi di rumore specificati "
                        "(es: typo ambiguous multilingual-it minimal)")
    p.add_argument("--fail-fast",       action="store_true")
    p.add_argument("--no-report",       action="store_true")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    data = run_tests(args)
    print_summary(data)
    if not args.no_report:
        write_reports(data, args.output_dir)