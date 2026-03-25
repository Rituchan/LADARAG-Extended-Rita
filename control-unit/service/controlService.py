from service.discoveryService import Discovery
from flask import Response
import json
import requests
import re
import aiohttp
import asyncio
import os
import mimetypes
import shutil
import time


# ─────────────────────────────────────────────────────────────────────────────
# PLAN VALIDATOR
# Valida il piano JSON prima dell'esecuzione per bloccare errori a monte.
# ─────────────────────────────────────────────────────────────────────────────

class PlanValidator:
    """
    Valida struttura e coerenza del piano generato dall'LLM.

    Controlli effettuati:
    - Presenza e tipo di 'tasks'
    - Campi obbligatori per ogni task
    - service_id presenti nella lista dei servizi disponibili
    - URL che inizia con http:// (o placeholder valido)
    - Placeholder {id} non risolti nell'URL
    - Operation valida (GET/POST/PUT/DELETE)
    - Placeholder {{task.field}} che referenziano solo task già definiti
    """

    VALID_OPERATIONS = {"GET", "POST", "PUT", "DELETE"}

    @staticmethod
    def validate(plan: dict, available_service_ids: list) -> tuple[bool, list[str]]:
        errors = []

        if not isinstance(plan, dict) or "tasks" not in plan:
            return False, ["Missing or invalid 'tasks' array"]

        tasks = plan["tasks"]
        if not isinstance(tasks, list) or len(tasks) == 0:
            return False, ["Empty tasks array"]

        service_id_set = set(available_service_ids)
        defined_task_names = set()

        for i, task in enumerate(tasks):
            prefix = f"Task {i} ({task.get('task_name', 'unnamed')})"

            if not isinstance(task, dict):
                errors.append(f"{prefix}: not a dictionary")
                continue

            # Campi obbligatori
            for field in ("task_name", "service_id", "url", "operation", "input"):
                if field not in task:
                    errors.append(f"{prefix}: missing required field '{field}'")

            # service_id valido
            sid = task.get("service_id", "")
            if sid and sid not in service_id_set:
                errors.append(f"{prefix}: unknown service_id '{sid}'")

            # URL
            url = str(task.get("url", ""))
            if not url:
                errors.append(f"{prefix}: empty url")
            elif not url.startswith("http") and "{{" not in url:
                errors.append(f"{prefix}: url must start with http:// (got '{url[:60]}')")

            # Placeholder non risolti ({id} invece di {{task.id}})
            if re.search(r'(?<!\{)\{(?!\{)[^{]*\}(?!\})', url):
                errors.append(f"{prefix}: unresolved path parameter in url '{url[:60]}'")

            # Operation
            op = task.get("operation", "")
            if op and op not in PlanValidator.VALID_OPERATIONS:
                errors.append(f"{prefix}: invalid operation '{op}'")

            # Chaining — i placeholder devono referenziare task già definiti
            task_str = json.dumps(task)
            for ref in re.findall(r'\{\{(\w+)[\.\[]', task_str):
                if ref not in defined_task_names:
                    errors.append(f"{prefix}: references undefined task '{ref}'")

            defined_task_names.add(task.get("task_name", ""))

        return len(errors) == 0, errors


class Controller:

    def __init__(self):
        self.model_name = os.environ.get("LLM_MODEL", "phi4:14b")
        os.makedirs("Files", exist_ok=True)

    # ------------------------------------------------------------------
    # ANALISI FILE ALLEGATI
    # ------------------------------------------------------------------

    def analyze_files(self, files: list):
        analyzed = []
        if files:
            for f in files:
                filename = f.filename
                content_type = f.mimetype or mimetypes.guess_type(filename)[0]
                size = f.content_length
                path = os.path.join("Files", filename)
                f.save(path)

                file_info = {
                    "filename": filename,
                    "content_type": content_type,
                    "size": size,
                    "path": path
                }

                if content_type == "application/pdf":
                    file_info["category"] = "document"
                elif content_type and content_type.startswith("image/"):
                    file_info["category"] = "image"
                elif content_type in ["text/csv", "application/vnd.ms-excel"]:
                    file_info["category"] = "tabular"
                else:
                    file_info["category"] = "unknown"

                analyzed.append(file_info)
        return analyzed

    # ------------------------------------------------------------------
    # QUERY OLLAMA — usa /api/chat + format:json
    # ------------------------------------------------------------------

    def query_ollama(self, system_prompt: str, user_prompt: str) -> tuple[str, float]:
        """
        Invia system_prompt e user_prompt come messaggi separati a /api/chat.
        Ollama applica automaticamente il template corretto per il modello
        (Phi-4, Llama, Qwen, Mistral, ecc.) senza dover hardcodare i tag.

        format="json" forza l'output a essere JSON sintatticamante valido:
        elimina testo libero, virgole trailing, commenti e troncamenti.
        Il contenuto rimane responsabilità del modello e dei few-shot examples.
        """
        url = os.environ.get("OLLAMA_API_URL", "http://localhost:11434")
        try:
            start_time = time.perf_counter()
            response = requests.post(
                f"{url}/api/chat",
                json={
                    "model": self.model_name,
                    "format": "json",
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user",   "content": user_prompt}
                    ],
                    "options": {
                        "temperature": 0.0,
                        "max_tokens": 4096,
                        "num_ctx": 8192,
                    },
                    "stream": False
                }
            )
            response.raise_for_status()
            end_time = time.perf_counter()

            latency = end_time - start_time
            data = response.json()
            # /api/chat restituisce il testo in message.content
            return data["message"]["content"].strip(), latency

        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"[HTTP ERROR] Errore nella richiesta a Ollama: {e}")
        except (ValueError, KeyError) as e:
            raise RuntimeError(f"[PARSE ERROR] Risposta non valida da Ollama: {e}")

    # ------------------------------------------------------------------
    # COSTRUZIONE PROMPT — system e user separati
    # ------------------------------------------------------------------

    def _build_system_prompt(self) -> str:
        """
        Costruisce il system prompt statico: regole, anti-pattern e
        three few-shot examples che coprono i pattern più comuni.

        Separare il system prompt dall'user prompt ha due vantaggi:
        1. Ollama lo passa al modello con il ruolo corretto (non come testo utente).
        2. In futuro si può cachare o versionare indipendentemente dai dati.
        """

        # --- Few-shot examples ---
        # Tre pattern che coprono la grande maggioranza delle query:
        # GET semplice, POST con dati, chaining GET+FIND+PUT.
        # Gli LLM non-reasoning imparano per imitazione: più esempi = meno errori.

        example_get = {
            "tasks": [{
                "task_name": "get_all_bins",
                "service_id": "smart-bins-mock",
                "url": "http://mock-server:8080/rest/Smart+Bins+API/1.0/bin",
                "operation": "GET",
                "input": ""
            }]
        }

        example_post = {
            "tasks": [{
                "task_name": "create_report",
                "service_id": "smart-citizens-reports-mock",
                "url": "http://mock-server:8080/rest/Smart+Citizens+Reports+API/1.0/citizen-report",
                "operation": "POST",
                "input": {
                    "reporterName": "Mario Rossi",
                    "location": "Piazza Castello",
                    "category": "sanitation",
                    "description": "Cestino pieno",
                    "status": "open",
                    "submittedAt": "2025-09-25T10:00:00Z"
                }
            }]
        }

        example_chain = {
            "tasks": [
                {
                    "task_name": "get_all_lights",
                    "service_id": "smart-lighting-mock",
                    "url": "http://mock-server:8080/rest/Smart+Lighting+API/1.0/light",
                    "operation": "GET",
                    "input": ""
                },
                {
                    "task_name": "turn_off_light",
                    "service_id": "smart-lighting-mock",
                    "url": "http://mock-server:8080/rest/Smart+Lighting+API/1.0/light/{{get_all_lights.FIND(location=Corso Garibaldi).id}}",
                    "operation": "PUT",
                    "input": {
                        "location": "Corso Garibaldi - Palo 12, Benevento",
                        "status": "off",
                        "brightness": 0,
                        "energyConsumption": 0.0,
                        "smartEnabled": True
                    }
                }
            ]
        }

        examples_str = (
            f"EXAMPLE 1 — Simple GET:\n{json.dumps(example_get, indent=2)}\n\n"
            f"EXAMPLE 2 — POST with body:\n{json.dumps(example_post, indent=2)}\n\n"
            f"EXAMPLE 3 — GET + FIND + PUT chaining:\n{json.dumps(example_chain, indent=2)}"
        )

        return f"""You are an API orchestrator for a Smart City system.
Given a user query and a catalog of available services, you produce ONLY a valid JSON execution plan.
No markdown, no explanation, no text outside the JSON object.

CRITICAL ANTI-PATTERN — NEVER create tasks with empty URLs:
BAD:
  Task 1: {{"task_name": "get_lights", "url": "http://mock-server.../light"}}
  Task 2: {{"task_name": "find_light", "url": ""}}          ← PROHIBITED
  Task 3: {{"url": ".../{{{{find_light.id}}}}"}}

GOOD:
  Task 1: {{"task_name": "get_lights", "url": "http://mock-server.../light"}}
  Task 2: {{"url": ".../{{{{get_lights.FIND(location=Street).id}}}}"}}

TEMPLATES — study these patterns carefully:
{examples_str}

RULES (in order of priority):
1. Use ONLY the provided services, endpoints and schemas. Never invent.
2. Tasks execute sequentially. Use {{{{task_name.field}}}} for chaining.
3. To filter arrays use: {{{{task_name.FIND(key=value).field}}}}.
4. To access array element by position use ONLY the dot-index syntax: {{{{task_name.0.field}}}} for first, {{{{task_name.1.field}}}} for second (0-based).
   NEVER use N(0), [0], .first, .get(0) or any other syntax — they will not work.
5. Path parameters MUST be injected directly into the url string, not in input.
   WRONG: "url": "/light/{{{{id}}}}", "input": {{"id": "{{{{task.id}}}}"}}
   RIGHT:  "url": "/light/{{{{get_lights.FIND(location=X).id}}}}", "input": ""
6. Use RESPONSE SCHEMAS for exact field names when chaining.
7. Use REQUEST SCHEMAS for POST/PUT bodies. Fields marked * are required.
8. operation must be exactly one of: GET, POST, PUT, DELETE.
9. Every task MUST have a non-empty url.
10. Required keys per task: task_name, service_id, url, operation, input.
11. NEVER create an intermediate GET-by-ID task if the ID is already available from a previous GET-all via FIND or index. Use {{{{task_name.FIND(key=value).field}}}} or {{{{task_name.0.field}}}} directly in the next task url."""

    def _build_user_prompt(self, discovered_services, discovered_capabilities,
                           discovered_endpoints, discovered_schemas,
                           discovered_request_schemas, query, input_files=None) -> str:
        """
        Costruisce la parte variabile del prompt con un context compatto:
        capabilities ed endpoints sono uniti per servizio invece di passarli
        come liste separate. Riduce i token e rende la struttura più leggibile
        per il modello, che non deve incrociare due liste parallele.
        """
        lines = ["SERVICES AND ENDPOINTS:"]

        for i, service in enumerate(discovered_services):
            sid  = service.get("_id", "")
            name = service.get("name", "")
            lines.append(f"\n[{sid}] {name}")

            caps = discovered_capabilities[i] if i < len(discovered_capabilities) else {}
            eps  = discovered_endpoints[i]    if i < len(discovered_endpoints)    else {}
            rsch = discovered_schemas[i]      if i < len(discovered_schemas)      else {}
            qsch = discovered_request_schemas[i] if i < len(discovered_request_schemas) else {}

            for key in caps:
                if key == "POST /register":
                    continue
                ep_url = eps.get(key, "N/A")
                desc   = caps[key]
                lines.append(f"  {key}")
                lines.append(f"    URL: {ep_url}")
                lines.append(f"    DESC: {desc}")
                if key in rsch:
                    lines.append(f"    RESPONSE SCHEMA: {rsch[key]}")
                if key in qsch:
                    lines.append(f"    REQUEST SCHEMA (* = required): {qsch[key]}")

        context = "\n".join(lines)

        files_str = str(input_files) if input_files else "none"

        return f"""{context}

FILES:
{files_str}

QUERY:
{query}"""

    # ------------------------------------------------------------------
    # DECOMPOSIZIONE TASK
    # ------------------------------------------------------------------

    def decompose_task(self, discovered_services, discovered_capabilities, discovered_endpoints,
                       discovered_schemas, discovered_request_schemas, query, input_files=None):
        """
        Costruisce system e user prompt separati e interroga l'LLM tramite
        query_ollama che usa /api/chat + format:json.
        """
        system_prompt = self._build_system_prompt()
        user_prompt   = self._build_user_prompt(
            discovered_services, discovered_capabilities, discovered_endpoints,
            discovered_schemas, discovered_request_schemas, query, input_files
        )

        response, plan_latency = self.query_ollama(system_prompt, user_prompt)
        print(f"[LLM RESPONSE] {response}")
        print("=" * 100)
        return response, plan_latency

    # ------------------------------------------------------------------
    # PARSING JSON — 3 livelli di fallback
    # ------------------------------------------------------------------

    def extract_agents(self, agents_json):
        """
        Estrae il piano JSON dalla risposta dell'LLM.

        Con format:json attivo la risposta dovrebbe essere già JSON puro.
        I fallback rimangono per gestire edge case e modelli che ignorano il flag.

        Livello 1: parse diretto (caso normale con format:json).
        Livello 2: estrazione dopo </think> (modelli reasoning con CoT esplicito).
        Livello 3: ricerca del primo blocco JSON valido nell'intera stringa.
        """
        plan = {}

        def try_parse_json_block(text):
            start = text.find('{')
            end   = text.rfind('}') + 1
            if start != -1 and end > start:
                candidate = text[start:end].strip()
                try:
                    return json.loads(candidate), candidate
                except json.JSONDecodeError:
                    pass
            return None, ""

        # Livello 1: parse diretto — dovrebbe sempre funzionare con format:json
        try:
            result = json.loads(agents_json)
            if isinstance(result, dict):
                print("[PARSE] Piano estratto con parse diretto (format:json).")
                return result
        except json.JSONDecodeError:
            pass

        # Livello 2: dopo </think> (phi4-reasoning e modelli CoT)
        think_match = re.search(r'</think>', agents_json, flags=re.IGNORECASE) 
        if think_match:
            after_think = agents_json[think_match.end():].strip()
            result, _ = try_parse_json_block(after_think)
            if result is not None:
                print("[PARSE] Piano estratto dopo </think>.")
                return result
            print("[WARN] </think> trovato ma nessun JSON valido dopo. Fallback...")

        # Livello 3: ricerca JSON nell'intera stringa
        result, _ = try_parse_json_block(agents_json)
        if result is not None:
            print("[PARSE] Piano estratto dalla risposta grezza.")
            return result

        # Fallback finale
        preview = agents_json[:300].replace('\n', ' ') if agents_json else "(risposta vuota)"
        print(f"[FORMAT ERROR] Nessun JSON valido trovato nella risposta LLM.")
        print(f"[FORMAT ERROR] Anteprima: {preview}")
        return plan

    # ------------------------------------------------------------------
    # RISOLUZIONE PLACEHOLDER
    # ------------------------------------------------------------------

    def resolve_placeholders(self, data, context):
        """
        Sostituisce i placeholder {{task.field}}, {{task.FIND(k=v).field}},
        {{task.N.field}} con i valori reali dal contesto di esecuzione.
        """
        if isinstance(data, str):
            matches = re.findall(r'\{\{(.*?)\}\}', data) # trova tutti i placeholder {{...}} nella stringa

            # CASO 1: la stringa è esattamente un placeholder → preserva il tipo originale
            if len(matches) == 1 and data.strip() == f"{{{{{matches[0]}}}}}":
                match       = matches[0]
                match_clean = match.replace(".output", "").replace(".response", "").replace(".data", "")
                match_clean = re.sub(r'\[(\d+)\]', r'.\1', match_clean)
                parts       = match_clean.split('.')
                task_name   = parts[0]
                val         = context.get(task_name, {})

                for part in parts[1:]:
                    if part.startswith("FIND(") and part.endswith(")"):
                        condition    = part[5:-1]
                        if "=" in condition and isinstance(val, list):
                            key, expected_val = condition.split("=", 1)
                            key           = key.strip()
                            expected_val  = expected_val.strip().lower().replace('+', ' ').replace('%20', ' ')
                            found         = next(
                                (item for item in val
                                 if isinstance(item, dict)
                                 and expected_val in str(item.get(key, "")).lower()),
                                None
                            )
                            if not found:
                                print(f"[AUTO-CORREZIONE] Chiave '{key}' non trovata, ricerca globale...")
                                for item in val:
                                    if isinstance(item, dict) and any(
                                        expected_val in str(v).lower()
                                        for k, v in item.items() if v is not None
                                    ):
                                        found = item
                                        print(f"[AUTO-CORREZIONE] Match trovato: {found}")
                                        break
                            val = found if found else ""
                        else:
                            val = ""
                    elif isinstance(val, dict):
                        val = val.get(part, "")
                    elif isinstance(val, list) and part.isdigit():
                        idx = int(part)
                        val = val[idx] if 0 <= idx < len(val) else ""
                    elif isinstance(val, list) and re.match(r'^N\((\d+)\)$', part):
                        # Fallback: riconosce N(0), N(1), ecc. — sintassi errata ma recuperabile
                        idx = int(re.match(r'^N\((\d+)\)$', part).group(1))
                        val = val[idx] if 0 <= idx < len(val) else ""
                    else:
                        val = ""
                return val

            # CASO 2: placeholder mescolati con testo (es. URL con parametro)
            for match in matches:
                match_clean = match.replace(".output", "").replace(".response", "").replace(".data", "")
                match_clean = re.sub(r'\[(\d+)\]', r'.\1', match_clean)
                parts       = match_clean.split('.')
                task_name   = parts[0]
                val         = context.get(task_name, {})

                for part in parts[1:]:
                    if part.startswith("FIND(") and part.endswith(")"):
                        condition = part[5:-1]
                        if "=" in condition and isinstance(val, list):
                            key, expected_val = condition.split("=", 1)
                            key           = key.strip()
                            expected_val  = expected_val.strip().lower().replace('+', ' ').replace('%20', ' ')
                            found         = next(
                                (item for item in val
                                 if isinstance(item, dict)
                                 and expected_val in str(item.get(key, "")).lower()),
                                None
                            )
                            if not found:
                                print(f"[AUTO-CORREZIONE] Chiave '{key}' non trovata, ricerca globale...")
                                for item in val:
                                    if isinstance(item, dict) and any(
                                        expected_val in str(v).lower()
                                        for k, v in item.items() if v is not None
                                    ):
                                        found = item
                                        print(f"[AUTO-CORREZIONE] Match trovato: {found}")
                                        break
                            val = found if found else ""
                        else:
                            val = ""
                    elif isinstance(val, dict):
                        val = val.get(part, "")
                    elif isinstance(val, list) and part.isdigit():
                        idx = int(part)
                        val = val[idx] if 0 <= idx < len(val) else ""
                    elif isinstance(val, list) and re.match(r'^N\((\d+)\)$', part):
                        # Fallback: riconosce N(0), N(1), ecc. — sintassi errata ma recuperabile
                        idx = int(re.match(r'^N\((\d+)\)$', part).group(1))
                        val = val[idx] if 0 <= idx < len(val) else ""
                    else:
                        val = ""

                if isinstance(val, (dict, list)):
                    val = json.dumps(val)

                data = data.replace(f"{{{{{match}}}}}", str(val))
            return data

        elif isinstance(data, dict):
            return {k: self.resolve_placeholders(v, context) for k, v in data.items()}
        elif isinstance(data, list):
            return [self.resolve_placeholders(item, context) for item in data]
        return data

    # ------------------------------------------------------------------
    # ESECUZIONE SINGOLO TASK
    # ------------------------------------------------------------------

    async def call_agent(self, session, task, discovered_services):
        """
        Esegue un singolo task atomico generato dall'LLM in modo asincrono.
        Normalizza i campi del task, risolve URL malformati e gestisce
        file multipart e payload JSON.
        """
        task_name  = task.get("task_name") or task.get("name") or task.get("taskName") or "unnamed_task"
        endpoint   = task.get("url") or task.get("endpoint") or ""
        input_data = task.get("input", "")
        operation  = str(task.get("operation") or task.get("method") or "GET").upper()

        # Pulizia: rimuove metodo prefisso (es. "POST http://...")
        if " " in endpoint:
            endpoint = endpoint.split(" ")[-1]

        # Auto-correzione: aggiunge http:// se mancante
        if endpoint and not endpoint.startswith("http"):
            mock_url = os.environ.get("MOCK_SERVER_URL", "http://mock-server:8080")
            endpoint = f"{mock_url}{endpoint}" if endpoint.startswith("/") else f"{mock_url}/{endpoint}"

        # Forzatura porta 8080 se omessa
        if endpoint and "mock-server/" in endpoint:
            endpoint = endpoint.replace("mock-server/", "mock-server:8080/")

        # Skip task fantasma (URL vuoto)
        if not endpoint or str(endpoint).strip() == "":
            print(f"[WARN] Task fantasma '{task_name}' ignorato (nessun endpoint).")
            return {
                "task_name": task_name,
                "operation": operation,
                "status": "SUCCESS",
                "status_code": 200,
                "result": {"skipped": True, "message": "Dummy task ignored"}
            }

        response_result = {
            "task_name": task_name,
            "operation": operation
        }

        # Gestione tag multimodali [FILE] / [TEXT]
        tag_pattern = r"\[(\w+)\](.*?)\[/\1\]"
        payload   = input_data
        is_file   = False
        file_path = None
        filename  = None

        if isinstance(input_data, str):
            match = re.search(tag_pattern, input_data, re.DOTALL)
            if match:
                tag     = match.group(1)
                content = match.group(2)
                if tag == "TEXT":
                    try:
                        payload = json.loads(content)
                    except Exception:
                        payload = content
                elif tag == "FILE":
                    filename  = content
                    file_path = os.path.join("Files", filename)
                    if not os.path.exists(file_path):
                        response_result.update({
                            "status": "ERROR", "status_code": 404,
                            "result": f"File '{filename}' non trovato"
                        })
                        return response_result
                    is_file = True

        try:
            request_kwargs = {"timeout": 5}

            if is_file:
                form = aiohttp.FormData()
                form.add_field("file", open(file_path, "rb"),
                               filename=filename, content_type="application/octet-stream")
                request_kwargs["data"] = form
            else:
                if payload is not None and payload != "":
                    request_kwargs["json"] = payload

            if operation == "POST":
                resp_ctx = session.post(endpoint, **request_kwargs)
            elif operation == "PUT":
                resp_ctx = session.put(endpoint, **request_kwargs)
            elif operation == "GET":
                resp_ctx = session.get(endpoint, timeout=5)
            elif operation == "DELETE":
                resp_ctx = session.delete(endpoint, timeout=5)
            else:
                raise ValueError(f"HTTP Operation non supportata: {operation}")

            async with resp_ctx as resp:
                status       = resp.status
                content_type = resp.headers.get("Content-Type", "")

                if 200 <= status < 300:
                    if content_type.startswith("application/pdf") or content_type.startswith("image/"):
                        return {
                            "status": "FILE",
                            "status_code": status,
                            "headers": dict(resp.headers),
                            "body": await resp.read()
                        }

                    raw_text = await resp.text()

                    if not raw_text.strip():
                        print(f"[WARN] Task '{task_name}': risposta {status} con body vuoto.")
                        result = None
                    elif "application/json" in content_type:
                        try:
                            result = json.loads(raw_text)
                        except json.JSONDecodeError:
                            print(f"[WARN] JSON dichiarato ma parsing fallito. Tratto come testo.")
                            result = raw_text
                    else:
                        result = raw_text

                    print(f"[SUCCESS] Task '{task_name}' completed.")
                    response_result.update({"status": "SUCCESS", "status_code": status, "result": result})

                else:
                    error_text = await resp.text()
                    print(f"[ERROR] Task '{task_name}' failed with status {status}: {error_text}")
                    response_result.update({"status": "ERROR", "status_code": status, "result": error_text})

        except Exception as e:
            print(f"[EXCEPTION] Task '{task_name}' fallito: {e}")
            response_result.update({"status": "EXCEPTION", "status_code": 500, "result": str(e)})

        return response_result

    # ------------------------------------------------------------------
    # ORCHESTRAZIONE ASINCRONA
    # ------------------------------------------------------------------

    async def trigger_agents_async(self, agents: dict, discovered_services):
        tasks   = agents.get("tasks", [])
        results = []
        execution_context = {}

        async with aiohttp.ClientSession() as session:
            for task in tasks:
                task_url      = task.get("url") or task.get("endpoint") or ""
                task["url"]   = self.resolve_placeholders(task_url, execution_context)
                if task.get("input"):
                    task["input"] = self.resolve_placeholders(task.get("input"), execution_context)

                result = await self.call_agent(session, task, discovered_services)

                if result.get("status") == "FILE":
                    return result

                results.append(result)

                safe_task_name = task.get("task_name") or task.get("name") or "unnamed_task"

                if result.get("status") == "SUCCESS":
                    execution_context[safe_task_name] = result.get("result", {})
                else:
                    print(f"[CHAIN BROKEN] Task '{safe_task_name}' fallito. Interruzione pipeline.")
                    break

        return results

    def trigger_agents(self, agents: dict, discovered_services):
        return asyncio.run(self.trigger_agents_async(agents, discovered_services))

    # ------------------------------------------------------------------
    # UTILITÀ
    # ------------------------------------------------------------------

    def replace_endpoints(self, endpoints_list, mock_server_address):
        updated = []
        for endpoint_dict in endpoints_list:
            new_dict = {}
            for k, v in endpoint_dict.items():
                if isinstance(v, str):
                    new_url = re.sub(r"http://localhost:8585", mock_server_address, v)
                    new_dict[k] = new_url
                else:
                    new_dict[k] = v
            updated.append(new_dict)
        return updated

    def _attempt_auto_fix(self, plan: dict) -> dict:
        """
        Auto-correzione per errori comuni e recuperabili nel piano.
        Applicata dopo la validazione solo se ci sono errori non bloccanti.

        Fix applicati:
        - Aggiunge http://mock-server:8080 agli URL senza schema
        - Normalizza operation a uppercase
        - Rimuove task a cui mancano campi critici (task_name, service_id, url, operation)
        """
        if "tasks" not in plan:
            return plan

        mock_url    = os.environ.get("MOCK_SERVER_URL", "http://mock-server:8080")
        fixed_tasks = []

        for task in plan["tasks"]:
            if not isinstance(task, dict):
                continue

            # Fix URL senza schema
            url = str(task.get("url", ""))
            if url and not url.startswith("http") and not url.startswith("{{"):
                prefix = mock_url if url.startswith("/") else mock_url + "/"
                task["url"] = prefix + url.lstrip("/")
                print(f"[AUTO-FIX] URL corretto: {task['url']}")

            # Fix operation lowercase
            if "operation" in task and isinstance(task["operation"], str):
                task["operation"] = task["operation"].upper()

            # Scarta task con campi critici mancanti
            if all(f in task for f in ("task_name", "service_id", "url", "operation")):
                fixed_tasks.append(task)
            else:
                print(f"[AUTO-FIX] Task scartato (campi mancanti): {task.get('task_name', 'unnamed')}")

        plan["tasks"] = fixed_tasks
        return plan

    # ------------------------------------------------------------------
    # ENTRY POINT
    # ------------------------------------------------------------------

    def control(self, query, files=None):
        input_files    = files or []
        analyzed_files = self.analyze_files(input_files)

        catalog_url         = os.environ.get("CATALOG_URL",    "http://catalog-gateway:5000")
        registry_url        = os.environ.get("REGISTRY_URL",   "http://registry:8500")
        mock_server_address = os.environ.get("MOCK_SERVER_URL", "http://mock-server:8080")

        registry = Discovery(registry_url)

        discovered_services        = []
        discovered_capabilities    = []
        discovered_endpoints       = []
        discovered_schemas         = []
        discovered_request_schemas = []

        services     = registry.services()

        input_data   = {"query": query}
        service_data = requests.post(f"{catalog_url}/index/search", json=input_data)
        service_data = service_data.json()
        service_list = service_data["results"]

        if not service_list:
            return {"execution_plan": {}, "execution_results": [], "error": "No services matched the query"}

        registry_service_ids  = set(s["id"] for s in services)
        filtered_service_list = [s for s in service_list if s["_id"] in registry_service_ids]
        orphaned_services     = [s for s in service_list if s["_id"] not in registry_service_ids]

        if orphaned_services:
            print("[WARNING] Servizi trovati via semantic search ma non più nel registry:")
            for s in orphaned_services:
                print(f"  - {s.get('_id')} : {s.get('name')}")

        if not filtered_service_list:
            return {
                "execution_plan": {}, "execution_results": [],
                "error": "None of the discovered services are currently available in the registry"
            }

        for service in filtered_service_list:
            discovered_services.append({
                "_id":         service.get("_id"),
                "name":        service.get("name"),
                "description": service.get("description"),
            })
            discovered_capabilities.append(service.get("capabilities", {}))
            discovered_endpoints.append(service.get("endpoints", {}))
            discovered_schemas.append(service.get("response_schemas", {}))
            discovered_request_schemas.append(service.get("request_schemas", {}))

        discovered_endpoints = self.replace_endpoints(discovered_endpoints, mock_server_address)

        plan_json, plan_latency = self.decompose_task(
            discovered_services,
            discovered_capabilities,
            discovered_endpoints,
            discovered_schemas,
            discovered_request_schemas,
            query,
            analyzed_files
        )
        plan = self.extract_agents(plan_json)

        # Validazione pre-esecuzione
        available_ids = [s["_id"] for s in discovered_services]
        is_valid, validation_errors = PlanValidator.validate(plan, available_ids)

        if not is_valid:
            print(f"[VALIDATION] {len(validation_errors)} errore/i trovato/i:")
            for err in validation_errors:
                print(f"  - {err}")
            plan = self._attempt_auto_fix(plan)
            # Rivalida dopo auto-fix
            is_valid, validation_errors = PlanValidator.validate(plan, available_ids)
            if not is_valid:
                print("[VALIDATION] Piano non recuperabile dopo auto-fix.")
                return {
                    "execution_plan":          plan,
                    "execution_results":       [],
                    "validation_errors":       validation_errors,
                    "plan_generation_latency": plan_latency
                }
            print("[VALIDATION] Piano recuperato con auto-fix.")
        else:
            print(f"[VALIDATION] Piano valido ({len(plan.get('tasks', []))} task/s).")

        results = self.trigger_agents(plan, discovered_services)

        if isinstance(results, dict) and results.get("status") == "FILE":
            return Response(results["body"], status=results["status_code"], headers=results["headers"])

        # Pulizia cartella Files
        if os.path.exists('Files'):
            for filename in os.listdir('Files'):
                file_path = os.path.join('Files', filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    print(f'Failed to delete {file_path}. Reason: {e}')

        return {
            "execution_plan":          plan,
            "execution_results":       results,
            "plan_generation_latency": plan_latency
        }