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
# ─────────────────────────────────────────────────────────────────────────────

class PlanValidator:

    '''Valida il piano di esecuzione generato dal modello rispetto a un insieme di regole predefinite, 
    restituendo un booleano di validità e una lista di errori se non valido.'''

    VALID_OPERATIONS = {"GET", "POST", "PUT", "DELETE"}

    @staticmethod
    def validate(plan: dict, available_service_ids: list) -> tuple[bool, list[str]]:
        errors = []

        # Regola 1: il piano deve essere un dizionario con chiave "tasks" che contiene una lista non vuota di task
        if not isinstance(plan, dict) or "tasks" not in plan:
            return False, ["Missing or invalid 'tasks' array"]
        
        # Regola 2: ogni task deve essere un dizionario con i campi richiesti e validi, e i placeholder devono referenziare task già definiti
        tasks = plan["tasks"]
        if not isinstance(tasks, list) or len(tasks) == 0:
            return False, ["Empty tasks array"]

        service_id_set     = set(available_service_ids)
        defined_task_names = set()

        for i, task in enumerate(tasks):
            prefix = f"Task {i} ({task.get('task_name', 'unnamed')})"

            if not isinstance(task, dict):
                errors.append(f"{prefix}: not a dictionary")
                continue
            
            for field in ("task_name", "service_id", "url", "operation", "input"):
                if field not in task:
                    errors.append(f"{prefix}: missing required field '{field}'")

            sid = task.get("service_id", "")
            if sid and sid not in service_id_set:
                errors.append(f"{prefix}: unknown service_id '{sid}'")

            url = str(task.get("url", ""))
            if not url:
                errors.append(f"{prefix}: empty url")
            elif not url.startswith("http") and "{{" not in url:
                errors.append(f"{prefix}: url must start with http:// (got '{url[:60]}')")

            # Controlla placeholder singoli non risolti {id} — escludi prima i {{...}} validi
            clean_url = re.sub(r'\{\{.*?\}\}', '', url)
            if re.search(r'(?<!\{)\{(?!\{)[^{]*\}(?!\})', clean_url):
                errors.append(f"{prefix}: unresolved path parameter in url '{url[:60]}'")

            op = task.get("operation", "")
            if op and op not in PlanValidator.VALID_OPERATIONS:
                errors.append(f"{prefix}: invalid operation '{op}'")

            # Chaining: i placeholder devono referenziare task già definiti
            task_str = json.dumps(task)
            for ref in re.findall(r'\{\{(\w+)[\.\[]', task_str):
                if ref not in defined_task_names:
                    errors.append(f"{prefix}: references undefined task '{ref}'")

            defined_task_names.add(task.get("task_name", ""))

        return len(errors) == 0, errors


# ─────────────────────────────────────────────────────────────────────────────
# CONTROLLER
# ─────────────────────────────────────────────────────────────────────────────

class Controller:

    def __init__(self):
        self.model_name = os.environ.get("LLM_MODEL", "phi4-reasoning:14b")
        os.makedirs("Files", exist_ok=True)

    # ------------------------------------------------------------------
    # FILE ALLEGATI
    # ------------------------------------------------------------------

    def analyze_files(self, files: list):
        analyzed = []
        for f in files:
            filename     = f.filename
            content_type = f.mimetype or mimetypes.guess_type(filename)[0]
            path         = os.path.join("Files", filename)
            f.save(path)

            category = "unknown"
            if content_type == "application/pdf":
                category = "document"
            elif content_type and content_type.startswith("image/"):
                category = "image"
            elif content_type in ["text/csv", "application/vnd.ms-excel"]:
                category = "tabular"

            analyzed.append({
                "filename":     filename,
                "content_type": content_type,
                "size":         f.content_length,
                "path":         path,
                "category":     category,
            })
        return analyzed

    # ------------------------------------------------------------------
    # LLM
    # ------------------------------------------------------------------

    '''
     Interroga Ollama con un prompt di sistema e uno utente, restituendo la risposta testuale e la latenza.
    '''
    def query_ollama(self, system_prompt: str, user_prompt: str) -> tuple[str, float]:
        url = os.environ.get("OLLAMA_API_URL", "http://localhost:11434")
        try:
            t0       = time.perf_counter()
            response = requests.post(
                f"{url}/api/chat",
                json={
                    "model":   self.model_name,
                    "format":  "json",
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user",   "content": user_prompt},
                    ],
                    "options": {"temperature": 0.0, "max_tokens": 4096, "num_ctx": 8192},
                    "stream":  False,
                },
            )
            response.raise_for_status()
            return response.json()["message"]["content"].strip(), time.perf_counter() - t0
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"[HTTP ERROR] {e}")
        except (ValueError, KeyError) as e:
            raise RuntimeError(f"[PARSE ERROR] {e}")

    # ------------------------------------------------------------------
    # PROMPT
    # ------------------------------------------------------------------

    def _build_system_prompt(self) -> str:
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
4. To access array element by position: {{{{task_name.0.field}}}} (0-based). NEVER use N(0), [0], .first.
5. Path parameters AND query parameters MUST be injected directly into the url string, not in input.
   WRONG: "url": "/light", "input": {{"zoneId": "Z-CENTRO"}}
   RIGHT: "url": "/light?zoneId=Z-CENTRO", "input": ""
6. Use RESPONSE SCHEMAS for exact field names when chaining.
7. Use REQUEST SCHEMAS for POST/PUT bodies. Fields marked * are required.
8. operation must be exactly one of: GET, POST, PUT, DELETE.
9. Every task MUST have a non-empty url.
10. Required keys per task: task_name, service_id, url, operation, input.
11. NEVER create an intermediate GET-by-ID if the ID is already available via FIND or index.
12. Check PARAMETERS for GET/PUT/DELETE. Parameters marked * are REQUIRED and must appear in the url.
13. Use optional parameters ONLY when the user explicitly mentions a value for them. Never invent filter values."""

    def _build_user_prompt(self, discovered_services, discovered_capabilities,
                           discovered_endpoints, discovered_schemas,
                           discovered_request_schemas, discovered_parameters,
                           query, input_files=None) -> str:
        lines = ["SERVICES AND ENDPOINTS:"]

        for i, service in enumerate(discovered_services):
            lines.append(f"\n[{service.get('_id')}] {service.get('name')}")

            caps   = discovered_capabilities[i]   if i < len(discovered_capabilities)   else {}
            eps    = discovered_endpoints[i]       if i < len(discovered_endpoints)       else {}
            rsch   = discovered_schemas[i]         if i < len(discovered_schemas)         else {}
            qsch   = discovered_request_schemas[i] if i < len(discovered_request_schemas) else {}
            params = discovered_parameters[i]      if i < len(discovered_parameters)      else {}

            for key in caps:
                if key == "POST /register":
                    continue
                lines.append(f"  {key}")
                lines.append(f"    URL: {eps.get(key, 'N/A')}")
                lines.append(f"    DESC: {caps[key]}")
                if params.get(key):
                    lines.append(f"    PARAMETERS (* = required): {params[key]}")
                if rsch.get(key):
                    lines.append(f"    RESPONSE SCHEMA: {rsch[key]}")
                if qsch.get(key):
                    lines.append(f"    REQUEST SCHEMA (* = required): {qsch[key]}")

        return f"""{chr(10).join(lines)}

FILES:
{str(input_files) if input_files else "none"}

QUERY:
{query}"""

    '''Chiede al modello di decomporre il task in un piano eseguibile, restituendo la risposta testuale e la latenza.'''
    def decompose_task(self, discovered_services, discovered_capabilities, discovered_endpoints,
                       discovered_schemas, discovered_request_schemas, discovered_parameters,
                       query, input_files=None):
        system_prompt = self._build_system_prompt()
        user_prompt   = self._build_user_prompt(
            discovered_services, discovered_capabilities, discovered_endpoints,
            discovered_schemas, discovered_request_schemas, discovered_parameters,
            query, input_files
        )
        response, latency = self.query_ollama(system_prompt, user_prompt)
        print(f"[LLM RESPONSE] {response}")
        print("=" * 100)
        return response, latency

    # ------------------------------------------------------------------
    # PARSING JSON
    # ------------------------------------------------------------------

    def extract_agents(self, agents_json):
        def try_parse(text):
            s, e = text.find('{'), text.rfind('}') + 1
            if s != -1 and e > s:
                try:
                    return json.loads(text[s:e])
                except json.JSONDecodeError:
                    pass
            return None

        # Livello 1: parse diretto
        try:
            result = json.loads(agents_json)
            if isinstance(result, dict):
                print("[PARSE] Diretto.")
                return result
        except json.JSONDecodeError:
            pass

        # Livello 2: dopo </think> (modelli reasoning)
        m = re.search(r'</think>', agents_json, flags=re.IGNORECASE)
        if m:
            result = try_parse(agents_json[m.end():].strip())
            if result is not None:
                print("[PARSE] Dopo </think>.")
                return result

        # Livello 3: ricerca grezza
        result = try_parse(agents_json)
        if result is not None:
            print("[PARSE] Grezzo.")
            return result

        print(f"[FORMAT ERROR] Nessun JSON valido. Anteprima: {agents_json[:200]}")
        return {}

    # ------------------------------------------------------------------
    # RISOLUZIONE PLACEHOLDER
    # ------------------------------------------------------------------

    def _traverse(self, val, parts):
        """Naviga val seguendo la lista di parti (field, FIND(...), indice numerico)."""
        for part in parts:
            if part.startswith("FIND(") and part.endswith(")"):
                condition = part[5:-1] # estrae il contenuto di FIND(...)
                if "=" not in condition or not isinstance(val, list):
                    return ""
                key, expected = condition.split("=", 1)
                key      = key.strip()
                expected = expected.strip().lower().replace('+', ' ').replace('%20', ' ') 
                found    = next(
                    (item for item in val
                     if isinstance(item, dict) and expected in str(item.get(key, "")).lower()), 
                    None
                )
                if not found:
                    # fallback: ricerca globale su tutti i valori dell'oggetto
                    for item in val:
                        if isinstance(item, dict) and any(
                            expected in str(v).lower() for v in item.values() if v is not None
                        ):
                            found = item
                            print(f"[AUTO-CORREZIONE] Match globale: {found}")
                            break
                val = found if found else ""

            # Accesso diretto a indice numerico (es. .0, .1)
            elif isinstance(val, list) and part.isdigit():
                idx = int(part)
                val = val[idx] if 0 <= idx < len(val) else ""

            # Accesso a indice numerico con sintassi N(0), N(1), ...
            elif isinstance(val, list) and re.match(r'^N\((\d+)\)$', part): 
                idx = int(re.match(r'^N\((\d+)\)$', part).group(1))
                val = val[idx] if 0 <= idx < len(val) else ""

            # Accesso a campo di dizionario
            elif isinstance(val, dict):
                val = val.get(part, "") # se la chiave non esiste, restituisce stringa vuota per evitare errori di tipo in catene successive

            else:
                return ""

        return val

    def _resolve_expression(self, expr, context):
        """Risolve una singola espressione task.field[.field...] nel contesto."""
        expr  = re.sub(r'\.(output|response|data)\b', '', expr) # rimuove suffissi comuni
        expr  = re.sub(r'\[(\d+)\]', r'.\1', expr) # converte [0], [1] in .0, .1 per uniformità
        parts = expr.split('.')
        val   = context.get(parts[0], {}) # parte iniziale: nome del task
        return self._traverse(val, parts[1:]) # naviga nelle parti successive

    def resolve_placeholders(self, data, context):
        if isinstance(data, str): #data è una stringa, cerca placeholder {{...}}
            matches = re.findall(r'\{\{(.*?)\}\}', data) #estrae tutte le espressioni {{...}} presenti nella stringa
            if not matches:
                return data

            # Placeholder singolo che occupa tutta la stringa → preserva il tipo
            if len(matches) == 1 and data.strip() == f"{{{{{matches[0]}}}}}":
                return self._resolve_expression(matches[0], context)

            # Placeholder inline in una stringa (es. URL)
            for match in matches:
                val = self._resolve_expression(match, context)
                if isinstance(val, (dict, list)):
                    val = json.dumps(val)
                data = data.replace(f"{{{{{match}}}}}", str(val) if val != "" else "")
            return data

        if isinstance(data, dict): #data è un dizionario, risolve ricorsivamente i valori
            return {k: self.resolve_placeholders(v, context) for k, v in data.items()}
        if isinstance(data, list): #data è una lista, risolve ricorsivamente gli elementi
            return [self.resolve_placeholders(item, context) for item in data]
        return data

    # ------------------------------------------------------------------
    # ESECUZIONE TASK
    # ------------------------------------------------------------------

    async def call_agent(self, session, task, discovered_services):
        task_name  = task.get("task_name") or task.get("name") or task.get("taskName") or "unnamed_task"
        endpoint   = task.get("url") or task.get("endpoint") or ""
        input_data = task.get("input", "")
        operation  = str(task.get("operation") or task.get("method") or "GET").upper()

        # Normalizzazione URL
        if " " in endpoint:
            endpoint = endpoint.split(" ")[-1]
        if endpoint and not endpoint.startswith("http"): #http mancante → assume percorso relativo al mock server
            mock_url = os.environ.get("MOCK_SERVER_URL", "http://mock-server:8080") 
            endpoint = f"{mock_url}{endpoint}" if endpoint.startswith("/") else f"{mock_url}/{endpoint}" #aggiunge mock_url come prefisso
        if endpoint and "mock-server/" in endpoint: #
            endpoint = endpoint.replace("mock-server/", "mock-server:8080/")

        if not endpoint or not endpoint.strip(): #url vuota → task fantasma, ignora ma restituisci successo per non interrompere la catena
            print(f"[WARN] Task fantasma '{task_name}' ignorato.")
            return {"task_name": task_name, "operation": operation,
                    "status": "SUCCESS", "status_code": 200,
                    "result": {"skipped": True, "message": "Dummy task ignored"}}

        response_result = {"task_name": task_name, "operation": operation}

        # Gestione tag multimodali [FILE] / [TEXT]
        payload, is_file, file_path, filename = input_data, False, None, None
        if isinstance(input_data, str):
            m = re.search(r"\[(\w+)\](.*?)\[/\1\]", input_data, re.DOTALL)
            if m:
                tag, content = m.group(1), m.group(2)
                if tag == "TEXT":
                    try:
                        payload = json.loads(content)
                    except Exception:
                        payload = content
                elif tag == "FILE":
                    filename  = content
                    file_path = os.path.join("Files", filename)
                    if not os.path.exists(file_path):
                        response_result.update({"status": "ERROR", "status_code": 404,
                                                "result": f"File '{filename}' non trovato"})
                        return response_result
                    is_file = True

        try:
            kwargs = {"timeout": 5}
            if is_file:
                form = aiohttp.FormData()
                form.add_field("file", open(file_path, "rb"),
                               filename=filename, content_type="application/octet-stream")
                kwargs["data"] = form
            elif payload is not None and payload != "":
                kwargs["json"] = payload

            ops = {"POST": session.post, "PUT": session.put,
                   "GET":  session.get,  "DELETE": session.delete}
            if operation not in ops:
                raise ValueError(f"Operazione non supportata: {operation}")

            async with ops[operation](endpoint, **kwargs) as resp:
                status       = resp.status
                content_type = resp.headers.get("Content-Type", "")

                if 200 <= status < 300:
                    if content_type.startswith(("application/pdf", "image/")):
                        return {"status": "FILE", "status_code": status,
                                "headers": dict(resp.headers), "body": await resp.read()}

                    raw = await resp.text()
                    if not raw.strip():
                        result = None
                    elif "application/json" in content_type:
                        try:
                            result = json.loads(raw)
                        except json.JSONDecodeError:
                            result = raw
                    else:
                        result = raw

                    print(f"[SUCCESS] '{task_name}' completato.")
                    response_result.update({"status": "SUCCESS", "status_code": status, "result": result})
                else:
                    error = await resp.text()
                    print(f"[ERROR] '{task_name}' fallito con status {status}: {error}")
                    response_result.update({"status": "ERROR", "status_code": status, "result": error})

        except Exception as e:
            print(f"[EXCEPTION] '{task_name}': {e}")
            response_result.update({"status": "EXCEPTION", "status_code": 500, "result": str(e)})

        return response_result

    # ------------------------------------------------------------------
    # ORCHESTRAZIONE
    # ------------------------------------------------------------------

    async def trigger_agents_async(self, agents: dict, discovered_services):
        results   = []
        context   = {}

        async with aiohttp.ClientSession() as session: 
            for task in agents.get("tasks", []):
                task["url"] = self.resolve_placeholders(task.get("url") or "", context)
                if task.get("input"):
                    task["input"] = self.resolve_placeholders(task["input"], context) #risolve placeholder anche nell'input, utile per POST/PUT con chaining nei body

                result = await self.call_agent(session, task, discovered_services) #chiama l'agente con i dati risolti

                if result.get("status") == "FILE":
                    return result

                results.append(result)
                name = task.get("task_name") or "unnamed_task"

                if result.get("status") == "SUCCESS":
                    context[name] = result.get("result", {})
                else:
                    print(f"[CHAIN BROKEN] '{name}' fallito. Interruzione pipeline.")
                    break

        return results

    # Serve per chiamare la versione asincrona da contesti sincroni (es. Flask)
    def trigger_agents(self, agents: dict, discovered_services):
        return asyncio.run(self.trigger_agents_async(agents, discovered_services))

    # ------------------------------------------------------------------
    # UTILITÀ
    # ------------------------------------------------------------------

    def replace_endpoints(self, endpoints_list, mock_server_address):
        return [ #sostituisce ogni occorrenza di "http://localhost:8585" con l'indirizzo del mock server, se endpoint è una stringa
            {k: re.sub(r"http://localhost:8585", mock_server_address, v) 
             if isinstance(v, str) else v
             for k, v in ep.items()}
            for ep in endpoints_list
        ]

    def _attempt_auto_fix(self, plan: dict) -> dict:
        #cerca di correggere automaticamente errori comuni come URL mancanti o operazioni in minuscolo, ma solo se è possibile rendere il piano valido senza inventare dati o strutture non presenti nell'output originale
        mock_url    = os.environ.get("MOCK_SERVER_URL", "http://mock-server:8080")
        fixed_tasks = []

        for task in plan.get("tasks", []):
            if not isinstance(task, dict): #task non è un dizionario, scartalo perché non possiamo correggerlo
                continue
            url = str(task.get("url", "")) #se l'URL è presente ma non inizia con http, prova ad aggiungere il prefisso del mock server (assumendo che sia un percorso relativo)
            if url and not url.startswith("http") and not url.startswith("{{"):
                task["url"] = (mock_url if url.startswith("/") else mock_url + "/") + url.lstrip("/") #aggiunge mock_url come prefisso
                print(f"[AUTO-FIX] URL: {task['url']}")
            if isinstance(task.get("operation"), str): 
                task["operation"] = task["operation"].upper()
            if all(f in task for f in ("task_name", "service_id", "url", "operation")): 
                #se dopo le correzioni il task ha tutti i campi richiesti, aggiungilo alla lista dei task corretti; altrimenti scartalo perché non è recuperabile
                fixed_tasks.append(task)
            else:
                print(f"[AUTO-FIX] Task scartato: {task.get('task_name', 'unnamed')}")

        plan["tasks"] = fixed_tasks #sostituisce la lista originale dei task con quella filtrata e corretta
        return plan

    # ------------------------------------------------------------------
    # ENTRY POINT
    # ------------------------------------------------------------------

    def control(self, query, files=None):
        analyzed_files = self.analyze_files(files or [])

        catalog_url  = os.environ.get("CATALOG_URL",    "http://catalog-gateway:5000")
        registry_url = os.environ.get("REGISTRY_URL",   "http://registry:8500")
        mock_url     = os.environ.get("MOCK_SERVER_URL", "http://mock-server:8080")

        registry     = Discovery(registry_url)
        services     = registry.services()
        service_data = requests.post(f"{catalog_url}/index/search", json={"query": query}).json()
        service_list = service_data.get("results", [])

        if not service_list:
            return {"execution_plan": {}, "execution_results": [], "error": "No services matched the query"}

        registry_ids          = {s["id"] for s in services}
        filtered_service_list = [s for s in service_list if s["_id"] in registry_ids]
        orphaned              = [s for s in service_list if s["_id"] not in registry_ids]

        if orphaned:
            print("[WARNING] Servizi non più nel registry:", [s.get("_id") for s in orphaned])

        if not filtered_service_list:
            return {"execution_plan": {}, "execution_results": [],
                    "error": "None of the discovered services are currently available"}

        # Log retrieval
        print("\n" + "=" * 60)
        print(f"[RETRIEVAL] Query: {query}")
        for s in filtered_service_list:
            caps = s.get("capabilities", {})
            print(f"  - {s.get('_id')} | {s.get('name')} | {len(caps)} endpoints")
            for key in caps:
                if key != "POST /register":
                    print(f"      {key}")
        print("=" * 60 + "\n")

        # Costruzione liste per il prompt
        disc_services, disc_caps, disc_eps = [], [], []
        disc_schemas, disc_req_schemas, disc_params = [], [], []

        for s in filtered_service_list:
            disc_services.append({"_id": s.get("_id"), "name": s.get("name"), "description": s.get("description")})
            disc_caps.append(s.get("capabilities", {}))
            disc_eps.append(s.get("endpoints", {}))
            disc_schemas.append(s.get("response_schemas", {}))
            disc_req_schemas.append(s.get("request_schemas", {}))
            disc_params.append(s.get("parameters", {}))

        disc_eps = self.replace_endpoints(disc_eps, mock_url) #sostituisce eventuali endpoint con URL del mock server, se presenti

        # Chiede al modello di decomporre il task in un piano eseguibile, restituendo la risposta testuale e la latenza.
        plan_json, latency = self.decompose_task(
            disc_services, disc_caps, disc_eps,
            disc_schemas, disc_req_schemas, disc_params,
            query, analyzed_files
        )

        # Estrae il piano JSON dalla risposta del modello, gestendo eventuali errori di formato o testo extra.
        plan = self.extract_agents(plan_json) 

        available_ids          = [s["_id"] for s in disc_services]
        is_valid, val_errors   = PlanValidator.validate(plan, available_ids) #valida il piano generato rispetto alle regole definite, restituendo un booleano di validità e una lista di errori se non valido

        if not is_valid:
            print(f"[VALIDATION] {len(val_errors)} errore/i:")
            for e in val_errors:
                print(f"  - {e}")
            plan     = self._attempt_auto_fix(plan) #prova a correggere automaticamente errori comuni, ma solo se è possibile rendere il piano valido senza inventare dati o strutture non presenti nell'output originale
            is_valid, val_errors = PlanValidator.validate(plan, available_ids)
            if not is_valid:
                print("[VALIDATION] Piano non recuperabile.")
                return {"execution_plan": plan, "execution_results": [],
                        "validation_errors": val_errors, "plan_generation_latency": latency}
            print("[VALIDATION] Recuperato con auto-fix.")
        else:
            print(f"[VALIDATION] Piano valido ({len(plan.get('tasks', []))} task/s).")

        # Esegue il piano chiamando gli agenti in sequenza, risolvendo i placeholder e gestendo i risultati intermedi, restituendo la lista dei risultati finali o un file se uno dei task restituisce un file.
        results = self.trigger_agents(plan, disc_services) 

        if isinstance(results, dict) and results.get("status") == "FILE":
            return Response(results["body"], status=results["status_code"], headers=results["headers"])

        # Pulizia Files
        if os.path.exists("Files"):
            for fname in os.listdir("Files"):
                fpath = os.path.join("Files", fname)
                try:
                    os.unlink(fpath) if os.path.isfile(fpath) else shutil.rmtree(fpath)
                except Exception as e:
                    print(f"[CLEANUP] {fpath}: {e}")

        return {"execution_plan": plan, "execution_results": results, "plan_generation_latency": latency}