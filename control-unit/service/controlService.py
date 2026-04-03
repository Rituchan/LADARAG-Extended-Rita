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
import jmespath                        # pip install jmespath
from jmespath import exceptions as jmespath_exc


# ─────────────────────────────────────────────────────────────────────────────
# PLAN VALIDATOR
# ─────────────────────────────────────────────────────────────────────────────

class PlanValidator:

    VALID_OPERATIONS = {"GET", "POST", "PUT", "DELETE"}

    @staticmethod
    def validate(plan: dict, available_service_ids: list) -> tuple[bool, list[str]]:
        errors = []
        if not isinstance(plan, dict) or "tasks" not in plan:
            return False, ["Missing or invalid 'tasks' array"]
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

            # Controlla placeholder singoli non risolti {id} — esclude i {{...}} validi
            clean_url = re.sub(r'\{\{.*?\}\}', '', url)
            if re.search(r'(?<!\{)\{(?!\{)[^{]*\}(?!\})', clean_url):
                errors.append(f"{prefix}: unresolved path parameter in url '{url[:60]}'")

            op = task.get("operation", "")
            if op and op not in PlanValidator.VALID_OPERATIONS:
                errors.append(f"{prefix}: invalid operation '{op}'")

            # Chaining: i placeholder devono referenziare task già definiti
            task_str = json.dumps(task)
            for ref in re.findall(r'\{\{\s*([a-zA-Z0-9_]+)', task_str):
                if ref not in defined_task_names:
                    errors.append(f"{prefix}: references undefined task '{ref}'")

            defined_task_names.add(task.get("task_name", ""))

        return len(errors) == 0, errors


# ─────────────────────────────────────────────────────────────────────────────
# CONTROLLER
# ─────────────────────────────────────────────────────────────────────────────

class Controller:

    def __init__(self):
        self.model_name = os.environ.get("LLM_MODEL", "qwen3.5:27b")
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

    def query_ollama(self, system_prompt: str, user_prompt: str) -> tuple[str, float]:
        url = os.environ.get("OLLAMA_API_URL", "http://localhost:11434")
        try:
            t0 = time.perf_counter()
            response = requests.post(
                f"{url}/api/chat",
                json={
                    "model": self.model_name,
                    # Schema strutturato: "reasoning" alla radice + array di task
                    "format": {
                        "type": "object",
                        "properties": {
                            "reasoning": {
                                "type": "string"
                            },
                            "tasks": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "task_name":  {"type": "string"},
                                        "service_id": {"type": "string"},
                                        "url":        {"type": "string"},
                                        "operation":  {
                                            "type": "string",
                                            "enum": ["GET", "POST", "PUT", "DELETE"]
                                        },
                                        "input": {"type": ["string", "object", "null"]},
                                    },
                                    "required": ["task_name", "service_id", "url", "operation", "input"],
                                    "additionalProperties": False,
                                }
                            }
                        },
                        "required": ["tasks"],
                        "additionalProperties": False,
                    },
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user",   "content": user_prompt},
                    ],
                    "think": False,          # disabilita thinking mode nativo Qwen3/Ollama
                    "options": {
                        "temperature": 0.0,
                        "num_ctx":     16384,
                    },
                    "stream": False,
                },
                timeout=120,    # ← fail fast dopo 120s invece di aspettare all'infinito

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

        ex_get = {
            "reasoning": "The user wants to see all currently open tourist attractions. I will query the smart-tourist-attractions service with a filter on status=open.",
            "tasks": [{
                "task_name":  "get_open_attractions",
                "service_id": "smart-tourist-attractions-mock",
                "url":        "http://mock-server:8080/rest/Smart+Tourist+Attractions+API/1.0/attraction?status=open",
                "operation":  "GET",
                "input":      ""
            }]
        }

        ex_post = {
            "reasoning": "The user wants to create a new citizen report. I will call the smart-citizens-reports service with the provided details.",
            "tasks": [{
                "task_name":  "create_report",
                "service_id": "smart-citizens-reports-mock",
                "url":        "http://mock-server:8080/rest/Smart+Citizens+Reports+and+Ticketing+API/1.0/citizen-report",
                "operation":  "POST",
                "input": {
                    "zoneId":       "Z-CENTRO",
                    "reporterName": "Mario Rossi",
                    "location":     "Piazza Castello",
                    "category":     "sanitation",
                    "description":  "Cestino pieno",
                    "status":       "open",
                    "submittedAt":  "2025-09-25T10:00:00Z"
                }
            }]
        }

        ex_chain_single = {
            "reasoning": "The user wants to turn off a specific light. I will GET all lights, filter by location with JMESPath, and inject the id directly into the PUT url.",
            "tasks": [
                {
                    "task_name":  "get_all_lights",
                    "service_id": "smart-lighting-mock",
                    "url":        "http://mock-server:8080/rest/Smart+Lighting+and+Energy+API/1.0/light",
                    "operation":  "GET",
                    "input":      ""
                },
                {
                    "task_name":  "turn_off_light",
                    "service_id": "smart-lighting-mock",
                    "url":        "http://mock-server:8080/rest/Smart+Lighting+and+Energy+API/1.0/light/{{get_all_lights[?location=='Corso Garibaldi - Palo 12, Benevento'] | [0].id}}",
                    "operation":  "PUT",
                    "input": {
                        "zoneId":           "Z-CENTRO",
                        "location":         "Corso Garibaldi - Palo 12, Benevento",
                        "status":           "off",
                        "brightness":       0,
                        "energyConsumption": 0.0,
                        "smartEnabled":     True
                    }
                }
            ]
        }

        ex_chain_multivalore = {
            "reasoning": "The user wants parking near tourist attractions. I get all open attractions, collect ALL their zoneIds with JMESPath join, and pass them as a comma-separated zoneIds param to the parking service.",
            "tasks": [
                {
                    "task_name":  "get_open_attractions",
                    "service_id": "smart-tourist-attractions-mock",
                    "url":        "http://mock-server:8080/rest/Smart+Tourist+Attractions+API/1.0/attraction?status=open",
                    "operation":  "GET",
                    "input":      ""
                },
                {
                    "task_name":  "get_parking_near_attractions",
                    "service_id": "smart-parking-mock",
                    "url":        "http://mock-server:8080/rest/Smart+Parking+and+Mobility+API/1.0/parking-spot?zoneIds={{get_open_attractions[*].zoneId | join(',', @)}}&available=true",
                    "operation":  "GET",
                    "input":      ""
                }
            ]
        }

        ex_chain_alert = {
            "reasoning": "The user wants to find attractions in zones with active environmental alerts. I get all sensors with alertActive=true, collect their zoneIds, and query attractions in those zones.",
            "tasks": [
                {
                    "task_name":  "get_alert_sensors",
                    "service_id": "smart-environment-sensors-mock",
                    "url":        "http://mock-server:8080/rest/Smart+Environment+and+Temperature+Sensors+API/1.0/sensor?alertActive=true",
                    "operation":  "GET",
                    "input":      ""
                },
                {
                    "task_name":  "get_attractions_in_alert_zones",
                    "service_id": "smart-tourist-attractions-mock",
                    "url":        "http://mock-server:8080/rest/Smart+Tourist+Attractions+API/1.0/attraction?zoneIds={{get_alert_sensors[*].zoneId | join(',', @)}}",
                    "operation":  "GET",
                    "input":      ""
                }
            ]
        }

        examples_str = (
            f"EXAMPLE 1 — Simple GET with filter:\n{json.dumps(ex_get, indent=2)}\n\n"
            f"EXAMPLE 2 — POST with full body:\n{json.dumps(ex_post, indent=2)}\n\n"
            f"EXAMPLE 3 — JMESPath filter + path param injection:\n{json.dumps(ex_chain_single, indent=2)}\n\n"
            f"EXAMPLE 4 — Multi-zone JOIN (collect ALL zones, pass as comma-separated):\n{json.dumps(ex_chain_multivalore, indent=2)}\n\n"
            f"EXAMPLE 5 — Boolean filter + JOIN:\n{json.dumps(ex_chain_alert, indent=2)}"
        )

        return f"""You are an API orchestrator for a distributed Smart City system.
Given a user query and a catalog of available services, produce ONLY a valid JSON execution plan.

─────────────────────────────────────────────────────────────────────────────
CHAINING — JMESPath syntax inside {{{{...}}}}
─────────────────────────────────────────────────────────────────────────────
Format: {{{{task_name<jmespath_expression>}}}}
The JMESPath expression runs on the JSON result of the named previous task.

CHEAT SHEET:
  Simple field:            {{{{task.field}}}}
  Nested field:            {{{{task.nested.field}}}}
  Array index:             {{{{task[0].field}}}}
  Filter + first element:  {{{{task[?key=='value'] | [0].field}}}}
  All values of a field:   {{{{task[*].field}}}}
  Multi-zone join:         {{{{task[*].zoneId | join(',', @)}}}}
  Filter + join:           {{{{task[?status=='open'][*].zoneId | join(',', @)}}}}
  Boolean filter + join:   {{{{task[?available==`true`][*].id | join(',', @)}}}}
  Number filter:           {{{{task[?pricePerHour==`0.0`] | [0].id}}}}
  Sort ascending (min):    {{{{task | sort_by(@, &waitingTimeMinutes)[0].id}}}}
  Sort descending (max):   {{{{task | sort_by(@, &fillLevel)[-1].id}}}}
  Count matches:           {{{{task[?alertActive==`true`] | length(@)}}}}
  OR filter + join:        {{{{task[?field=='a' || field=='b'][*].zoneId | join(',', @)}}}}

SYNTAX RULES:
  - String values  → single quotes:  [?status=='open']
  - Booleans       → backticks:      [?available==`true`]
  - Numbers        → backticks:      [?pricePerHour==`1.5`]
  - Null           → backticks:      [?incidentId==`null`]

MULTI-ZONE QUERIES — use zoneIds (comma-separated) when querying multiple zones:
  url=".../parking-spot?zoneIds={{{{get_attractions[*].zoneId | join(',', @)}}}}&available=true"
  Services supporting ?zoneIds=: parking-spot, attraction, sensor, traffic-sensor,
  light, bin, charging-station, building, emergency-unit, vehicle, citizen-report.

CRITICAL FOR PATH PARAMETERS — inject chaining directly in the URL string:
  RIGHT: url=".../light/{{{{get_lights[?location=='Corso Garibaldi'] | [0].id}}}}"
  WRONG: url=".../light/{{id}}", input={{"id": "{{{{get_lights[0].id}}}}"}}

─────────────────────────────────────────────────────────────────────────────
EXAMPLES — study these patterns carefully:
─────────────────────────────────────────────────────────────────────────────
{examples_str}

─────────────────────────────────────────────────────────────────────────────
RULES (in order of priority):
─────────────────────────────────────────────────────────────────────────────
1.  Use "reasoning" to think step-by-step: identify services, dependencies, and JMESPath expressions before writing tasks.
2.  Use ONLY the provided services, endpoints, parameters, and schemas. Never invent.
3.  Tasks execute sequentially. Use {{{{task_name<jmespath>}}}} for chaining.
4.  For path params: inject directly into the url string (see EXAMPLE 3).
5.  For query params: append to url as ?key=value (never in input field).
6.  For multi-zone queries: use join(',', @) and the zoneIds param (see EXAMPLE 4).
7.  Use RESPONSE SCHEMAS for exact field names when chaining. Never guess.
8.  Use REQUEST SCHEMAS for POST/PUT bodies. Fields marked * are required.
9.  operation must be exactly one of: GET, POST, PUT, DELETE.
10. Every task MUST have a non-empty url starting with http://.
11. Required keys per task: task_name, service_id, url, operation, input.
    service_id MUST be exactly the SERVICE_ID value shown in the catalog (e.g. 'smart-traffic-monitoring-mock').
    NEVER use the NAME field as service_id.
12. NEVER create an intermediate GET-by-ID if the ID is already available.
13. PARAMETERS marked * are REQUIRED and must appear in the url.
14. NEVER return a url with unresolved placeholders like {{id}} or {{zoneId}}.
15. NEVER concatenate two {{{{}}}} placeholders in the same URL param value:
    WRONG: ?zoneIds={{{{task1[*].zoneId | join(',', @)}}}},{{{{task2[*].zoneId | join(',', @)}}}}
    RIGHT: query all data in ONE prior task, then filter with JMESPath OR:
           ?zoneIds={{{{get_all[?level=='high' || level=='critical'][*].zoneId | join(',', @)}}}}"""

    # ------------------------------------------------------------------
    # USER PROMPT
    # ------------------------------------------------------------------

    def _build_user_prompt(self, discovered_services, discovered_capabilities,
                           discovered_endpoints, discovered_schemas,
                           discovered_request_schemas, discovered_parameters,
                           query, input_files=None) -> str:
        lines = ["/no_think", "SERVICES AND ENDPOINTS:"]   # ← aggiunta /no_think
        for i, service in enumerate(discovered_services):
            lines.append(f"\nSERVICE_ID: {service.get('_id')}")
            lines.append(f"NAME: {service.get('name')}")
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

    # ------------------------------------------------------------------
    # DECOMPOSE TASK
    # ------------------------------------------------------------------

    def decompose_task(self, discovered_services, discovered_capabilities,
                       discovered_endpoints, discovered_schemas,
                       discovered_request_schemas, discovered_parameters,
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

    def extract_agents(self, agents_json: str) -> dict:
        def try_parse(text):
            s, e = text.find('{'), text.rfind('}') + 1
            if s != -1 and e > s:
                try:
                    return json.loads(text[s:e])
                except json.JSONDecodeError:
                    pass
            return None

        # Livello 1: parse diretto (sempre il caso con structured output)
        try:
            result = json.loads(agents_json)
            if isinstance(result, dict):
                print("[PARSE] Diretto.")
                return result
        except json.JSONDecodeError:
            pass

        # Livello 2: dopo </think> (fallback per modelli reasoning)
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
    # RISOLUZIONE PLACEHOLDER — JMESPath
    # ------------------------------------------------------------------

    def _resolve_expression(self, expr: str, context: dict):
        """
        Risolve un placeholder JMESPath sul contesto dei task precedenti.

        Formato:  task_name<jmespath_expression>
        Esempi:
          get_bins.location
          get_bins[0].id
          get_attractions[?status=='open'] | [0].zoneId
          get_attractions[*].zoneId | join(',', @)
          get_sensors[?alertActive==`true`][*].zoneId | join(',', @)
          get_lights | sort_by(@, &brightness)[0].id
        """
        # Rimuove suffissi legacy
        expr = re.sub(r'\.(output|response|data)\b', '', expr)
        # Normalizza [N] → .[N]
        expr = re.sub(r'(?<![\[\]])(\[)(\d+\])', r'.\1\2', expr)
        m = re.match(r'^(\w+)(.*)', expr, re.DOTALL)
        if not m:
            print(f"[JMESPATH] Impossibile estrarre task_name da '{expr}'")
            return ""

        task_name = m.group(1)
        remainder = m.group(2).strip()

        val = context.get(task_name)
        if val is None:
            print(f"[JMESPATH] Task '{task_name}' non trovato nel contesto")
            return ""

        if not remainder:
            return val

        # Rimuove il punto iniziale se presente (JMESPath non lo accetta)
        jmespath_expr = remainder[1:] if remainder.startswith('.') else remainder

        if not jmespath_expr:
            return val

        try:
            result = jmespath.search(jmespath_expr, val)
            if result is None:
                print(f"[JMESPATH] Nessun match per '{jmespath_expr}' su '{task_name}'")
                return ""
            # Deduplicazione: liste di stringhe (es. zoneIds per join)
            if isinstance(result, list) and all(isinstance(x, str) for x in result):
                seen = set()
                result = [x for x in result if not (x in seen or seen.add(x))]
            return result
        except jmespath_exc.JMESPathError as e:
            print(f"[JMESPATH ERROR] expr='{expr}' jmespath='{jmespath_expr}': {e}")
            if isinstance(val, dict):
                return val.get(jmespath_expr.split('.')[0], "")
            return "" 

    def resolve_placeholders(self, data, context: dict):
        """
        Risolve tutti i placeholder {{...}} in una struttura dati.
        Supporta stringhe, dizionari e liste ricorsivamente.
        """
        if isinstance(data, str):
            # ── Normalizza concatenazione malformata generata dall'LLM ──────────
            # Pattern errato: {{A},{{B}}  (manca un } prima della virgola)
            # Pattern atteso: {{A}},{{B}} (due placeholder distinti)
            # Causa: il modello chiude il primo con } invece di }} per concatenare
            if '},{{'  in data and '}},{{'  not in data:
                print(f"[PLACEHOLDER FIX] Malformed concatenation normalized in: {data[:80]}")
                data = data.replace('},{{'  , '}},{{'  )

            matches = re.findall(r'\{\{(.*?)\}\}', data)
            if not matches:
                return data

            # Caso 1: l'intera stringa è esattamente un placeholder → preserva il tipo
            if len(matches) == 1 and data.strip() == f"{{{{{matches[0]}}}}}":
                return self._resolve_expression(matches[0].strip(), context)

            # Caso 2: placeholder inline in URL o stringa → converte tutto a stringa
            for match in matches:
                val = self._resolve_expression(match.strip(), context)
                if isinstance(val, (dict, list)):
                    val = json.dumps(val)
                elif val is None:
                    val = ""
                else:
                    val = str(val)
                data = data.replace(f"{{{{{match}}}}}", val)
            return data

        if isinstance(data, dict):
            return {k: self.resolve_placeholders(v, context) for k, v in data.items()}
        if isinstance(data, list):
            return [self.resolve_placeholders(item, context) for item in data]
        return data

    # ------------------------------------------------------------------
    # ESECUZIONE TASK
    # ------------------------------------------------------------------

    async def call_agent(self, session, task, discovered_services):
        task_name  = task.get("task_name") or "unnamed_task"
        endpoint   = task.get("url") or task.get("endpoint") or ""
        input_data = task.get("input", "")
        operation  = str(task.get("operation") or "GET").upper()

        if " " in endpoint:
            endpoint = endpoint.split(" ")[-1]
        if endpoint and not endpoint.startswith("http"):
            mock_url = os.environ.get("MOCK_SERVER_URL", "http://mock-server:8080")
            endpoint = f"{mock_url}{endpoint}" if endpoint.startswith("/") else f"{mock_url}/{endpoint}"

        response_result = {"task_name": task_name, "operation": operation}

        if not endpoint or not endpoint.strip():
            print(f"[WARN] Task fantasma '{task_name}' ignorato.")
            response_result.update({"status": "SUCCESS", "status_code": 200, "result": {}})
            return response_result

        try:
            tag_pattern = r"\[(\w+)\](.*?)\[/\1\]"
            tag_matches = re.findall(tag_pattern, str(input_data), re.DOTALL) \
                          if isinstance(input_data, str) else []

            if operation == "GET":
                async with session.get(endpoint) as resp:
                    status = resp.status
                    result = await resp.json()
                    response_result.update({
                        "status":      "SUCCESS" if status in (200, 201, 204) else "ERROR",
                        "status_code": status,
                        "result":      result,
                    })

            elif operation in ("POST", "PUT", "PATCH"):
                if tag_matches:
                    form_data = aiohttp.FormData()
                    for tag_type, tag_content in tag_matches:
                        tag_content = tag_content.strip()
                        if tag_type == "FILE":
                            file_path = os.path.join("Files", tag_content)
                            if os.path.exists(file_path):
                                form_data.add_field(
                                    "file", open(file_path, "rb"),
                                    filename=tag_content,
                                    content_type=mimetypes.guess_type(file_path)[0]
                                                 or "application/octet-stream"
                                )
                        elif tag_type == "TEXT":
                            form_data.add_field("data", tag_content, content_type="application/json")
                    async with session.request(operation, endpoint, data=form_data) as resp:
                        status = resp.status
                        result = await resp.json()
                        response_result.update({
                            "status": "SUCCESS" if status in (200, 201, 204) else "ERROR",
                            "status_code": status, "result": result,
                        })
                else:
                    payload = input_data if isinstance(input_data, dict) else {}
                    async with session.request(operation, endpoint, json=payload) as resp:
                        status = resp.status
                        result = await resp.json()
                        response_result.update({
                            "status": "SUCCESS" if status in (200, 201, 204) else "ERROR",
                            "status_code": status, "result": result,
                        })

            elif operation == "DELETE":
                async with session.delete(endpoint) as resp:
                    status = resp.status
                    try:
                        result = await resp.json()
                    except Exception:
                        result = await resp.text()
                    response_result.update({
                        "status": "SUCCESS" if status in (200, 201, 204) else "ERROR",
                        "status_code": status, "result": result,
                    })

        except Exception as e:
            print(f"[EXCEPTION] '{task_name}': {e}")
            response_result.update({"status": "EXCEPTION", "status_code": 500, "result": str(e)})

        return response_result

    # ------------------------------------------------------------------
    # ORCHESTRAZIONE
    # ------------------------------------------------------------------

    async def trigger_agents_async(self, agents: dict, discovered_services):
        results = []
        context = {}
        async with aiohttp.ClientSession() as session:
            for task in agents.get("tasks", []):
                task["url"] = self.resolve_placeholders(task.get("url") or "", context)
                if task.get("input"):
                    task["input"] = self.resolve_placeholders(task["input"], context)
                result = await self.call_agent(session, task, discovered_services)
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

    def trigger_agents(self, agents: dict, discovered_services):
        return asyncio.run(self.trigger_agents_async(agents, discovered_services))

    # ------------------------------------------------------------------
    # UTILITÀ
    # ------------------------------------------------------------------

    def replace_endpoints(self, endpoints_list, mock_server_address):
        return [
            {k: re.sub(r"http://localhost:8585", mock_server_address, v)
             if isinstance(v, str) else v
             for k, v in ep.items()}
            for ep in endpoints_list
        ]

    def _attempt_auto_fix(self, plan: dict, available_ids: list = None, name_to_id: dict = None) -> dict:
        mock_url = os.environ.get("MOCK_SERVER_URL", "http://mock-server:8080")
        id_set   = set(available_ids or [])
        fixed_tasks = []
        for task in plan.get("tasks", []):
            if not isinstance(task, dict):
                continue

            # ── Corregge service_id: modello ha usato name invece di _id ──
            sid = task.get("service_id", "")
            if sid and sid not in id_set and name_to_id and sid in name_to_id:
                corrected = name_to_id[sid]
                print(f"[AUTO-FIX] service_id '{sid}' → '{corrected}'")
                task["service_id"] = corrected

            url = str(task.get("url", ""))
            if url and not url.startswith("http") and not url.startswith("{{"):
                task["url"] = (mock_url if url.startswith("/") else mock_url + "/") + url.lstrip("/")
                print(f"[AUTO-FIX] URL: {task['url']}")
            if isinstance(task.get("operation"), str):
                task["operation"] = task["operation"].upper()
            if all(f in task for f in ("task_name", "service_id", "url", "operation")):
                fixed_tasks.append(task)
            else:
                print(f"[AUTO-FIX] Task scartato: {task.get('task_name', 'unnamed')}")
        plan["tasks"] = fixed_tasks
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

        # Log retrieval — il merge avviene nel gateway, qui i servizi sono già distinti
        print("\n" + "=" * 60)
        print(f"[RETRIEVAL] Query: {query}")
        for s in filtered_service_list:
            caps = s.get("capabilities", {})
            print(f"  - {s.get('_id')} | {s.get('name')} | {len(caps)} endpoints")
        print("=" * 60 + "\n")

        disc_services, disc_caps, disc_eps = [], [], []
        disc_schemas, disc_req_schemas, disc_params = [], [], []

        for s in filtered_service_list:
            if isinstance(s.get("capabilities"), dict): s["capabilities"].pop("POST /register", None)
            if isinstance(s.get("endpoints"), dict):    s["endpoints"].pop("POST /register", None)
            disc_services.append({
                "_id":         s.get("_id"),
                "name":        s.get("name"),
                "description": s.get("description"),
            })
            disc_caps.append(s.get("capabilities", {}))
            disc_eps.append(s.get("endpoints", {}))
            disc_schemas.append(s.get("response_schemas", {}))
            disc_req_schemas.append(s.get("request_schemas", {}))
            disc_params.append(s.get("parameters", {}))

        disc_eps = self.replace_endpoints(disc_eps, mock_url)

        plan_json, latency = self.decompose_task(
            disc_services, disc_caps, disc_eps,
            disc_schemas, disc_req_schemas, disc_params,
            query, analyzed_files
        )
        print(f"[LATENCY] Piano generato in {latency:.2f}s")

        plan = self.extract_agents(plan_json)

        # Stampa il ragionamento del modello
        if isinstance(plan, dict) and "reasoning" in plan:
            print("\n" + "🧠 " * 20)
            print("[PENSIERO DI QWEN 3.5]:")
            print(plan.get("reasoning"))
            print("🧠 " * 20 + "\n")

        available_ids = [s["_id"] for s in disc_services]
        # Mappa name → _id per auto-fix quando il modello usa il name come service_id
        name_to_id    = {s.get("name"): s.get("_id") for s in disc_services}

        is_valid, val_errors = PlanValidator.validate(plan, available_ids)

        if not is_valid:
            print(f"[VALIDATION] {len(val_errors)} errore/i:")
            for e in val_errors:
                print(f"  - {e}")
            plan     = self._attempt_auto_fix(plan, available_ids, name_to_id)
            is_valid, val_errors = PlanValidator.validate(plan, available_ids)
            if not is_valid:
                print("[VALIDATION] Piano non recuperabile.")

        results = self.trigger_agents(plan, disc_services)

        if isinstance(results, dict) and results.get("status") == "FILE":
            return Response(results["body"], status=results["status_code"],
                            headers=results["headers"])

        if os.path.exists("Files"):
            for filename in os.listdir("Files"):
                file_path = os.path.join("Files", filename)
                try:
                    if os.path.isfile(file_path): os.unlink(file_path)
                    elif os.path.isdir(file_path): shutil.rmtree(file_path)
                except Exception as e:
                    print(f"[WARN] Pulizia fallita: {e}")

        return {"execution_plan": plan, "execution_results": results}