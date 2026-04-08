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
import math
import jmespath                        # pip install jmespath
import duckdb                          # pip install duckdb
import pandas as pd                    # pip install pandas
from jmespath import exceptions as jmespath_exc


# ─────────────────────────────────────────────────────────────────────────────
# PLAN VALIDATOR
# ─────────────────────────────────────────────────────────────────────────────

class PlanValidator:

    VALID_OPERATIONS = {"GET", "POST", "PUT", "DELETE", "SQL"}

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
            op  = task.get("operation", "")

            # sql-processor è un servizio interno, non nel registry — salta il check
            if sid and sid not in service_id_set and op != "SQL":
                errors.append(f"{prefix}: unknown service_id '{sid}'")

            op  = task.get("operation", "")
            url = str(task.get("url", ""))

            if op and op not in PlanValidator.VALID_OPERATIONS:
                errors.append(f"{prefix}: invalid operation '{op}'")

            # Task SQL: l'input è la query, l'url è irrilevante — salta validazioni HTTP
            if op != "SQL":
                if not url:
                    errors.append(f"{prefix}: empty url")
                elif not url.startswith("http") and "{{" not in url:
                    errors.append(f"{prefix}: url must start with http:// (got '{url[:60]}')")

                # Controlla placeholder singoli non risolti {id} — esclude i {{...}} validi
                clean_url = re.sub(r'\{\{.*?\}\}', '', url)
                if re.search(r'(?<!\{)\{(?!\{)[^{]*\}(?!\})', clean_url):
                    errors.append(f"{prefix}: unresolved path parameter in url '{url[:60]}'")

                # GET non deve avere params nel body input
                if op == "GET" and isinstance(task.get("input"), dict) and task.get("input"):
                    errors.append(
                        f"{prefix}: GET request has non-empty 'input' dict — "
                        f"query params must be in the url, not in input field"
                    )

            # Chaining: i placeholder devono referenziare task già definiti
            task_str     = json.dumps(task)
            current_name = task.get("task_name", "")
            for ref in re.findall(r'\{\{\s*([a-zA-Z0-9_]+)', task_str):
                if ref == current_name:
                    errors.append(f"{prefix}: self-reference — task cannot reference itself in chaining")
                elif ref not in defined_task_names:
                    errors.append(f"{prefix}: references undefined task '{ref}'")

            defined_task_names.add(current_name)

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
    # SCHEMA ENFORCEMENT — Input Format Builder + Plan Validator
    # ------------------------------------------------------------------

    def _build_input_format_schema(self, discovered_request_schemas: list) -> dict:
        """
        Aggrega tutti i campi validi da tutti i request_schemas scoperti nel
        registry e costruisce uno schema JSON con additionalProperties: false.

        Questo schema viene iniettato nel 'format' di Ollama (guided decoding):
        l'LLM è fisicamente impossibilitato a generare chiavi non presenti in
        nessun servizio registrato (es. "field" e "limit" avranno probabilità 0).

        Ogni campo è dichiarato come string | <tipo nativo> per permettere
        i placeholder JMESPath (es. "{{task_1[*].id}}") che sono stringhe
        a tempo di generazione ma vengono risolti a runtime da resolve_placeholders.

        Formato stringa atteso dal DB (es. smart-environment-sensors):
            "{data:arr*, by:str*, order:enum(asc,desc), top:int}"
        """
        field_pattern = re.compile(r'(\w+):([\w]+)(?:\([^)]*\))?\*?')

        # Mappa tipo compatto → schema JSON con string come alternativa per JMESPath
        type_map = {
            "arr":   {"type": ["array",   "string"]},
            "str":   {"type": "string"},
            "int":   {"type": ["integer", "string"]},
            "float": {"type": ["number",  "string"]},
            "bool":  {"type": ["boolean", "string"]},
            "obj":   {"type": ["object",  "string"]},
            "enum":  {"type": "string"},
            "any":   {},
        }

        all_properties: dict = {}

        for schemas_per_service in discovered_request_schemas:
            if not isinstance(schemas_per_service, dict):
                continue
            for endpoint, schema_str in schemas_per_service.items():
                if not schema_str:
                    continue
                for match in field_pattern.finditer(schema_str):
                    field_name = match.group(1)
                    field_type = match.group(2)
                    if field_name not in all_properties:
                        all_properties[field_name] = type_map.get(field_type, {})

        if not all_properties:
            # Nessun request_schema nel registry → fallback permissivo
            print("[INPUT SCHEMA] Nessun request_schema trovato, uso fallback permissivo.")
            return {"type": ["string", "object", "null"]}

        # Aggiunge sql_query come chiave globale per i task SQL
        # Senza questa, il guided decoding blocca la stringa SQL (tipo object ≠ string)
        all_properties["sql_query"] = {"type": "string"}

        print(f"[INPUT SCHEMA] Chiavi ammesse per 'input': {sorted(all_properties.keys())}")
        return {
            "type":                 "object",
            "properties":          all_properties,
            "additionalProperties": False,   # ← blocco matematico delle allucinazioni
        }

    def _validate_plan(self,
                       plan: dict,
                       discovered_services: list,
                       discovered_request_schemas: list) -> list[str]:
        """
        Validatore post-parse per le chiavi del campo 'input' di ogni task.

        Confronta le chiavi presenti nell'input generato dall'LLM con quelle
        dichiarate nello schema dell'endpoint specifico chiamato.
        Ritorna una lista di warning: lista vuota significa piano pulito.

        Utile per:
          - Loggare violazioni residue che il guided decoding non ha bloccato
          - Raccogliere dati quantitativi per la tesi (% violazioni pre/post patch)
          - Estendere in futuro con auto-retry selettivo sul singolo task violato
        """
        warnings: list[str] = []
        field_pattern = re.compile(r'(\w+):')

        # Costruisce mappa  path_endpoint → set_chiavi_valide
        # es. "/sort" → {"data", "by", "order", "top"}
        endpoint_valid_keys: dict[str, set] = {}
        for i, _ in enumerate(discovered_services):
            schemas = discovered_request_schemas[i] \
                      if i < len(discovered_request_schemas) else {}
            for ep_key, schema_str in schemas.items():
                if not schema_str:
                    continue
                path = ep_key.split(" ")[-1]          # "POST /sort" → "/sort"
                endpoint_valid_keys[path] = set(field_pattern.findall(schema_str))

        for task in plan.get("tasks", []):
            task_name = task.get("task_name", "?")
            url       = task.get("url", "")
            input_val = task.get("input")

            # Salta task senza body strutturato (GET, input stringa/null/vuoto)
            if not isinstance(input_val, dict) or not input_val:
                continue

            matched_valid_keys = None
            for ep_path, valid_keys in endpoint_valid_keys.items():
                if ep_path in url:
                    matched_valid_keys = valid_keys
                    break

            if matched_valid_keys is None:
                continue   # endpoint non nel registry, skip

            hallucinated = set(input_val.keys()) - matched_valid_keys
            if hallucinated:
                warnings.append(
                    f"[SCHEMA VIOLATION] Task '{task_name}' → "
                    f"chiavi non valide: {sorted(hallucinated)} | "
                    f"chiavi ammesse: {sorted(matched_valid_keys)}"
                )

        return warnings

    # ------------------------------------------------------------------
    # LLM
    # ------------------------------------------------------------------

    def query_ollama(self, system_prompt: str, user_prompt: str,
                     input_schema: dict | None = None) -> tuple[str, float]:
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
                                            "enum": ["GET", "POST", "PUT", "DELETE", "SQL"]
                                        },
                                        "input": input_schema or {"type": ["string", "object", "null"]},
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

        # ── EXAMPLE 1: Simple GET with enum filter ────────────────────────────
        # Domain: smart library (not used in test queries)
        # Pattern: single GET with a query param that has an enum value
        ex_get = {
            "reasoning": "The user wants all library books currently available for loan. I query the smart-library service filtering by status=available.",
            "tasks": [{
                "task_name":  "get_available_books",
                "service_id": "smart-library-mock",
                "url":        "http://mock-server:8080/rest/Smart+Library+Management+API/1.0/book?status=available",
                "operation":  "GET",
                "input":      ""
            }]
        }

        # ── EXAMPLE 2: POST with full required body ───────────────────────────
        # Domain: smart sports facility (not used in test queries)
        # Pattern: POST with a complete JSON body including all required fields
        ex_post = {
            "reasoning": "The user wants to reserve a sports court. I call the smart-sports-facility service with the reservation details as a POST body.",
            "tasks": [{
                "task_name":  "create_reservation",
                "service_id": "smart-sports-facility-mock",
                "url":        "http://mock-server:8080/rest/Smart+Sports+Facility+API/1.0/reservation",
                "operation":  "POST",
                "input": {
                    "zoneId":      "Z-NORD",
                    "facilityId":  3,
                    "sport":       "tennis",
                    "courtNumber": 2,
                    "userName":    "Marco Verdi",
                    "date":        "2025-09-26",
                    "startTime":   "10:00",
                    "endTime":     "11:00",
                    "status":      "confirmed"
                }
            }]
        }

        # ── EXAMPLE 3: JMESPath filter + path param injection ─────────────────
        # Domain: smart hospital (not used in test queries)
        # Pattern: GET list → filter by field → inject id into PUT url
        ex_chain_single = {
            "reasoning": "The user wants to discharge a specific patient named Rossi. I GET all patients, filter by surname using JMESPath, and inject the patient id directly into the PUT url to update their status.",
            "tasks": [
                {
                    "task_name":  "get_all_patients",
                    "service_id": "smart-hospital-mock",
                    "url":        "http://mock-server:8080/rest/Smart+Hospital+Management+API/1.0/patient",
                    "operation":  "GET",
                    "input":      ""
                },
                {
                    "task_name":  "discharge_patient",
                    "service_id": "smart-hospital-mock",
                    "url":        "http://mock-server:8080/rest/Smart+Hospital+Management+API/1.0/patient/{{get_all_patients[?surname=='Rossi'] | [0].id}}",
                    "operation":  "PUT",
                    "input": {
                        "zoneId":    "Z-SUD",
                        "surname":   "Rossi",
                        "status":    "discharged",
                        "wardId":    12,
                        "updatedAt": "2025-09-25T14:00:00Z"
                    }
                }
            ]
        }

        # ── EXAMPLE 4: Multi-zone JOIN ────────────────────────────────────────
        # Domain: smart university campus (not used in test queries)
        # Pattern: GET list → extract ALL zoneIds → pass as zoneIds to second service
        ex_chain_multivalore = {
            "reasoning": "The user wants food services near occupied lecture halls. I get all lecture halls with status=occupied, collect ALL their zoneIds, and pass them to the canteen service to find nearby options.",
            "tasks": [
                {
                    "task_name":  "get_occupied_halls",
                    "service_id": "smart-campus-mock",
                    "url":        "http://mock-server:8080/rest/Smart+University+Campus+API/1.0/lecture-hall?status=occupied",
                    "operation":  "GET",
                    "input":      ""
                },
                {
                    "task_name":  "get_canteens_near_halls",
                    "service_id": "smart-campus-mock",
                    "url":        "http://mock-server:8080/rest/Smart+University+Campus+API/1.0/canteen?zoneIds={{get_occupied_halls[*].zoneId | join(',', @)}}&open=true",
                    "operation":  "GET",
                    "input":      ""
                }
            ]
        }

        # ── EXAMPLE 5: Boolean filter + JOIN ──────────────────────────────────
        # Domain: smart agriculture (not used in test queries)
        # Pattern: GET with boolean filter → extract zoneIds → second service
        ex_chain_alert = {
            "reasoning": "The user wants irrigation controllers in zones where soil moisture alerts are active. I get all soil sensors with alertActive=true, collect their zoneIds, and query irrigation controllers in those zones.",
            "tasks": [
                {
                    "task_name":  "get_dry_soil_zones",
                    "service_id": "smart-agriculture-mock",
                    "url":        "http://mock-server:8080/rest/Smart+Agriculture+Monitoring+API/1.0/soil-sensor?alertActive=true",
                    "operation":  "GET",
                    "input":      ""
                },
                {
                    "task_name":  "get_irrigation_in_dry_zones",
                    "service_id": "smart-agriculture-mock",
                    "url":        "http://mock-server:8080/rest/Smart+Agriculture+Monitoring+API/1.0/irrigation-controller?zoneIds={{get_dry_soil_zones[*].zoneId | join(',', @)}}",
                    "operation":  "GET",
                    "input":      ""
                }
            ]
        }

        # ── EXAMPLE 6: OR filter on same field + JOIN ─────────────────────────
        # Domain: smart logistics (not used in test queries)
        # Pattern: GET all (no filter) → OR filter in JMESPath → join zoneIds
        # KEY: when filtering on multiple values of the same field,
        # use ONE GET + JMESPath OR instead of two separate GETs
        ex_or_filter = {
            "reasoning": "The user wants trucks in zones with critically loaded waste bins (full or overflowing). Instead of two separate GETs, I query all bins in one call and apply a JMESPath OR filter to collect zoneIds, then find available trucks in those zones.",
            "tasks": [
                {
                    "task_name":  "get_urgent_bins",
                    "service_id": "smart-logistics-mock",
                    "url":        "http://mock-server:8080/rest/Smart+Logistics+and+Fleet+API/1.0/waste-bin",
                    "operation":  "GET",
                    "input":      ""
                },
                {
                    "task_name":  "get_trucks_for_urgent_zones",
                    "service_id": "smart-logistics-mock",
                    "url":        "http://mock-server:8080/rest/Smart+Logistics+and+Fleet+API/1.0/collection-truck?zoneIds={{get_urgent_bins[?status=='full' || status=='overflowing'].zoneId | join(',', @)}}&available=true",
                    "operation":  "GET",
                    "input":      ""
                }
            ]
        }

        # ── EXAMPLE 7: Substring match → path param injection ──────────────
        # Domain: smart hotel (not used in test queries)
        # Pattern: GET list → contains() filter on composite field → inject id in PUT url
        # KEY: use contains() when the field value may include extra text (e.g. addresses)
        ex7 = {
                "reasoning": "The user wants to update the cleaning status of the room near 'Suite Deluxe'. I GET all rooms, use contains() to find the one whose name includes 'Suite Deluxe', extract its id, and inject it into the PUT url.",
                "tasks": [
                        {
                                "task_name": "get_all_rooms",
                                "service_id": "smart-hotel-mock",
                                "url": "http://mock-server:8080/rest/Smart+Hotel+Management+API/1.0/room",
                                "operation": "GET",
                                "input": ""
                        },
                        {
                                "task_name": "update_room_cleaning",
                                "service_id": "smart-hotel-mock",
                                "url": "http://mock-server:8080/rest/Smart+Hotel+Management+API/1.0/room/{{get_all_rooms[?contains(name, 'Suite Deluxe')] | [0].id}}",
                                "operation": "PUT",
                                "input": {
                                        "cleaningStatus": "in_progress",
                                        "assignedStaff": "Maria Rossi"
                                }
                        }
                ]
        }

        examples_str = (
            f"EXAMPLE 1 — Simple GET with enum filter:\n{json.dumps(ex_get, indent=2)}\n\n"
            f"EXAMPLE 2 — POST with full body:\n{json.dumps(ex_post, indent=2)}\n\n"
            f"EXAMPLE 3 — JMESPath filter + path param injection:\n{json.dumps(ex_chain_single, indent=2)}\n\n"
            f"EXAMPLE 4 — Multi-zone JOIN (collect ALL zones, pass as comma-separated):\n{json.dumps(ex_chain_multivalore, indent=2)}\n\n"
            f"EXAMPLE 5 — Boolean filter + JOIN:\n{json.dumps(ex_chain_alert, indent=2)}\n\n"
            f"EXAMPLE 6 — OR filter on same field (ONE GET + JMESPath, not two GETs):\n{json.dumps(ex_or_filter, indent=2)}\n\n"
            f"EXAMPLE 7 — Substring match on composite field (contains) + path param injection:\n{json.dumps(ex7, indent=2)}"
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
  All values of a field:   {{{{task[*].field}}}}
  Single field list:       {{{{task[*].zoneId}}}}
  Filter + first element:  {{{{task[?key=='value'] | [0].field}}}}
  First match available:   {{{{task[?available==`true`] | [0].id}}}}
  Number filter + first:   {{{{task[?pricePerHour==`0.0`] | [0].id}}}}
  Substring match:         {{{{task[?contains(description, 'urgent')] | [0].id}}}}
  Multi-zone join:         {{{{task[*].zoneId | join(',', @)}}}}
  Filter + join:           {{{{task[?status=='open'].zoneId | join(',', @)}}}}
  Boolean filter + join:   {{{{task[?available==`true`].zoneId | join(',', @)}}}}
  OR filter + join:        {{{{task[?field=='a' || field=='b'].zoneId | join(',', @)}}}}
  Count matches:           {{{{task[?alertActive==`true`] | length(@)}}}}
  Sort ascending (min):    {{{{task | sort_by(@, &waitingTimeMinutes)[0].id}}}}
  Sort descending (max):   {{{{task | sort_by(@, &fillLevel)[-1].id}}}}
  Min element:             {{{{min_by(task, &fillLevel).id}}}}
  Max element:             {{{{max_by(task, &fillLevel).id}}}}

CRITICAL JMESPATH RULES:
  - join(',', @) ONLY works on string fields (e.g. zoneId). NEVER use it on integer fields (e.g. id).
  - To get the first match after a filter, ALWAYS use the pipe: [?key=='val'] | [0].field
    WITHOUT pipe ([?key=='val'][0].field) returns empty — this is a known Python jmespath limitation.
  - [?filter].field extracts a field from all matching items (returns list).
  - [?filter] | [0].field gets a single field from the first match only.
  - [?field==`null`] matches items where the field is absent OR explicitly null — use with caution.
  - Use contains(field, 'substring') for partial string matches (e.g. location contains a street name).
    NEVER use == for fields that may contain extra text like addresses or descriptions.
  - min_by/max_by take the task name as first argument: min_by(task_name, &field).field

POST-PROCESSING — inline JMESPath vs SQL task:
  Use JMESPath inline when: simple filter, first/last element, join comma-separated IDs.
  Use a SQL task when: joining two datasets, avg/sum/stddev, set operations (intersect/diff),
  multi-criteria ranking, grouping, or any operation JMESPath cannot express.

  SQL TASK SYNTAX:
    {{
      "task_name": "your_task_name",
      "service_id": "sql-processor",
      "operation": "SQL",
      "url":        "",
      "input":      {{"sql_query": "SELECT ... FROM previous_task_name WHERE ..."}}
    }}

  - The SQL query MUST be inside an object with key "sql_query". Never a plain string.
  - NEVER use SQL reserved keywords as task_name (e.g. order, group, select, user, table, index). Use descriptive names like get_orders, group_results.
  - Each previous task result is available as a table named after its task_name.
  - Use standard SQL (DuckDB dialect): JOIN, GROUP BY, ORDER BY, LIMIT, AVG, COUNT, etc.
  - Reference previous tasks directly by task_name as table name (no {{{{}}}} placeholders needed).

  SQL EXAMPLES:
    Sort + top-N:   {{"sql_query": "SELECT * FROM get_sensors ORDER BY lastReading ASC LIMIT 1"}}
    Join:           {{"sql_query": "SELECT a.*, b.congestionLevel FROM get_parking a JOIN get_traffic b ON a.zoneId = b.zoneId"}}
    Aggregate:      {{"sql_query": "SELECT zoneId, AVG(lastReading) as avg_aqi FROM get_sensors GROUP BY zoneId"}}
    Intersect:      {{"sql_query": "SELECT * FROM get_bins WHERE zoneId IN (SELECT zoneId FROM get_traffic WHERE congestionLevel='high')"}}
    Diff:           {{"sql_query": "SELECT * FROM get_zones WHERE zoneId NOT IN (SELECT zoneId FROM get_stations)"}}

  NEVER add a duplicate GET task just to sort or filter already-fetched data.

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
9.  operation must be exactly one of: GET, POST, PUT, DELETE, SQL.
10. Every task MUST have a non-empty url starting with http://, EXCEPT SQL tasks (url must be empty string).
11. Required keys per task: task_name, service_id, url, operation, input.
    service_id MUST be exactly the SERVICE_ID value shown in the catalog (e.g. 'smart-traffic-monitoring-mock').
    NEVER use the NAME field as service_id.
12. NEVER create an intermediate GET-by-ID if the ID is already available.
13. PARAMETERS marked * are REQUIRED and must appear in the url.
14. NEVER return a url with unresolved placeholders like {{id}} or {{zoneId}}.
16. NEVER add a duplicate task that calls the same url+service as a previous task.
    Use JMESPath chaining on the existing result or add a SQL task instead.
17. For operations JMESPath cannot do (join, avg, group, rank, intersect, diff),
    add a SQL task (operation: SQL, service_id: sql-processor, url: "").
    Write standard SQL in the input field. Use task names as table names directly.
15. NEVER concatenate two {{{{}}}} placeholders in the same URL param value:
    WRONG: ?zoneIds={{{{task1[*].zoneId | join(',', @)}}}},{{{{task2[*].zoneId | join(',', @)}}}}
    RIGHT: query all data in ONE prior task, then filter with JMESPath OR:
           ?zoneIds={{{{get_all[?level=='high' || level=='critical'].zoneId | join(',', @)}}}}"""

    # ------------------------------------------------------------------
    # USER PROMPT
    # ------------------------------------------------------------------

    def _build_user_prompt(self, discovered_services, discovered_capabilities,
                           discovered_endpoints, discovered_schemas,
                           discovered_request_schemas, discovered_parameters,
                           query, input_files=None) -> str:
        lines = ["SERVICES AND ENDPOINTS:"]
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
        # Costruisce lo schema dinamico per il campo 'input' dai request_schemas del registry
        input_schema = self._build_input_format_schema(discovered_request_schemas)

        response, latency = self.query_ollama(system_prompt, user_prompt, input_schema)
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
          get_sensors[?alertActive==`true`].zoneId | join(',', @)
          get_lights | sort_by(@, &brightness)[0].id
        """
        # Rimuove suffissi legacy
        expr = re.sub(r'\.(output|response|data)\b', '', expr)
        # Normalizza [N] → .[N]
        # Aggiunge '.' prima di [N] solo quando preceduto da word char (task[0] → task.[0])
        # NON dopo spazio o pipe (| [0] deve restare | [0], non | .[0])
        expr = re.sub(r'(?<=\w)(\[)(\d+\])', r'.\1\2', expr)
        m = re.match(r'^(\w+)(.*)', expr, re.DOTALL)
        if not m:
            print(f"[JMESPATH] Impossibile estrarre task_name da '{expr}'")
            return ""

        task_name = m.group(1)
        remainder = m.group(2).strip()

        # ── Funzioni JMESPath usate come prefisso (min_by, max_by, sort_by, ecc.) ──
        # La regex cattura "min_by" come task_name, ma non è un task nel context:
        # è una funzione JMESPath. In questo caso valutiamo l'intera espressione
        # sul context dict, dove i task_name sono chiavi risolvibili da JMESPath.
        JMESPATH_FUNCTIONS = {"min_by", "max_by", "sort_by", "length", "keys",
                              "values", "contains", "starts_with", "ends_with",
                              "reverse", "to_array", "to_string", "to_number", "type"}
        if task_name in JMESPATH_FUNCTIONS:
            try:
                result = jmespath.search(expr, context)
                if result is None or result == [] or result == "":
                    print(f"[JMESPATH] Funzione '{task_name}' — nessun risultato per '{expr}'")
                    return ""
                return result
            except jmespath_exc.JMESPathError as e:
                print(f"[JMESPATH ERROR] funzione '{task_name}': {e}")
                return ""

        val = context.get(task_name)
        if val is None:
            print(f"[JMESPATH] Task '{task_name}' non trovato nel contesto")
            return ""

        if not remainder:
            return val

        # ── Pipe iniziale (es. task | sort_by(@, &field)[0].id) ──────────────────
        # La regex estrae "task" come task_name e "| sort_by(...)" come remainder.
        # Il pipe è un operatore binario: non può iniziare un'espressione JMESPath.
        # Fix: strippa il | iniziale — val è già il left-hand side del pipe.
        if remainder.startswith('|'):
            remainder = remainder[1:].strip()

        # Rimuove il punto iniziale se presente (JMESPath non lo accetta)
        jmespath_expr = remainder[1:] if remainder.startswith('.') else remainder

        if not jmespath_expr:
            return val

        try:
            result = jmespath.search(jmespath_expr, val)
            if result is None:
                print(f"[JMESPATH] Nessun match per '{jmespath_expr}' su '{task_name}'")
                return ""
            # Lista vuota dopo filtro → il param risultante sarà vuoto
            # (es: zoneIds= ) che causa 400 su Microcks — logga warning esplicito
            if isinstance(result, list) and len(result) == 0:
                print(f"[JMESPATH WARN] Empty list for '{jmespath_expr}' on '{task_name}' — resulting URL param will be empty")
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

    def _execute_sql_task(self, task: dict, context: dict) -> dict:
        """
        Esegue un task SQL (operation: SQL) usando DuckDB in-process.

        Ogni entry dell'execution_context viene registrata come tabella DuckDB
        con il nome del task che l'ha prodotta. La query SQL nel campo 'input'
        può referenziare qualsiasi task precedente direttamente per nome,
        senza bisogno di placeholder JMESPath {{...}}.

        Restituisce un dict compatibile con il formato dei risultati di call_agent.
        """
        task_name  = task.get("task_name") or "sql_task"
        input_data = task.get("input", {})

        # input è un oggetto {"sql_query": "SELECT ..."} — estrae la query
        if isinstance(input_data, dict):
            sql_query = input_data.get("sql_query", "")
        else:
            sql_query = ""

        if not sql_query:
            return {
                "task_name":   task_name,
                "operation":   "SQL",
                "status":      "ERROR",
                "status_code": 400,
                "result":      "SQL task requires input={'sql_query': 'SELECT ...'}",
            }

        tail = "..." if len(sql_query) > 120 else ""
        print(f"[SQL] Task '{task_name}': {sql_query[:120]}{tail}")

        # ── Edge Case 2: sanifica nomi tabella che sono parole riservate SQL ─
        SQL_RESERVED = {
            "order","group","select","where","from","join","table","user",
            "index","key","column","database","schema","view","limit","offset",
            "having","union","insert","update","delete","create","drop","alter",
            "with","as","on","in","not","and","or","is","null","true","false",
        }

        def safe_tbl(name: str) -> str:
            """Aggiunge prefisso t_ se il nome è una keyword SQL riservata."""
            return f"t_{name}" if name.lower() in SQL_RESERVED else name

        # Se il task_name stesso è riservato, rinomina anche nella query
        safe_task_name = safe_tbl(task_name)
        if safe_task_name != task_name:
            print(f"[SQL WARN] task_name '{task_name}' è una keyword SQL — rinominato '{safe_task_name}' nella query")
            sql_query = re.sub(rf'\b{re.escape(task_name)}\b', safe_task_name, sql_query)

        try:
            conn = duckdb.connect()   # database in-memory, isolato per ogni task

            # Registra ogni risultato precedente come tabella DuckDB
            for tbl, data in context.items():
                safe_name = safe_tbl(tbl)
                # Se il nome era riservato, aggiorna la query per usare il nome sicuro
                if safe_name != tbl:
                    sql_query = re.sub(rf'\b{re.escape(tbl)}\b', safe_name, sql_query)

                if isinstance(data, list):
                    if not data:
                        # Lista vuota: registra tabella sentinel — colonne sconosciute
                        # producono Binder Error se la query le referenzia,
                        # gestito nel blocco except come graceful empty result
                        conn.execute(f'CREATE TABLE "{safe_name}" (dummy VARCHAR)')
                    else:
                        df = pd.DataFrame(data)
                        conn.register(safe_name, df)
                elif isinstance(data, dict):
                    df = pd.DataFrame([data])
                    conn.register(safe_name, df)

            rel  = conn.execute(sql_query)
            rows = rel.fetchall()
            cols = [desc[0] for desc in rel.description]

            # ── Bug critico: sanitizza NaN e datetime in uscita da DuckDB ─────
            # pd.DataFrame inserisce NaN per campi mancanti e converte ISO strings
            # in datetime — entrambi non serializzabili da json.dumps nativo.
            result = []
            for row in rows:
                row_dict = {}
                for col, val in zip(cols, row):
                    if isinstance(val, float) and math.isnan(val):
                        val = None                  # NaN → null JSON
                    elif hasattr(val, 'isoformat'):
                        val = val.isoformat()       # datetime → stringa ISO
                    row_dict[col] = val
                result.append(row_dict)

            conn.close()
            print(f"[SQL] Task '{task_name}' completato — {len(result)} righe.")
            return {
                "task_name":   task_name,
                "operation":   "SQL",
                "status":      "SUCCESS",
                "status_code": 200,
                "result":      result,
            }

        except Exception as e:
            error_msg = str(e)
            print(f"[SQL ERROR] Task '{task_name}': {error_msg}")

            # ── Edge Case 1: Binder Error da tabella sentinel (lista vuota) ──
            # Se un task precedente ha restituito [] e la query referenzia le sue
            # colonne, DuckDB genera un Binder Error perché la tabella ha solo
            # la colonna 'dummy'. Restituiamo [] con SUCCESS invece di 500,
            # così la pipeline non si interrompe per mancanza di dati a monte.
            if "Binder Error" in error_msg or "dummy" in error_msg:
                print(f"[SQL WARN] Task '{task_name}': Binder Error su tabella vuota — restituito risultato vuoto")
                return {
                    "task_name":   task_name,
                    "operation":   "SQL",
                    "status":      "SUCCESS",
                    "status_code": 200,
                    "result":      [],
                }

            return {
                "task_name":   task_name,
                "operation":   "SQL",
                "status":      "ERROR",
                "status_code": 500,
                "result":      error_msg,
            }

    async def trigger_agents_async(self, agents: dict, discovered_services):
        results = []
        context = {}
        async with aiohttp.ClientSession() as session:
            for task in agents.get("tasks", []):
                operation = str(task.get("operation") or "GET").upper()

                # ── SQL task: esecuzione DuckDB in-process ────────────────────
                if operation == "SQL":
                    result = self._execute_sql_task(task, context)
                else:
                    task["url"] = self.resolve_placeholders(task.get("url") or "", context)
                    if task.get("input"):
                        task["input"] = self.resolve_placeholders(task["input"], context)
                    result = await self.call_agent(session, task, discovered_services)

                if result.get("status") == "FILE":
                    return result
                results.append(result)
                name = task.get("task_name") or "unnamed_task"
                if result.get("status") == "SUCCESS":
                    raw = result.get("result", {})
                    if isinstance(raw, (dict, list)):
                        context[name] = raw
                    else:
                        print(f"[CONTEXT] '{name}' returned non-JSON ({type(raw).__name__}) — not stored in chain context")
                        context[name] = {}
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
            # Sostituisce localhost con l'indirizzo del mock server (URL dal YAML)
            if url and re.search(r'http://localhost:\d+', url):
                fixed = re.sub(r'http://localhost:\d+', mock_url, url)
                print(f"[AUTO-FIX] localhost → mock-server: {fixed}")
                task["url"] = fixed
                url = fixed
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

        # ── Validazione schema input (post-parse) ─────────────────────────────
        schema_warnings = self._validate_plan(plan, disc_services, disc_req_schemas)
        if schema_warnings:
            print(f"\n{'⚠️  ' * 10}")
            print(f"[SCHEMA VALIDATOR] {len(schema_warnings)} violazione/i rilevata/e:")
            for w in schema_warnings:
                print(f"  {w}")
            print(f"{'⚠️  ' * 10}\n")
        else:
            print("[SCHEMA VALIDATOR] ✅ Nessuna violazione rilevata.")

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