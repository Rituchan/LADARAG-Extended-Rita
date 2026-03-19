from concurrent.futures import ThreadPoolExecutor
import requests
import yaml
import prance
import os
import json
import re
from urllib.parse import quote, unquote


class Service:
    CONSUL_HOST = os.environ.get("CONSUL_HOST", "registry")
    CONSUL_PORT = int(os.environ.get("CONSUL_PORT", 8500))

    GATEWAY_HOST = os.environ.get("GATEWAY_HOST", "catalog-gateway")
    GATEWAY_PORT = int(os.environ.get("GATEWAY_PORT", 5000))

    HEALTHCHECK_SERVICE_HOST = os.environ.get("HEALTHCHECK_SERVICE_HOST", "healthcheck-service")
    HEALTHCHECK_SERVICE_PORT = os.environ.get("HEALTHCHECK_SERVICE_PORT", 5600)

    MOCK_SERVER_URL = os.environ.get("MOCK_SERVER_URL", "http://mock-server:8080")

    def fetch_providers(self):
        url = "https://api.apis.guru/v2/providers.json"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()["data"]

    def fetch_api_details(self, provider):
        url = f"https://api.apis.guru/v2/{provider}.json"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def extract_swagger_url(self, api_data, provider):
        apis = api_data.get("apis")

        if isinstance(apis, dict):
            for version_key, version_data in apis.items():
                swagger_url = version_data.get("swaggerYamlUrl")
                if swagger_url:
                    return swagger_url

        elif isinstance(apis, list):
            for entry in apis:
                if isinstance(entry, dict):
                    swagger_url = entry.get("swaggerYamlUrl") or entry.get("swaggerUrl")
                    if swagger_url:
                        return swagger_url

        print(f"[WARN] swaggerYamlUrl not found for provider {provider}")
        return None

    def extract_endpoints_from_swagger(self, swagger_url):
        response = requests.get(swagger_url)
        if response.status_code != 200:
            return []
        try:
            swagger_data = yaml.safe_load(response.text)
            return list(swagger_data.get("paths", {}).keys())
        except yaml.YAMLError:
            return []

    # -----------------------------------------------------------------------
    # Helpers per estrazione schema — dotted keys con tipi compatti
    # -----------------------------------------------------------------------

    def _map_type(self, prop_schema):
        """
        Mappa uno schema OpenAPI di una singola property a un tipo compatto.

        Mapping:
          integer / number  → int / float
          string + enum     → enum
          string            → str
          boolean           → bool
          array             → arr
          object            → obj  (solo se foglia, altrimenti si ricorre)
          default           → any
        """
        if not isinstance(prop_schema, dict):
            return "any"

        if prop_schema.get("enum"):
            return "enum"

        oa_type = prop_schema.get("type", "")

        if oa_type == "integer":
            return "int"
        if oa_type == "number":
            return "float"
        if oa_type == "boolean":
            return "bool"
        if oa_type == "array":
            return "arr"
        if oa_type == "string":
            return "str"
        if oa_type == "object" or "properties" in prop_schema or "allOf" in prop_schema:
            return "obj"

        return "any"

    def _flatten_dotted(self, schema, prefix=""):
        """
        Riceve uno schema OpenAPI già dereferenziato da prance e restituisce
        un dict piatto { "dotted.key": "type_label" } espandendo ricorsivamente
        gli oggetti annidati.

        Gestisce:
          - properties dirette
          - allOf: merge ricorsivo completo di tutti i sotto-schemi
          - anyOf / oneOf: merge di TUTTE le varianti (non solo la prima),
            per non perdere campi in schemi polimorfici
          - type: array → ricorre sugli items con lo stesso prefix
        """
        if not isinstance(schema, dict):
            return {}

        result = {}

        # allOf: merge ricorsivo di tutti i sotto-schemi
        for sub in schema.get("allOf", []):
            result.update(self._flatten_dotted(sub, prefix))

        # anyOf / oneOf: merge di TUTTE le varianti con properties
        for combinator in ("anyOf", "oneOf"):
            for sub in schema.get(combinator, []):
                result.update(self._flatten_dotted(sub, prefix))

        # properties dirette
        for prop_name, prop_schema in schema.get("properties", {}).items():
            if not isinstance(prop_schema, dict):
                prop_schema = {}

            full_key = f"{prefix}{prop_name}"

            # Se la property contiene a sua volta properties (oggetto annidato),
            # ricorriamo invece di emettere "obj" come foglia
            sub_props = self._flatten_dotted(prop_schema, f"{full_key}.")
            if sub_props:
                result.update(sub_props)
            else:
                result[full_key] = self._map_type(prop_schema)

        # type: array → ricorriamo sugli items
        if not result and schema.get("type") == "array":
            items = schema.get("items", {})
            result.update(self._flatten_dotted(items, prefix))

        return result

    def _infer_type_from_value(self, value):
        """Inferisce il tipo compatto da un valore di esempio concreto."""
        if isinstance(value, bool):
            return "bool"
        if isinstance(value, int):
            return "int"
        if isinstance(value, float):
            return "float"
        if isinstance(value, str):
            return "str"
        if isinstance(value, list):
            return "arr"
        if isinstance(value, dict):
            return "obj"
        return "any"

    def _flatten_dotted_from_example(self, example_value, prefix=""):
        """
        Versione di _flatten_dotted che lavora su un valore di esempio concreto
        invece che su uno schema OpenAPI. Usata come fallback quando lo schema
        è assente o privo di properties.

        Inferisce i tipi dai valori runtime dell'example.
        """
        if isinstance(example_value, dict):
            result = {}
            for key, val in example_value.items():
                full_key = f"{prefix}{key}"
                if isinstance(val, dict):
                    sub = self._flatten_dotted_from_example(val, f"{full_key}.")
                    result.update(sub) if sub else result.update({full_key: "obj"})
                else:
                    result[full_key] = self._infer_type_from_value(val)
            return result

        elif isinstance(example_value, list) and example_value:
            return self._flatten_dotted_from_example(example_value[0], prefix)

        return {}

    def _schema_to_string(self, flat_dict, is_array):
        """
        Converte un dict { dotted.key: type } nella stringa compatta
        da salvare in MongoDB e passare all'LLM nel prompt.

        Esempi:
          is_array=False → {id:int, location:str, fillLevel:int, status:enum}
          is_array=True  → [{id:int, location:str, fillLevel:int, status:enum}]
        """
        if not flat_dict:
            return None
        inner = ", ".join(f"{k}:{v}" for k, v in flat_dict.items())
        return f"[{{{inner}}}]" if is_array else f"{{{inner}}}"

    def _collect_required_fields(self, schema):
        """
        Raccoglie ricorsivamente tutti i campi required da uno schema,
        gestendo allOf (pattern comune per extends di oggetti in OpenAPI).
        Ritorna un set di nomi di campi required al livello top.
        """
        required = set(schema.get("required", []))
        for sub in schema.get("allOf", []):
            required.update(sub.get("required", []))
        return required

    def _extract_schema_from_details(self, details):
        """
        Estrae lo schema dei campi dalla risposta 2xx di un endpoint.

        Priorità:
          1. Schema OpenAPI (source of truth contrattuale) — OpenAPI 3.0 e Swagger 2.0
          2. Examples come fallback — solo quando lo schema è assente o senza properties

        Note:
          - HTTP 204 escluso intenzionalmente: non ha body per definizione.
          - anyOf/oneOf: merge di tutte le varianti (non solo la prima).
          - Oggetti annidati: espansi in dotted keys (es. sensor.type:str).
          - Formato output: {id:int, sensor.type:str, status:enum}
        """
        responses = details.get("responses", {})

        # Cerca 200 o 201 — 204 escluso: "No Content", nessun body
        success_response = None
        for code in ["200", "201", 200, 201]:
            if code in responses:
                success_response = responses[code]
                break

        if not success_response or not isinstance(success_response, dict):
            return None

        # Swagger 2.0
        swagger2_schema   = success_response.get("schema", {})
        swagger2_examples = success_response.get("examples", {})
        swagger2_json_ex  = swagger2_examples.get("application/json") if isinstance(swagger2_examples, dict) else None

        # OpenAPI 3.0
        content      = success_response.get("content", {})
        json_content = content.get("application/json", {})

        # --- Strategia 1: Schema (source of truth) ---
        final_schema = json_content.get("schema") or swagger2_schema

        if isinstance(final_schema, dict) and final_schema:
            is_array  = final_schema.get("type") == "array"
            flat_dict = self._flatten_dotted(final_schema)
            if flat_dict:
                return self._schema_to_string(flat_dict, is_array)

        # --- Strategia 2: Examples come fallback ---

        # 2a. OpenAPI 3.0 examples (plurale)
        examples = json_content.get("examples", {})
        if examples:
            first_example = next(iter(examples.values()), None)
            if isinstance(first_example, dict) and first_example.get("value") is not None:
                ex_val    = first_example["value"]
                is_array  = isinstance(ex_val, list)
                flat_dict = self._flatten_dotted_from_example(ex_val)
                if flat_dict:
                    return self._schema_to_string(flat_dict, is_array)

        # 2b. OpenAPI 3.0 example (singolare)
        if "example" in json_content and json_content["example"] is not None:
            ex_val    = json_content["example"]
            is_array  = isinstance(ex_val, list)
            flat_dict = self._flatten_dotted_from_example(ex_val)
            if flat_dict:
                return self._schema_to_string(flat_dict, is_array)

        # 2c. Swagger 2.0 explicit JSON example
        if swagger2_json_ex is not None:
            is_array  = isinstance(swagger2_json_ex, list)
            flat_dict = self._flatten_dotted_from_example(swagger2_json_ex)
            if flat_dict:
                return self._schema_to_string(flat_dict, is_array)

        return None

    def _extract_request_schema_from_details(self, details, method):
        """
        Estrae lo schema del body di input per POST e PUT.
        GET e DELETE non hanno body — vengono ignorati.

        I campi required sono marcati con * nel formato output.
        Formato output: {location:str*, fillLevel:int*, binType:enum, status:enum*}

        Gestisce:
          - OpenAPI 3.0: requestBody.content.application/json.schema
          - Swagger 2.0: parameters[in=body].schema
        """
        if method.upper() not in {"POST", "PUT"}:
            return None

        schema = None

        # OpenAPI 3.0
        request_body = details.get("requestBody", {})
        if isinstance(request_body, dict):
            content      = request_body.get("content", {})
            json_content = content.get("application/json", {})
            schema       = json_content.get("schema")

        # Swagger 2.0 — body parameter
        if not schema:
            for param in details.get("parameters", []):
                if isinstance(param, dict) and param.get("in") == "body":
                    schema = param.get("schema")
                    break

        if not schema or not isinstance(schema, dict):
            return None

        # Raccoglie i required (gestisce allOf)
        required_fields = self._collect_required_fields(schema)

        flat_dict = self._flatten_dotted(schema)
        if not flat_dict:
            return None

        # Serializza con marker * sui campi required
        parts = []
        for key, type_label in flat_dict.items():
            # Per dotted keys (es. address.street), il campo root è la prima parte
            root_field = key.split(".")[0]
            marker = "*" if root_field in required_fields else ""
            parts.append(f"{key}:{type_label}{marker}")

        inner = ", ".join(parts)
        return f"{{{inner}}}"

    def _build_swagger_url_from_endpoint(self, endpoint_url, mock_server_url):
        """
        Ricostruisce l'URL dello YAML Microcks a partire da un endpoint
        già memorizzato in MongoDB.

        Esempio:
          endpoint_url = "http://localhost:8585/rest/Smart+Charging+Stations+API/1.0/health"
          → "http://mock-server:8080/api/resources/Smart%20Charging%20Stations%20API-1.0.yaml"
        """
        match = re.search(r"/rest/(.+)", endpoint_url)
        if not match:
            return None

        rest_path = match.group(1)
        parts     = rest_path.split("/")
        if len(parts) < 2:
            return None

        api_name_raw     = parts[0]
        version          = parts[1]
        api_name         = unquote(api_name_raw.replace("+", " "))
        api_name_encoded = quote(api_name)

        filename = f"{api_name_encoded}-{version}.yaml"
        return f"{mock_server_url.rstrip('/')}/api/resources/{filename}"

    def _extract_schemas_from_yaml(self, swagger_url):
        """
        Usa prance per scaricare e dereferenziare lo YAML, poi estrae
        response_schemas e request_schemas per ogni endpoint.

        Ritorna un dict:
          {
            "response_schemas": { "GET /path": "{id:int, ...}", ... },
            "request_schemas":  { "POST /path": "{field:type*, ...}", ... }
          }
        """
        try:
            parser  = prance.ResolvingParser(
                swagger_url,
                lazy=False,
                strict=False,
            )
            swagger = parser.specification
        except Exception as e:
            print(f"[ENRICH] prance failed ({swagger_url}): {e}")
            return {"response_schemas": {}, "request_schemas": {}}

        response_schemas = {}
        request_schemas  = {}

        paths = swagger.get("paths", {})
        for path, methods in paths.items():
            if not isinstance(methods, dict):
                continue
            for method, details in methods.items():
                if method.lower() not in {"get", "post", "put", "delete"}:
                    continue
                if not isinstance(details, dict):
                    continue

                key = f"{method.upper()} {path}"

                resp_schema = self._extract_schema_from_details(details)
                if resp_schema:
                    response_schemas[key] = resp_schema

                req_schema = self._extract_request_schema_from_details(details, method)
                if req_schema:
                    request_schemas[key] = req_schema

        return {"response_schemas": response_schemas, "request_schemas": request_schemas}

    # -----------------------------------------------------------------------
    # Metodo principale di enrichment
    # -----------------------------------------------------------------------

    def enrich_schemas(self, mock_server_url=None, service_id=None):
        """
        Recupera tutti i servizi dal catalog-gateway, scarica il loro YAML
        da Microcks tramite prance, estrae response_schemas e request_schemas
        e aggiorna ogni documento tramite PATCH /services/<id>/schemas.

        Ritorna un dict { enriched, skipped, errors }.
        """
        mock_server_url = mock_server_url or self.MOCK_SERVER_URL
        gateway_base    = f"http://{self.GATEWAY_HOST}:{self.GATEWAY_PORT}"

        try:
            resp = requests.get(f"{gateway_base}/services", timeout=10)
            resp.raise_for_status()
            all_services = resp.json()
        except Exception as e:
            print(f"[ENRICH] Cannot fetch services from gateway: {e}")
            return {"enriched": 0, "skipped": 0, "errors": 1}

        if service_id:
            all_services = [s for s in all_services if s.get("_id") == service_id]

        enriched = 0
        skipped  = 0
        errors   = 0

        for svc in all_services:
            doc_id = svc.get("_id")

            # Skippa se ha già gli schemi
            if "response_schemas" in svc and "request_schemas" in svc:
                print(f"[ENRICH] Skipping {doc_id} (already enriched)")
                skipped += 1
                continue

            # STRATEGIA AGGIORNATA: Usa l'URL salvato nel DB
            swagger_url = svc.get("swagger_url")

            # Fallback opzionale a Microcks solo per vecchi dati o servizi mockati specifici
            if not swagger_url:
                endpoints = svc.get("endpoints", {})
                for ep_url in endpoints.values():
                    if isinstance(ep_url, str) and "/rest/" in ep_url:
                        swagger_url = self._build_swagger_url_from_endpoint(ep_url, mock_server_url)
                        break

            if not swagger_url:
                print(f"[ENRICH] Cannot find or reconstruct swagger URL for {doc_id}")
                skipped += 1
                continue

            schemas = self._extract_schemas_from_yaml(swagger_url)

            try:
                patch_resp = requests.patch(
                    f"{gateway_base}/services/{doc_id}/schemas",
                    json=schemas,
                    timeout=10
                )
                patch_resp.raise_for_status()
                n_resp = len(schemas.get("response_schemas", {}))
                n_req  = len(schemas.get("request_schemas", {}))
                print(f"[ENRICH] {doc_id} → {n_resp} response schemas, {n_req} request schemas saved")
                enriched += 1
            except Exception as e:
                print(f"[ENRICH] Failed to update {doc_id}: {e}")
                errors += 1

        return {"enriched": enriched, "skipped": skipped, "errors": errors}

    # -----------------------------------------------------------------------
    # Metodi originali di registrazione
    # -----------------------------------------------------------------------

    def parse_swagger(self, service, swagger_url):
        try:
            parser  = prance.ResolvingParser(
                swagger_url,
                lazy=False,
                strict=False,
            )
            swagger = parser.specification
        except Exception as e:
            print(f"Failed to load/resolve YAML from {swagger_url}: {e}")
            return None

        try:
            info         = swagger.get("info", {})
            service_name = info.get("title", service)
            paths        = swagger.get("paths", {})
            servers      = swagger.get("servers")

            if servers and isinstance(servers, list) and len(servers) > 0 and isinstance(servers[0], dict):
                # OpenAPI 3.0
                host_url = servers[0].get("url", "http://localhost")
            else:
                # Fallback per Swagger 2.0
                host      = swagger.get("host", "localhost")
                schemes   = swagger.get("schemes", ["http"])
                base_path = swagger.get("basePath", "")
                scheme    = schemes[0] if isinstance(schemes, list) and schemes else "http"
                host_url  = f"{scheme}://{host}{base_path}"

        except Exception as e:
            print(f"Failed to get information from parsed swagger: {e}")
            return None

        capabilities     = {}
        endpoints        = {}
        response_schemas = {}
        request_schemas  = {}

        for path, methods in paths.items():
            if not isinstance(methods, dict):
                continue
            for method, details in methods.items():
                if method.lower() not in {"get", "post", "put", "delete"}:
                    continue
                if not isinstance(details, dict):
                    continue

                key      = f"{method.upper()} {path}"
                desc     = details.get("description") or details.get("summary") or details.get("operationId") or method
                full_url = f"{host_url.rstrip('/')}{path}"

                capabilities[key] = desc
                endpoints[key]    = full_url

                resp_schema = self._extract_schema_from_details(details)
                if resp_schema:
                    response_schemas[key] = resp_schema

                req_schema = self._extract_request_schema_from_details(details, method)
                if req_schema:
                    request_schemas[key] = req_schema

        return {
            "id":               service,
            "name":             service_name,
            "description":      swagger.get("info", {}).get("description", "No description"),
            "swagger_url":      swagger_url,
            "capabilities":     capabilities,
            "endpoints":        endpoints,
            "response_schemas": response_schemas,
            "request_schemas":  request_schemas,
        }

    def register_to_redis(self, service_name, service_status):
        headers      = {"Content-Type": "application/json"}
        json_payload = {"key": service_name, "value": service_status}
        response = requests.post(
            f"http://{self.HEALTHCHECK_SERVICE_HOST}:{self.HEALTHCHECK_SERVICE_PORT}/status/register",
            headers=headers,
            data=json.dumps(json_payload)
        )
        if response.status_code == 200:
            print(f"Registered {service_name} to Redis with status {service_status}")
        else:
            print(f"Failed to register {service_name} to Redis: HTTP {response.status_code}")

    def register_to_consul(self, service_id, service_name):
        payload = {
            "Name": service_name,
            "Id":   service_id,
            "Meta": {"service_doc_id": service_id},
            "Check": {
                "TlsSkipVerify":                 True,
                "Method":                         "GET",
                "Http":                           f"http://{self.HEALTHCHECK_SERVICE_HOST}:{self.HEALTHCHECK_SERVICE_PORT}/status/{service_id}",
                "Interval":                       "10s",
                "Timeout":                        "5s",
                "DeregisterCriticalServiceAfter": "30s"
            }
        }
        try:
            url      = f"http://{self.CONSUL_HOST}:{self.CONSUL_PORT}/v1/agent/service/register"
            response = requests.put(url, json=payload)
            print(f"Consul registration ({service_id}): HTTP {response.status_code}")
        except Exception as e:
            print(f"Failed to register to Consul: {e}")

    def register_to_mongo(self, catalog_payload):
        try:
            response = requests.post(
                f"http://{self.GATEWAY_HOST}:{self.GATEWAY_PORT}/service",
                json=catalog_payload
            )
            print(f"Catalog registered to Mongo API: HTTP {response.status_code}")
        except Exception as e:
            print(f"Failed to send catalog payload: {e}")

    def import_apis(self):
        all_endpoints = {}
        providers     = self.fetch_providers()
        print(f"Found {len(providers)} providers.")
        for provider in providers:
            try:
                api_data    = self.fetch_api_details(provider)
                swagger_url = self.extract_swagger_url(api_data, provider)
                all_endpoints[provider] = swagger_url
            except Exception as e:
                print(f"Error processing {provider}: {e}")

        print(f"\nFound: {len(all_endpoints)} endpoints.")
        for service, swagger_url in all_endpoints.items():
            if not swagger_url:
                print(f"[SKIP] {service}: swagger URL not found.")
                continue

            openapi = self.parse_swagger(service, swagger_url)
            if not openapi:
                print(f"[SKIP] {service}: error parsing swagger.")
                continue

            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = [
                    executor.submit(self.register_to_redis, service, "true"),
                    executor.submit(self.register_to_consul, service, openapi.get("id", service)),
                    executor.submit(self.register_to_mongo, openapi)
                ]

                # BARRIER
                for future in futures:
                    try:
                        future.result()
                    except Exception as e:
                        print(f"[ERROR] Register function failed: {e}")