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
    # Helpers per estrazione schema
    # -----------------------------------------------------------------------

    def _extract_response_schema(self, example_value):
        """Riduce un esempio di risposta ai soli nomi dei campi."""
        if isinstance(example_value, list):
            if not example_value:
                return "[]"
            first = example_value[0]
            if isinstance(first, dict):
                return f"[{{{', '.join(first.keys())}}}]"
            return f"[{type(first).__name__}]"
        elif isinstance(example_value, dict):
            return f"{{{', '.join(example_value.keys())}}}"
        elif example_value is not None:
            return str(example_value)
        return ""

    def _flatten_schema(self, schema):
        """
        Riceve uno schema OpenAPI già dereferenziato da prance e restituisce
        un dict piatto di properties gestendo:
          - properties dirette
          - allOf: merge ricorsivo completo
          - anyOf / oneOf: usa il primo sotto-schema con properties
          - type: array con items
        """
        if not isinstance(schema, dict):
            return {}

        merged = {}

        for sub in schema.get("allOf", []):
            merged.update(self._flatten_schema(sub))

        for combinator in ("anyOf", "oneOf"):
            if not merged:
                for sub in schema.get(combinator, []):
                    candidate = self._flatten_schema(sub)
                    if candidate:
                        merged.update(candidate)
                        break

        merged.update(schema.get("properties", {}))

        if not merged and schema.get("type") == "array":
            merged.update(self._flatten_schema(schema.get("items", {})))

        return merged

    def _extract_schema_from_details(self, details):
        """
        Estrae lo skeleton dei campi dalla risposta 2xx di un endpoint.
        Risolve i problemi di compatibilità con Swagger 2.0, OpenAPI 3.0 e chiavi numeriche.
        """
        responses = details.get("responses", {})

        success_response = None
        # FIX 1: Ricerca robusta sia per stringhe che per interi ("200" e 200)
        for code in ["200", "201", "204", 200, 201, 204]:
            if code in responses:
                success_response = responses[code]
                break

        if not success_response or not isinstance(success_response, dict):
            return None

        # FIX 2: Supporto per Swagger 2.0 (usato da apis.guru)
        swagger2_schema = success_response.get("schema", {})
        swagger2_examples = success_response.get("examples", {})
        swagger2_json_ex = swagger2_examples.get("application/json") if isinstance(swagger2_examples, dict) else None

        # OpenAPI 3.0
        content = success_response.get("content", {})
        json_content = content.get("application/json", {})

        # --- Strategia 1: Estrazione dagli Examples ---
        # 1a. OpenAPI 3.0 (examples plurale)
        examples = json_content.get("examples", {})
        if examples:
            first_example = next(iter(examples.values()), None)
            if isinstance(first_example, dict) and first_example.get("value") is not None:
                return self._extract_response_schema(first_example["value"])
                
        # 1b. OpenAPI 3.0 (example singolare)
        if "example" in json_content and json_content["example"] is not None:
            return self._extract_response_schema(json_content["example"])

        # 1c. Swagger 2.0 explicit JSON example
        if swagger2_json_ex is not None:
            return self._extract_response_schema(swagger2_json_ex)

        # --- Strategia 2: Estrazione dallo Schema ---
        # Unisce fallback per OpenAPI 3.0 e Swagger 2.0
        final_schema = json_content.get("schema") or swagger2_schema
        
        if isinstance(final_schema, dict) and final_schema:
            is_array = final_schema.get("type") == "array"
            props = self._flatten_schema(final_schema)
            if props:
                keys = ", ".join(props.keys())
                return f"[{{{keys}}}]" if is_array else f"{{{keys}}}"

        return None

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
        response_schemas per ogni endpoint.
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
            return {}

        response_schemas = {}
        paths = swagger.get("paths", {})
        for path, methods in paths.items():
            if not isinstance(methods, dict):
                continue
            for method, details in methods.items():
                if method.lower() not in {"get", "post", "put", "delete"}:
                    continue
                if not isinstance(details, dict):
                    continue
                key    = f"{method.upper()} {path}"
                schema = self._extract_schema_from_details(details)
                if schema:
                    response_schemas[key] = schema

        return response_schemas

    # -----------------------------------------------------------------------
    # Metodo principale di enrichment
    # -----------------------------------------------------------------------

    def enrich_schemas(self, mock_server_url=None, service_id=None):
        """
        Recupera tutti i servizi dal catalog-gateway, scarica il loro YAML
        da Microcks tramite prance, estrae response_schemas e aggiorna
        ogni documento tramite PATCH /services/<id>/schemas.

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

            if svc.get("response_schemas"):
                print(f"[ENRICH] Skipping {doc_id} (already has schemas)")
                skipped += 1
                continue

            endpoints   = svc.get("endpoints", {})
            swagger_url = None
            for ep_url in endpoints.values():
                if isinstance(ep_url, str) and "/rest/" in ep_url:
                    swagger_url = self._build_swagger_url_from_endpoint(ep_url, mock_server_url)
                    break

            if not swagger_url:
                print(f"[ENRICH] Cannot reconstruct swagger URL for {doc_id}")
                skipped += 1
                continue

            schemas = self._extract_schemas_from_yaml(swagger_url)

            try:
                patch_resp = requests.patch(
                    f"{gateway_base}/services/{doc_id}/schemas",
                    json={"response_schemas": schemas},
                    timeout=10
                )
                patch_resp.raise_for_status()
                print(f"[ENRICH] {doc_id} → {len(schemas)} schemas saved")
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
                host = swagger.get("host", "localhost")
                schemes = swagger.get("schemes", ["http"])
                base_path = swagger.get("basePath", "")
                scheme = schemes[0] if isinstance(schemes, list) and schemes else "http"
                host_url = f"{scheme}://{host}{base_path}"
                
        except Exception as e:
            print(f"Failed to get information from parsed swagger: {e}")
            return None

        capabilities     = {}
        endpoints        = {}
        response_schemas = {}

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

                schema = self._extract_schema_from_details(details)
                if schema:
                    response_schemas[key] = schema

        return {
            "id":               service,
            "name":             service_name,
            "description":      swagger.get("info", {}).get("description", "No description"),
            "capabilities":     capabilities,
            "endpoints":        endpoints,
            "response_schemas": response_schemas,
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
                "TlsSkipVerify":                True,
                "Method":                        "GET",
                "Http":                          f"http://{self.HEALTHCHECK_SERVICE_HOST}:{self.HEALTHCHECK_SERVICE_PORT}/status/{service_id}",
                "Interval":                      "10s",
                "Timeout":                       "5s",
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
                for future in futures:
                    try:
                        future.result()
                    except Exception as e:
                        print(f"[ERROR] Register function failed: {e}")