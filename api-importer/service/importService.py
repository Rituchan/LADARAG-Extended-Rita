from concurrent.futures import ThreadPoolExecutor
import requests
import yaml
import os
import json

class Service:
    CONSUL_HOST = os.environ.get("CONSUL_HOST", "registry")
    CONSUL_PORT = int(os.environ.get("CONSUL_PORT", 8500))

    GATEWAY_HOST = os.environ.get("GATEWAY_HOST", "catalog-gateway")
    GATEWAY_PORT = int(os.environ.get("GATEWAY_PORT", 5000))

    HEALTHCHECK_SERVICE_HOST = os.environ.get("HEALTHCHECK_SERVICE_HOST", "healthcheck-service")
    HEALTHCHECK_SERVICE_PORT = os.environ.get("HEALTHCHECK_SERVICE_PORT", 5600)

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
        
    def parse_swagger(self, service, swagger_url):
        try:
            response = requests.get(swagger_url, timeout=10)
            response.raise_for_status()
            cleaned_text = response.text.replace('\t', ' ')
            swagger = yaml.load(cleaned_text, Loader=yaml.BaseLoader)
        except Exception as e:
            print(f"Failed to load YAML from {swagger_url}: {e}")
            return None
        try:
            info = swagger.get("info", {})
            service_name = info.get("title", service)
            paths = swagger.get("paths", {})
            servers = swagger.get("servers", [{"url": "http://localhost"}])
            if isinstance(servers, list) and len(servers) > 0 and isinstance(servers[0], dict):
                host_url = servers[0].get("url", "http://localhost")
            else:
                host_url = "http://localhost"
        except Exception as e:
            print(f"Failed to get information from parsed swagger: {e}")


        capabilities = {}
        endpoints = {}

        for path, methods in paths.items():
            if not isinstance(methods, dict):
                continue

            for method, details in methods.items():
                if method.lower() not in {"get", "post", "put", "delete"}:
                    continue

                if not isinstance(details, dict):
                    continue

                key = f"{method.upper()} {path}"
                desc = details.get("description") or details.get("summary") or details.get("operationId") or method
                full_url = f"{host_url.rstrip('/')}{path}"

                capabilities[key] = desc
                endpoints[key] = full_url


        return {
            "id": service,
            "name": service_name,
            "description": swagger.get("info", {}).get("description", "No description"),
            "capabilities": capabilities,
            "endpoints": endpoints
        }

    def register_to_redis(self, service_name, service_status):
        headers = {
            "Content-Type": "application/json"
        }

        json_payload = {
            "key": service_name,
            "value": service_status
        }
        
        response = requests.post(f"http://{self.HEALTHCHECK_SERVICE_HOST}:{self.HEALTHCHECK_SERVICE_PORT}/status/register", headers=headers, data=json.dumps(json_payload))
        if response.status_code == 200:
            print(f"Registered {service_name} to Redis with status {service_status}")
        else:
            print(f"Failed to register {service_name} to Redis: HTTP {response.status_code}")

    def register_to_consul(self, service_id, service_name):
        payload = {
            "Name": service_name,
            "Id": service_id,
            "Meta": {
                "service_doc_id": service_id
            },
            "Check": {
                "TlsSkipVerify": True,
                "Method": "GET",
                "Http": f"http://{self.HEALTHCHECK_SERVICE_HOST}:{self.HEALTHCHECK_SERVICE_PORT}/status/{service_id}",
                "Interval": "10s",
                "Timeout": "5s",
                "DeregisterCriticalServiceAfter": "30s"
            }
        }

        try:
            url = f"http://{self.CONSUL_HOST}:{self.CONSUL_PORT}/v1/agent/service/register"
            response = requests.put(url, json=payload)
            print(f"Consul registration ({service_id}): HTTP {response.status_code}")
        except Exception as e:
            print(f"Failed to register to Consul: {e}")

    def register_to_mongo(self, catalog_payload):
        try:
            response = requests.post(f"http://{self.GATEWAY_HOST}:{self.GATEWAY_PORT}/service", json=catalog_payload)
            print(f"Catalog registered to Mongo API: HTTP {response.status_code}")
        except Exception as e:
            print(f"Failed to send catalog payload: {e}")

    def import_apis(self):
        all_endpoints = {}
        providers = self.fetch_providers()
        print(f"Found {len(providers)} providers.")
        for provider in providers:
            try:
                api_data = self.fetch_api_details(provider)
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


