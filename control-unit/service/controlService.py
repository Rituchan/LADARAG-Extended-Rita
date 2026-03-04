from service.discoveryService import Discovery
import json
import requests
import re
import aiohttp
import asyncio
import os

class Controller:

    def __init__(self):
        self.model_name = "phi4-reasoning:14b"
     
    def query_ollama(self, prompt: str) -> str:
        url = os.environ.get("OLLAMA_API_URL", "http://localhost:11434")
        try:
            response = requests.post(
                f"{url}/api/generate",
                json={
                    "model": self.model_name,
                    "prompt": prompt,
                    "options": {
                        "temperature": 0.0,
                        "max_tokens": 4096,
                        "num_ctx": 8192,
                    },
                    
                    "stream": False
                }
            )
            response.raise_for_status()
            data = response.json()
            return data.get("response", "").strip()

        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"[HTTP ERROR] Errore nella richiesta a Ollama: {e}")
        except ValueError:
            raise RuntimeError(f"[PARSE ERROR] Risposta non JSON valida da Ollama: {response.text}")


    def decompose_task(self, discovered_services, discovered_capabilities, discovered_endpoints, query):

        example = {
        "tasks": [
            {
            "task_name": "analyze text",
            "service_id": "svc-001",
            "endpoint": "service endpoint",
            "input": "text to analyze",
            "operation": "POST"
            },
            {
            "task_name": "retrieve report",
            "service_id": "svc-002",
            "endpoint": "service endpoint",
            "input": "",
            "operation": "GET"
            }
          ]
        }

        example_str = json.dumps(example)
        prompt = f"""
            <|system|>
            You have access to a list of services registered in a distributed system, each described by:
            - services
            - capabilities
            - endpoints

            You will receive a query in natural language and must:
            1. Decompose it into atomic tasks.
            2. Associate each task with one or more compatible services based on their capabilities and endpoints.
            3. Return an execution plan.

            REPLY ONLY with a valid JSON, WITHOUT any introductory text or comments.

            Example of the JSON Response (Make sure to fille the fields with data provided by user):
            TEMPLATE:
            {example_str}

            RULES:
            - Use only the data provided. Do not make assumptions or invent services or invent endpoints.
            - Be careful with endpoints names and HTTP operations, they must match date provided in ENDPOINTS section.
            - Endpoints may contain path parameters placeholders in curly brackets
            - You MUST replace these placeholders with actual values extracted from the user query.
            - NEVER return an endpoint containing unresolved placeholders.
            - You have to understand, given the endpoint, if there is a path parameter or a query parameter.
            - Think about the best way to decompose the query and assign tasks to services. 
            <|end|>
            <|user|>
            SERVICES:
            {discovered_services}

            CAPABILITIES:
            {discovered_capabilities}

            ENDPOINTS:
            {discovered_endpoints}

            QUERY:
            {query}
            <|end|>
            <|assistant|>
        """

        response = self.query_ollama(prompt)
        print(f"[LLM RESPONSE] {response}")
        print("="*100)
        return response

    def extract_agents(self, agents_json):
        plan = {}
        json_str = ""
        try:
            think_close_match = re.search(r'</think>', agents_json, flags=re.IGNORECASE)

            if think_close_match:
                after_think = agents_json[think_close_match.end():].strip()
                start = after_think.find('{')
                end = after_think.rfind('}') + 1

                if start != -1 and end != -1 and start < end:
                    json_str = after_think[start:end].strip()
                    plan = json.loads(json_str)
                else:
                    print("[FORMAT ERROR] JSON delimited by { ... } not fount after </think>.")
            else:
                print("[FORMAT ERROR] No tag </think> found.")
        except json.JSONDecodeError as e:
            print(f"[DECODE ERROR] Errors in JSON parsing: {e}\nExtracted content:\n{json_str}")
        return plan



    async def call_agent(self, session, task, discovered_services):
        task_name = task.get("task_name")
        service_id = task.get("service_id")
        endpoint = task.get("endpoint")
        input_data = task.get("input")
        operation = task.get("operation").upper()

        response_result = {}
        response_result['operation'] = operation

        match operation:
            case "POST":
                try:
                    async with session.post(endpoint, json=input_data, timeout=5) as resp:
                        if resp.status == 200 or resp.status == 201 or resp.status == 204:
                            result = await resp.json()
                            print(f"[SUCCESS] Task '{task_name}' completed: {result}")
                            response_result["status"] = "SUCCESS"
                            response_result["status_code"] = resp.status
                            response_result["task_name"] = task_name
                            response_result["result"] = result

                        else:
                            error_text = await resp.text()
                            print(f"[ERROR] Task '{task_name}' failed with status {resp.status}: {error_text}")
                            response_result["status"] = "ERROR"
                            response_result["status_code"] = resp.status
                            response_result["task_name"] = task_name
                            response_result["result"] = error_text
                except Exception as e:
                    print(f"[EXCEPTION] Error in task: '{task_name}' → {e}")
                    response_result["status"] = "EXCEPTION"
                    response_result["status_code"] = 500
                    response_result["task_name"] = task_name
                    response_result["result"] = str(e)
            case "GET":
                try:
                    async with session.get(endpoint, timeout=5) as resp:
                        if resp.status == 200:
                            result = await resp.json()
                            print(f"[SUCCESS] Task '{task_name}' completed: {result}")
                            response_result["status"] = "SUCCESS"
                            response_result["status_code"] = resp.status
                            response_result["task_name"] = task_name
                            response_result["result"] = result
                        else:
                            error_text = await resp.text()
                            print(f"[ERROR] Task '{task_name}' failed with status {resp.status}: {error_text}")
                            response_result["status"] = "ERROR"
                            response_result["status_code"] = resp.status
                            response_result["task_name"] = task_name
                            response_result["result"] = error_text
                except Exception as e:
                    print(f"[EXCEPTION] Error in task: '{task_name}' → {e}")
                    response_result["status"] = "EXCEPTION"
                    response_result["status_code"] = 500
                    response_result["task_name"] = task_name
                    response_result["result"] = str(e)
            case "DELETE":
                try:
                    async with session.delete(endpoint, timeout=5) as resp:
                        if resp.status == 200 or resp.status == 201 or resp.status == 204:
                            result = await resp.text()
                            print(f"[SUCCESS] Task '{task_name}' completed: {result}")
                            response_result["status"] = "SUCCESS"
                            response_result["status_code"] = resp.status
                            response_result["task_name"] = task_name
                            response_result["result"] = result
                        else:
                            error_text = await resp.text()
                            print(f"[ERROR] Task '{task_name}' failed with status {resp.status}: {error_text}")
                            response_result["status"] = "ERROR"
                            response_result["status_code"] = resp.status
                            response_result["task_name"] = task_name
                            response_result["result"] = error_text
                except Exception as e:
                    print(f"[EXCEPTION] Error in task: '{task_name}' → {e}")
                    response_result["status"] = "EXCEPTION"
                    response_result["status_code"] = 500
                    response_result["task_name"] = task_name
                    response_result["result"] = str(e)
            case "PUT":
                try:
                    async with session.put(endpoint, json=input_data, timeout=5) as resp:
                        if resp.status == 200 or resp.status == 201 or resp.status == 204:
                            result = await resp.json()
                            print(f"[SUCCESS] Task '{task_name}' completed: {result}")
                            response_result["status"] = "SUCCESS"
                            response_result["status_code"] = resp.status
                            response_result["task_name"] = task_name
                            response_result["result"] = result
                        else:
                            error_text = await resp.text()
                            print(f"[ERROR] Task '{task_name}' failed with status {resp.status}: {error_text}")
                            response_result["status"] = "ERROR"
                            response_result["status_code"] = resp.status
                            response_result["task_name"] = task_name
                            response_result["result"] = error_text
                except Exception as e:
                    print(f"[EXCEPTION] Error in task: '{task_name}' → {e}")
                    response_result["status"] = "EXCEPTION"
                    response_result["status_code"] = 500
                    response_result["task_name"] = task_name
                    response_result["result"] = str(e)

        return response_result

        
    async def trigger_agents_async(self, agents: dict, discovered_services):
        tasks = agents.get("tasks", [])
        async with aiohttp.ClientSession() as session:
            futures = [asyncio.create_task(self.call_agent(session, task, discovered_services)) for task in tasks]
            results = await asyncio.gather(*futures)
        return results

    def trigger_agents(self, agents: dict, discovered_services):
        results = asyncio.run(self.trigger_agents_async(agents, discovered_services))
        return results


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


    def control(self, query):
        catalog_url = os.environ.get("CATALOG_URL")
        registry = Discovery(os.environ.get("REGISTRY_URL"))
        mock_server_address = os.environ.get("MOCK_SERVER_URL")

        discovered_services = []
        discovered_capabilities = []
        discovered_endpoints = []

        services = registry.services()

        register_key = "POST /register"
        print("DISCOVERED SERVICES:")

        input = {
            "query": query
        }
        service_data = requests.post(f"{catalog_url}/index/search", json=input)
        service_data = service_data.json()
        service_list = service_data["results"]

        if not service_list:
            return {
                "execution_plan": {},
                "execution_results": [],
                "error": "No services matched the query"
            }
        
        registry_service_ids = set(s["id"] for s in services)
        filtered_service_list = [s for s in service_list if s["_id"] in registry_service_ids]
        orphaned_services = [s for s in service_list if s["_id"] not in registry_service_ids]
        if orphaned_services:
            print("[WARNING] Services found via semantic search but are no longer in the registry:")
            for s in orphaned_services:
                print(f"- {s.get('_id')} : {s.get('name')}")

        if not filtered_service_list:
            return {
                "execution_plan": {},
                "execution_results": [],
                "error": "None of the discovered services are currently available in the registry"
            }
        
        for service in filtered_service_list:
            if isinstance(service.get("capabilities"), dict):
                service["capabilities"].pop(register_key, None)

            if isinstance(service.get("endpoints"), dict):
                service["endpoints"].pop(register_key, None)


            print(service)
            print("="*100)

            service_preamble = {
                "_id": service.get("_id"),
                "name": service.get("name"),
                "description": service.get("description"),
            }
            discovered_services.append(service_preamble)
            discovered_capabilities.append(service.get("capabilities", {}))
            discovered_endpoints.append(service.get("endpoints", {}))
        
        discovered_endpoints = self.replace_endpoints(discovered_endpoints, mock_server_address)
        plan_json = self.decompose_task(discovered_services, discovered_capabilities, discovered_endpoints, query)
        plan = self.extract_agents(plan_json)

        results = self.trigger_agents(plan, discovered_services)
        return {
            "execution_plan": plan,
            "execution_results": results
        }
