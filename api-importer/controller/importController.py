from service import importService
from flask import request, jsonify
from flask_restx import Namespace, Resource, reqparse
import json
import os

api = Namespace("importer", description="External API import and management")
import_parser = reqparse.RequestParser()


@api.route("/import")
class ImportAPI(Resource):
    def post(self):
        import_service = importService.Service()
        try:
            import_service.import_apis()
        except Exception as e:
            return {"error": str(e)}, 500


@api.route("/import/<key>")
class ImportSpecificAPI(Resource):
    def post(self, key):
        import_service = importService.Service()
        try:
            providers = import_service.fetch_providers()
            if key not in providers:
                return {"message": f"No provider found for key: {key}"}, 404
            api_data    = import_service.fetch_api_details(key)
            swagger_url = import_service.extract_swagger_url(api_data, key)
            openapi     = import_service.parse_swagger(key, swagger_url)
            import_service.register_to_redis(key, "true")
            import_service.register_to_consul(key, openapi["name"])
            import_service.register_to_mongo(openapi)
        except Exception as e:
            return {"error": str(e)}, 500


@api.route("/enrich")
class EnrichSchemas(Resource):
    def post(self):
        """
        Scarica gli YAML da Microcks tramite prance, estrae response_schemas
        per ogni servizio registrato in MongoDB e aggiorna i documenti
        tramite PATCH /services/<id>/schemas sul catalog-gateway.

        Body JSON opzionale:
          { "mock_server_url": "http://mock-server:8080" }
          { "service_id": "smart-charging-stations-mock" }

        Risposta:
          { "enriched": 5, "skipped": 1, "errors": 0 }
        """
        data            = request.get_json(silent=True) or {}
        mock_server_url = data.get("mock_server_url") or os.environ.get("MOCK_SERVER_URL")
        service_id      = data.get("service_id")

        import_service = importService.Service()
        try:
            result = import_service.enrich_schemas(
                mock_server_url=mock_server_url,
                service_id=service_id
            )
            return result, 200
        except Exception as e:
            return {"error": str(e)}, 500