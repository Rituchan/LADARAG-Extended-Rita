from service import importService
from flask import request, jsonify
from flask_restx import Namespace, Resource, reqparse
from service import importService
import json

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
            api_data = import_service.fetch_api_details(key)
            swagger_url = import_service.extract_swagger_url(api_data, key)
            openapi = import_service.parse_swagger(key, swagger_url)
            import_service.register_to_redis(key, "true")
            import_service.register_to_consul(key, openapi["name"])
            import_service.register_to_mongo(openapi)
        except Exception as e:
            return {"error": str(e)}, 500
