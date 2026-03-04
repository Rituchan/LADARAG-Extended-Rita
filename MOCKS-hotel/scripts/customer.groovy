import groovy.json.JsonOutput
import org.yaml.snakeyaml.Yaml
import java.net.URL
import java.net.HttpURLConnection

def SERVICE_NAME = "customer-api-mock"
def SERVICE_ID = "customer-mock"
def MICROCKS_INTERNAL_PORT = 8080
def MICROCKS_HOST = "mock-server"
def CONSUL_INTERNAL_PORT = 8500
def CONSUL_HOST = "registry"
def GATEWAY_HOST = "catalog-gateway"
def GATEWAY_PORT = 5000
def API = "Customer+API"
def OPENAPI_FILE = "Customer%20API-1.0.yaml"
def API_VERSION = "1.0"

def openApiUrl = "http://${MICROCKS_HOST}:${MICROCKS_INTERNAL_PORT}/api/resources/${OPENAPI_FILE}"
def yamlContent = new URL(openApiUrl).text

def yaml = new Yaml()
def swaggerMap = yaml.load(yamlContent)

def description = swaggerMap.info?.description ?: "No description"
def paths = swaggerMap.paths ?: [:]
def host = swaggerMap.servers?.getAt(0)?.url ?: "http://${MICROCKS_HOST}:${MICROCKS_INTERNAL_PORT}"

def url = new URL(host)
def hostUrl = url.host + (url.port != -1 ? ":${url.port}" : "")
def base = url.path

def capabilities = [:]
def endpoints = [:]

paths.each { path, methods ->
    methods.each { method, details ->
        def endpointKey = "${method.toUpperCase()} ${path}"
        def desc = details.description ?: details.summary ?: details.operationId ?: method
        capabilities[endpointKey] = desc
        endpoints[endpointKey] = "http://${hostUrl}${base}${path}"
    }
}

def catalog_payload = [
    id          : SERVICE_ID,
    name        : SERVICE_NAME,
    description : description,
    capabilities: capabilities,
    endpoints   : endpoints
]

def payload = [
    Name: SERVICE_NAME,
    Id  : SERVICE_ID,
    Meta: [
        service_doc_id: SERVICE_ID
    ],
    Check: [
        TlsSkipVerify: true,
        Method       : "GET",
        Http         : "http://${MICROCKS_HOST}:${MICROCKS_INTERNAL_PORT}/rest/${API}/${API_VERSION}/health",
        Interval     : "10s",
        Timeout      : "5s",
        DeregisterCriticalServiceAfter: "30s"
    ]
]

def jsonPayload = JsonOutput.prettyPrint(JsonOutput.toJson(payload))
def gatewayPayload = JsonOutput.prettyPrint(JsonOutput.toJson(catalog_payload))

def responseMap = [:]

try {
    def gatewayUrl = new URL("http://${GATEWAY_HOST}:${GATEWAY_PORT}/service")
    def gatewayConn = gatewayUrl.openConnection() as HttpURLConnection
    gatewayConn.setRequestMethod("POST")
    gatewayConn.setDoOutput(true)
    gatewayConn.setRequestProperty("Content-Type", "application/json")
    gatewayConn.outputStream.withWriter("UTF-8") { it << gatewayPayload }

    def gatewayCode = gatewayConn.responseCode
    println "Catalog registered to gateway with HTTP ${gatewayCode}"

    if (gatewayCode >= 400) {
        throw new RuntimeException("Gateway returned HTTP ${gatewayCode}")
    }

} catch (Exception e) {
    println "Failed to register catalog to gateway: ${e.message}"
    responseMap.status = "error"
    responseMap.gatewayError = e.message
    return responseMap
}

try {
    def urlConsul = new URL("http://${CONSUL_HOST}:${CONSUL_INTERNAL_PORT}/v1/agent/service/register")
    def conn = urlConsul.openConnection() as HttpURLConnection
    conn.setRequestMethod("PUT")
    conn.setDoOutput(true)
    conn.setRequestProperty("Content-Type", "application/json")
    conn.outputStream.withWriter("UTF-8") { it << jsonPayload }

    def code = conn.responseCode
    println "Service registered to Consul with HTTP ${code}"

    responseMap.status = "success"
    responseMap.httpCode = code
    responseMap.message = "Service registered successfully"
} catch (Exception e) {
    println "Failed to register to Consul: ${e.message}"
    responseMap.status = "error"
    responseMap.httpCode = 500
    responseMap.message = "Failed to register to Consul"
    responseMap.error = e.message
}

return responseMap
