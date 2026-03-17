#!/bin/sh
set -e

echo "Debug: PATH is '$PATH'"

# Verifica presenza comandi critici
for cmd in date basename curl jq awk dd microcks; do
  if ! command -v "$cmd" > /dev/null 2>&1; then
    echo "⚠️  Warning: command '$cmd' not found in PATH"
  else
    echo "✅ Command '$cmd' found at: $(command -v $cmd)"
  fi
done

APIS_DIR="/apis"
SCRIPTS_DIR="/scripts"
MICROCKS_URL="${MICROCKS_URL:-http://mock-server:8080/api}"
TOKEN="${TOKEN:-dummy}"
API_IMPORTER_URL="${API_IMPORTER_URL:-http://api-importer:7500}"

echo "| Starting automatic import of APIs and dispatcher patching..."

for api_file in "$APIS_DIR"/*.yaml; do
  echo "Debug: processing file '$api_file'"

  api_filename=${api_file##*/}
  base_name=${api_filename%.yaml}
  echo "Debug: base_name='$base_name'"

  service_name=$(echo "$base_name" | tr '-' ' ' | awk '{ for(i=1;i<=NF;i++) $i=toupper(substr($i,1,1)) substr($i,2); print }')
  service_name="${service_name} API"
  echo "Debug: service_name='$service_name'"

  groovy_script="${SCRIPTS_DIR}/${base_name}.groovy"
  echo "Debug: groovy_script path='$groovy_script'"

  echo "| Importing API: $api_filename"
  microcks import "${api_file}:true" \
    --microcksURL="${MICROCKS_URL}" \
    --keycloakClientId=foo --keycloakClientSecret=bar
  echo "| Imported $api_filename."

  if [ -f "$groovy_script" ]; then
    echo "| Patching dispatcher for service: $service_name"

    # --- INIZIO LOGICA DI POLLING INTELLIGENTE (FIXED) ---
    MAX_RETRIES=15     # Massimo 15 tentativi
    RETRY_COUNT=0
    operation_id=""
    service_id=""

    # Normalizziamo il base_name togliendo i trattini per la ricerca fuzzy case-insensitive
    SEARCH_TERM=$(echo "$base_name" | tr '-' ' ')

    # Cicla finché non esaurisce i tentativi
    while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
      # 1. Cerca il service_id facendo un match case-insensitive parziale
      service_id=$(curl -s "${MICROCKS_URL}/services" \
        -H "Authorization: Bearer $TOKEN" | \
        /tmp/jq -r --arg term "$SEARCH_TERM" '.[] | select((.name | ascii_downcase) | contains($term | ascii_downcase)) | .id' | head -n 1)

      if [ -n "$service_id" ] && [ "$service_id" != "null" ]; then
        # 2. Cerca l'operation_id in modo flessibile ("POST /register" o simili)
        operation_id=$(curl -s "${MICROCKS_URL}/services/$service_id" \
          -H "Authorization: Bearer $TOKEN" | \
          /tmp/jq -r '.messagesMap | to_entries | .[] | select(.key | startswith("POST /register")) | .value[0].request.id' | head -n 1)

        # 3. Se ha trovato anche l'operazione, esci dal ciclo! (Successo)
        if [ -n "$operation_id" ] && [ "$operation_id" != "null" ]; then
          break
        fi
      fi

      echo "⏳ Waiting for Microcks DB to process API... (Attempt $((RETRY_COUNT+1))/$MAX_RETRIES)"
      sleep 2
      RETRY_COUNT=$((RETRY_COUNT+1))
    done

    # Se dopo tutti i tentativi è ancora vuoto, salta l'API
    if [ -z "$operation_id" ] || [ "$operation_id" = "null" ]; then
      echo "⚠️ Operation ID not found for POST /register after $MAX_RETRIES attempts — skipping."
      continue
    fi
    
    echo "| Service ID: $service_id"
    echo "| Operation ID: $operation_id"
    # --- FINE LOGICA DI POLLING INTELLIGENTE ---

    echo "| Preparing Groovy script payload..."

    # Legge il testo grezzo del file groovy. 
    # 'jq --arg' si occuperà automaticamente di encodare le virgolette e gli a capo nel JSON!
    DISPATCHER_SCRIPT=$(cat "$groovy_script")

    # Costruisci un payload compatibile con Microcks "SCRIPT" dispatcher
    PAYLOAD=$(/tmp/jq -n \
      --arg name "POST /register" \
      --arg method "POST" \
      --arg dispatcher "SCRIPT" \
      --arg dispatcherRules "$DISPATCHER_SCRIPT" \
      --argjson defaultDelay 0 \
      --argjson resourcePaths '["/register"]' \
      --argjson parameterConstraints '[]' \
      '{
        name: $name,
        method: $method,
        dispatcher: $dispatcher,
        dispatcherRules: $dispatcherRules,
        defaultDelay: $defaultDelay,
        resourcePaths: $resourcePaths,
        parameterConstraints: $parameterConstraints
      }')

    curl -s -X PUT "${MICROCKS_URL}/services/${service_id}/operation?operationName=POST%20/register" \
      -H "Authorization: Bearer $TOKEN" \
      -H "Content-Type: application/json" \
      -d "$PAYLOAD"

    echo "✅ Dispatcher configured for $service_name /register"

  else
    echo "⚠️  No script found for $api_filename — skipping dispatcher patch."
  fi

  echo "-----------------------------------"
done

echo "✅ Done: APIs imported and dispatchers patched."