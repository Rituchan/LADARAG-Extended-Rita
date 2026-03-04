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

    echo "Debug: Fetching service ID..."
    service_id=$(curl -s "${MICROCKS_URL}/services" \
      -H "Authorization: Bearer $TOKEN" | \
      /tmp/jq -r --arg name "$service_name" '.[] | select(.name == $name) | .id')
    echo "Debug: service_id='$service_id'"

    if [ -z "$service_id" ] || [ "$service_id" = "null" ]; then
      echo "⚠️  Service ID not found for $service_name — skipping."
      continue
    fi

    echo "| Service ID: $service_id"

    echo "Debug: Fetching operation ID for POST /register..."
    operation_id=$(curl -s "${MICROCKS_URL}/services/$service_id" \
      -H "Authorization: Bearer $TOKEN" | \
      /tmp/jq -r '.messagesMap | ."POST /register"[0].request | .id')
    echo "Debug: operation_id='$operation_id'"

    if [ -z "$operation_id" ] || [ "$operation_id" = "null" ]; then
      echo "⚠️  Operation ID not found for POST /register — skipping."
      continue
    fi

    echo "| Operation ID: $operation_id"

    echo "| Preparing Groovy script payload..."

    DISPATCHER_SCRIPT=$(/tmp/jq -Rs . < "$groovy_script")
    echo "Debug: Dispatcher script loaded, length=$(printf '%s' "$DISPATCHER_SCRIPT" | wc -c)"

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
        dispatcherRules: ($dispatcherRules | fromjson),
        defaultDelay: $defaultDelay,
        resourcePaths: $resourcePaths,
        parameterConstraints: $parameterConstraints
      }')
    echo "Debug: Payload prepared, length=$(printf '%s' "$PAYLOAD" | wc -c)"

    echo "| Sending PUT request..."

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
