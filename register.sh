#!/bin/sh
set -e

echo "| Starting POST /register invocations..."

APIS_DIR="/apis"
MOCK_BASE_URL="http://mock-server:8080/rest"
VERSION="1.0"

capitalize() {
  word="$1"
  first_char=$(printf '%s' "$word" | dd bs=1 count=1 2>/dev/null)
  rest_chars=${word#?}
  first_upper=$(printf '%s' "$first_char" | awk '{ print toupper($0) }')
  echo "${first_upper}${rest_chars}"
}

for api_file in "$APIS_DIR"/*.yaml; do
  echo "Debug: Processing file '$api_file'"

  api_filename=$(basename "$api_file")
  base_name="${api_filename%.yaml}"

  IFS='-' read -ra parts <<< "$base_name"
  capitalized_parts=()
  for part in "${parts[@]}"; do
    capitalized_parts+=("$(capitalize "$part")")
  done

  joined_name=$(IFS='+'; echo "${capitalized_parts[*]}")
  service_name="${joined_name} API"

  echo "Debug: service_name='$service_name'"

  service_url_part=$(echo "$service_name" | sed 's/ /+/g')
  register_url="${MOCK_BASE_URL}/${service_url_part}/${VERSION}/register"

  echo "Debug: register_url='$register_url'"

  response=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$register_url")

  if [ "$response" -eq 200 ]; then
    echo "✅ POST /register successful for $service_name"
  else
    echo "❌ POST /register failed for $service_name — HTTP $response"
  fi

  echo "-----------------------------------"
  sleep 0.5
done

echo "✅ Done: All /register endpoints invoked."
