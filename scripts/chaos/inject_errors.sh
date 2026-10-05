SERVICE=$1
RATE=$2
curl -s -X POST "http://localhost:8000/chaos" \
  -H "Content-Type: application/json" \
  -d "{\"error_rate\": ${RATE}}"
echo "injected error_rate=${RATE} into ${SERVICE}"