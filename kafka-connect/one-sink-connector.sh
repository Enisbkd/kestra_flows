curl --noproxy '*' -X DELETE http://localhost:8083/connectors/fintrans-connector

curl --noproxy '*' -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @one-sink-connector.json
