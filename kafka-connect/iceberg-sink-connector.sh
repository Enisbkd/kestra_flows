curl --noproxy '*' -X DELETE http://localhost:8083/connectors/iceberg-fintrans

curl --noproxy '*' -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @iceberg-sink-connector.json
