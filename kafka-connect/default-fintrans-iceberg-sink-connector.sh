curl --noproxy '*' -X DELETE http://localhost:8083/connectors/iceberg-fintrans-default

curl --noproxy '*' -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @default-fintrans-iceberg-sink-connector.json
