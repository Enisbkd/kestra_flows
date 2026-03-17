curl --noproxy '*' -X DELETE http://localhost:8083/connectors/iceberg-resvnames

curl --noproxy '*' -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @resvnames-iceberg-sink-connector.json
