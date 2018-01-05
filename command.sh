go get -u github.com/gin-gonic/gin
go get -u github.com/Shopify/sarama
go run main.go
go build
./kafka-pubsub

# Add new topic for kafka consumer to be running forever as a goroutine
curl -X POST -H "Accept: application/json" -H "Content-Type: application/json" -d '{"topic":"test2","group":"test2-group1"}' http://localhost:8010/subscribe/add
# Publish data to Kafka
curl -X POST -H "Accept: application/json" -H "Content-Type: application/json" -d @./json/example.json http://localhost:8010/publish/<topic-name>
