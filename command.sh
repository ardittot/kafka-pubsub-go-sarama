# Install all required GO packages
go get -u github.com/gin-gonic/gin
go get -u github.com/Shopify/sarama
go get -u gopkg.in/resty.v1

# Compile & run
#go run main.go
go build
./kafka-pubsub

# Preparation
##Edit kafka-sarama.go
##(1) Variable brokers to point to desired Kafka broker clusters
##(2) Function useConsumer():  to customize your desired commands for consumed data from Kafka

# Add new topic for kafka consumer to be running forever as a goroutine
curl -X POST -H "Accept: application/json" -H "Content-Type: application/json" -d '{"topic":"test2","group":"test2-group1"}' http://localhost:8010/subscribe/add
# Publish data to Kafka
curl -X POST -H "Accept: application/json" -H "Content-Type: application/json" -d @./json/example.json http://localhost:8010/publish/<topic-name>
