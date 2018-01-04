package main

import (
    "encoding/json"
    "fmt"
    "github.com/Shopify/sarama"
    "os"
    //"math/rand"
)

var (
    brokers = []string{"10.148.0.4:9092"}
    //topic   = "test2"
    //topics  = []string{topic}
)

var producer sarama.SyncProducer
var kafka sarama.Consumer

func newKafkaConfiguration() *sarama.Config {
    conf := sarama.NewConfig()
    conf.Producer.RequiredAcks = sarama.WaitForAll
    conf.Producer.Return.Successes = true
    conf.ChannelBufferSize = 1
    conf.Version = sarama.V0_10_1_0
    return conf
}

func newKafkaSyncProducer() sarama.SyncProducer {
    kafka, err := sarama.NewSyncProducer(brokers, newKafkaConfiguration())

    if err != nil {
        fmt.Printf("Kafka error: %s\n", err)
        os.Exit(-1)
    }

    return kafka
}

func newKafkaConsumer() sarama.Consumer {
	consumer, err := sarama.NewConsumer(brokers, newKafkaConfiguration())

	if err != nil {
		fmt.Printf("Kafka error: %s\n", err)
		os.Exit(-1)
	}

	return consumer
}

func sendMsg(topic string, event interface{}) error {
	json, err := json.Marshal(event)

	if err != nil {
		return err
	}

	msgLog := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(string(json)),
	}

	partition, offset, err := producer.SendMessage(msgLog)
	if err != nil {
		fmt.Printf("Kafka error: %s\n", err)
	}

	//fmt.Printf("Message: %+v\n", event)
	fmt.Printf("Message is stored in partition %d, offset %d\n", partition, offset)

	return nil
}

func receiveMsg(topic string) {
	var msgVal []byte
	var data interface{}
	//topics := []string{topic}
	partitions,err := kafka.Partitions(topic)
	if err!=nil {
		fmt.Printf("Kafka Partitions not detected")
	}
	for {
		//for _, part := range partitions {
		//part := partitions[rand.Intn(len(partitions))]
		part := int32(0)
		consumer, err := kafka.ConsumePartition(topic, part, sarama.OffsetOldest)
		if err != nil {
			fmt.Printf("Kafka error: %s\n", err)
			//os.Exit(-1)
		}
		select {
		case err := <-consumer.Errors():
			fmt.Printf("Kafka error: %s\n", err)
		case msg := <-consumer.Messages():
			msgVal = msg.Value
			json.Unmarshal(msgVal, &data)
			fmt.Printf("Message:\n%+v\n", data)
		}
		//}
	}
}
