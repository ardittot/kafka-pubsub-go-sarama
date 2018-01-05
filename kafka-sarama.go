package main

import (
    "encoding/json"
    "fmt"
    "github.com/Shopify/sarama"
    "os"
    "os/signal"
    //"sync"
)

var (
    brokers = []string{"10.148.0.4:9092"}
)

type topicType[]string
type ConsumerParam struct {
    Topic string `json:topic`
    Group string `json:group`
}

var producer sarama.SyncProducer
var kafka sarama.Consumer
var topicList topicType

func (l *topicType) removeElement(item string) {
    l1 := *l
    for i, other := range *l {
        if other == item {
            l1 = append(l1[:i], l1[i+1:]...)
        }
    }
    *l = l1
}

func(l *topicType) addElement(item string) {
    l1 := *l
    l1.removeElement(item)
    l1 = append(l1, item)
    *l = l1
}

func useConsumer(msg *sarama.ConsumerMessage) {
	var data interface{}
	msgVal := msg.Value
	json.Unmarshal(msgVal, &data)
	fmt.Printf("Message:\n%+v\n", data)
}

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

func receiveMsg(param ConsumerParam) error {
	topic := param.Topic

	partitionList,err := kafka.Partitions(topic)
	if err!=nil {
		fmt.Printf("Kafka Partitions not detected")
		return err
	}
	topicList.addElement(topic)
	fmt.Printf("Add New Topic %v",topicList)

	var (
		messages = make(chan *sarama.ConsumerMessage, 256)
		closing  = make(chan struct{})
		//wg       sync.WaitGroup
	)

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		//logger.Println("Initiating shutdown of consumer...")
		fmt.Println("Initiating shutdown of consumer...")
		close(closing)
	}()

	for _, partition := range partitionList {

		fmt.Printf("%d\n",partition)
		consumer, err := kafka.ConsumePartition(topic, partition, sarama.OffsetOldest)
		if err != nil {
			//topicList.removeElement(topic)
			fmt.Printf("Kafka error: %s\nLeft topics: %s\n", err,topicList)
			close(closing)
			return err
			//os.Exit(-1)
		}

		go func(consumer sarama.PartitionConsumer) {
			<-closing
			consumer.AsyncClose()
		}(consumer)

		//wg.Add(1)
		go func(consumer sarama.PartitionConsumer) {
			//defer wg.Done()
			for message := range consumer.Messages() {
				messages <- message
			}
		}(consumer)
	}

	go func() {
		for msg := range messages {
			/*
 			fmt.Printf("Partition:\t%d\n", msg.Partition)
 			fmt.Printf("Offset:\t%d\n", msg.Offset)
 			fmt.Printf("Key:\t%s\n", string(msg.Key))
 			fmt.Printf("Value:\t%s\n", string(msg.Value))
 			fmt.Println()
			*/
			go useConsumer(msg)
		}
	}()
	/*
	wg.Wait()
	logger.Println("Done consuming topic", topic)
	fmt.Println("Done consuming topic", topic)
	close(messages)

	if err := kafka.Close(); err != nil {
		//logger.Println("Failed to close consumer: ", err)
		fmt.Println("Failed to close consumer: ", err)
	}
	*/
	return err
}

