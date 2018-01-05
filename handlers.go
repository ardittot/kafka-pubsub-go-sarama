package main

import (
    "github.com/gin-gonic/gin"
    "net/http"
    //"strconv"
    //"fmt"
)

func AddConsumerTopic(c *gin.Context) {
    var param ConsumerParam
    if err := c.ShouldBindJSON(&param); err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
    }
    err := receiveMsg(param) // Add new Kafka topic 
    if err==nil {
        c.JSON(http.StatusOK, gin.H{"status": http.StatusOK})
    } else {
        c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
    }
}

func RunProduce(c *gin.Context) {
    var data    interface{}
    topic := c.Param("topic")
    if err := c.ShouldBindJSON(&data); err == nil {
	sendMsg(topic,data) // Produce data to Kafka topic
        c.JSON(http.StatusOK, gin.H{"status": http.StatusOK})
    } else {
        c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
    }
}

