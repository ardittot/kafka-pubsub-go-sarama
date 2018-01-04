package main

import (
	"github.com/gin-gonic/gin"
	//"fmt"
)

var router *gin.Engine

func main() {

  // Set the router as the default one provided by Gin
  router = gin.Default()

  // Initialize kafka
  producer = newKafkaSyncProducer()
  kafka = newKafkaConsumer()

  // Run kafka consumer asynchronously
  go receiveMsg("test2")

  // Initialize the routes
  initializeRoutes()

  // Start serving the application
  router.Run("0.0.0.0:8010")

}
