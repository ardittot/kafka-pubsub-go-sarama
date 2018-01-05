package main

func initializeRoutes() {

  // Handle the index route
  router.POST("/subscribe/add", AddConsumerTopic)
  router.POST("/publish/:topic", RunProduce)
}
