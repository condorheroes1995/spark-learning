package ss.streaming

object KafkaRedisProperties {
  val KAFKA_SERVER: String = "hellowin-1"
  val KAFKA_ADDR: String = KAFKA_SERVER + ":9092"
  val KAFKA_USER_TOPIC: String = "user_events"
}