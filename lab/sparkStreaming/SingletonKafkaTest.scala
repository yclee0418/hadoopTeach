package it30days

import kafka.producer.{ KeyedMessage, Producer, ProducerConfig }

//create a singleton class for producer
case class KafkaProducerWrapper(brokerList: String) {
  val producerProps = {
    val prop = new java.util.Properties
    prop.put("metadata.broker.list", brokerList)
    prop.put("serializer.class", "kafka.serializer.StringEncoder")
    prop.put("producer.type", "async")
    prop
  }
  val p = new Producer[String, String](new ProducerConfig(producerProps))
  def send(topic: String, key: String, value: String) {
    p.send(new KeyedMessage(topic, key, value))
  }
  def send(topic: String, value: String) {
    p.send(new KeyedMessage[String, String](topic, value))
  }
  def close() { p.close() }
}

object KafkaProducerWrapper {
  var brokerList: String = ""
  lazy val getInstance = new KafkaProducerWrapper(brokerList)
}

object SingletonKafkaTest {

}