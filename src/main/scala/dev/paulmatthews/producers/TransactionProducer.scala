package dev.paulmatthews.producers

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties
import scala.io.Source
import scala.util.Random

object TransactionProducer {

  def getTransactionStringFromAPI(amount: Int): Array[String] = {
    val url = s"https://explore.paulmatthews.dev/api/random/transactions?data_format=csv&amount=$amount"
    val bufferedSource = Source.fromURL(url)
    val stringResponse = bufferedSource.mkString
    bufferedSource.close
    stringResponse.split("\n").drop(1)
  }

  def getProperties: Properties = {
    val properties = new Properties
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "1")
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    properties
  }

  def main(args: Array[String]): Unit = {
    // create & configure Producer
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](getProperties)
    // infinite loop:
    while (true) {
      // make HTTP request for Transaction data
      val random = Random
      try {
        val transactionArray = getTransactionStringFromAPI(random.nextInt(10))
        // https://explore.paulmatthews.dev/api/random/transactions?amount=(random 1..15)
        // send transaction string CSV to Transaction Topic
        transactionArray.foreach(line => {
          val record: ProducerRecord[String, String] = new ProducerRecord[String, String]("transactions", line)
          producer.send(record)
        })
        // sleep (random 10seconds..45seconds)
        Thread.sleep(random.nextInt(45) * 1000)
      }
      catch  {
        case ioe: java.io.IOException => println("500 error...moving on")
      }

    }
    producer.close()

  }
}
