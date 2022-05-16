package dev.paulmatthews.splitters

import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import java.time.Duration
import java.util
import java.util.Properties

object TransactionToWithdrawalProducer {

  case class Transaction(name: String, email: String, transactionType: String, amount: Double)

  def getConsumerProperties(): Properties = {
    val props: Properties = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "withdrawal-consumer")
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props
  }

  def getProducerProperties(): Properties = {
    val props: Properties = new Properties()
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.setProperty(ProducerConfig.ACKS_CONFIG, "1")
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props
  }

  def main(args: Array[String]): Unit = {
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](getConsumerProperties())
    consumer.subscribe(util.Arrays.asList("transactions"))

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](getProducerProperties())

    val depositProducer: KafkaProducer[String, String] = new KafkaProducer[String, String](getProducerProperties())

    while(true) {
      val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(5000))
      records.forEach((record) => {
        val stringTransaction = record.value()
        val listTransaction = stringTransaction.split(",")
        val transaction: Transaction = Transaction(listTransaction(0), listTransaction(1),
          listTransaction(2), listTransaction(3).toDouble)
        if (transaction.transactionType == "withdrawl") {
//          println(transaction)
          val producerRecord: ProducerRecord[String, String] = new ProducerRecord[String, String]("withdrawals",
            f"${transaction.name},${transaction.email},${transaction.transactionType},${transaction.amount}")
          producer.send(producerRecord)
        }
        else {
          val producerRecord: ProducerRecord[String, String] = new ProducerRecord[String, String]("deposits",
            f"${transaction.name},${transaction.email},${transaction.transactionType},${transaction.amount}")
          depositProducer.send(producerRecord)
        }
      })
    }
  }
}
