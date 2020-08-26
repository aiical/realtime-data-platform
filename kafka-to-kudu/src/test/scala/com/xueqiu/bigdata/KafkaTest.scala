package com.xueqiu.bigdata

import java.util.Properties

import org.junit.{Before, Test}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import scala.collection.JavaConverters._


class KafkaTest {
  val prop = new Properties()
  lazy val consumer = new KafkaConsumer[String,String](prop)

  @Before
  def initProperties(): Unit ={
    prop.put("bootstrap.servers", "k8s-master01:32092")
    prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  }

  @Test
  def testConsumeAssignPartition(): Unit ={
    prop.put("group.id", System.currentTimeMillis().toString)
    prop.put("auto.offset.reset", "earliest")

    val partition = new TopicPartition("maxwell", 2)
    consumer.assign(List(partition).asJava)

    while (true) {
      val records = consumer.poll(100);
      val it = records.iterator()
      while (it.hasNext){
        val record = it.next()
        println(s"${record.key()} -> ${record.value()}")
      }
    }
  }
}
