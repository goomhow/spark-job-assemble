package com.ctc.stream

import java.util._

import scala.collection.JavaConverters.asScalaIteratorConverter
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.log4j.{Logger, PropertyConfigurator}



class Kafka extends Callback{

  val logger = Logger.getLogger(this.getClass)
  PropertyConfigurator.configure("/home/spark/log4j.properties")

  val currentOffset = new HashMap[TopicPartition,OffsetAndMetadata]()

  def createProducer(): KafkaProducer[String,String] = {
    val prop = new Properties()
    prop.setProperty("bootstrap.servers","133.0.6.87:9092,133.0.6.88:9092,133.0.6.89:9092")
    prop.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
    prop.setProperty("acks","1")
    prop.setProperty("buffer.memory","64000000")
    prop.setProperty("compression.type","snappy")
    prop.setProperty("retries","3")
    prop.setProperty("batch.size","32000000")
    prop.setProperty("linger.ms","100")
    prop.setProperty("client.id","test_for_test_producer")
    prop.setProperty("max.in.flight.requests.per.connection","50")
    prop.setProperty("max.block.ms","100")
    prop.setProperty("max.request.size","1000000")
    prop.setProperty("receive.buffer.bytes","-1")
    prop.setProperty("send.buffer.bytes","-1")


    val producer = new KafkaProducer[String,String](prop)
    producer
  }

  def createConsumer(topics:Array[String] = Array("test")):KafkaConsumer[String,String] = {
    val prop = new Properties()
    prop.setProperty("bootstrap.servers","133.0.6.87:9092,133.0.6.88:9092,133.0.6.89:9092")
    prop.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("group.id","test")

    prop.setProperty("fetch.min.bytes","100")
    prop.setProperty("fetch.max.wait.ms","1000")
    prop.setProperty("max.partition.fetch.bytes","50000000");prop.setProperty("max.message.size","25000000")
    prop.setProperty("session.timeout.ms","3000");prop.setProperty("heartbeat.interval.ms","1000")
    prop.setProperty("auto.offset.reset","latest") //latest,earliest
    prop.setProperty("enable.auto.commit","true");prop.setProperty("auto.commit.interval.ms","5000")
    prop.setProperty("partition.assignment.strategy","org.apache.kafka.clients.consumer.RoundRobinAssignor")//RangeAssignor,RoundRobinAssignor
    prop.setProperty("client.id","testConsumer")
    prop.setProperty("max.poll.records","100000")
    prop.setProperty("receive.buffer.bytes","-1")
    prop.setProperty("send.buffer.bytes","-1")

    val list = new ArrayList[String](topics.length)
    for(t <- topics) {
      list.add(t)
    }

    val consumer = new KafkaConsumer[String,String](prop)
    consumer.subscribe(list,
      new ConsumerRebalanceListener {
        override def onPartitionsRevoked(collection: Collection[TopicPartition]): Unit = {

        }
        override def onPartitionsAssigned(collection: Collection[TopicPartition]): Unit = {
          consumer.commitAsync(currentOffset,null)
        }
    })
    consumer
  }

  def send(data:Array[(String,String)]): Unit = {
    val producer = createProducer()
    val records = data.map(data => new ProducerRecord("test",data._1,data._2))
    for(record <- records)
      producer.send(record,this)
  }

  def receive()= {
    val consumer = createConsumer()
    try{
      while(true){
        currentOffset.clear()
        val records = asScalaIteratorConverter(consumer.poll(1000).iterator()).asScala
        for(record:ConsumerRecord[String,String] <- records){
          val json = s"""{"topic":"${record.topic()}","key":"${record.key()}","value":"${record.value()}"}"""
          logger.debug(json)
          currentOffset.put(new TopicPartition(record.topic(),record.partition()),new OffsetAndMetadata(record.offset()+1,""))
        }
        consumer.commitAsync(currentOffset,null)
      }
    }finally {
      consumer.commitSync()
      consumer.close()
    }
  }

  override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
    if(e!=null){
      e.printStackTrace()
    }
  }
}
