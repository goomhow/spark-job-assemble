package com.ctc.stream

import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


object KafkaStream {

  def send_black_records(id:Long,data:MessageQQ): Unit = {
    println(id)
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("qq_pb")
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val ssc = new StreamingContext(sc, Seconds(1))

    import spark.implicits._


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "133.0.6.87:9092,133.0.6.88:9092,133.0.6.89:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[QQDeserializer],
      "group.id" -> "process",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("qq_pb")

    val stream = KafkaUtils.createDirectStream[String, MessageQQ](
      ssc,
      PreferConsistent,
      Subscribe[String, MessageQQ](topics, kafkaParams)
    ).map(
      record => {
        val msg = record.value()
        (msg.getUserId,msg)
      }
    ).filter(! _._2.isProcessed)

    stream.foreachRDD(
      rdd => {
        spark.createDataFrame(rdd.map(r => {
          val msg = r._2
          msg.setProcessed()
        }),classOf[MessageQQ]).show()
      }
    )

    stream.groupByKeyAndWindow(Minutes(10),Minutes(1)).filter(t => t._2.size > 2).foreachRDD(
      rdd => {
        spark.createDataFrame(rdd.map(_._2.head),classOf[MessageQQ]).show()
      }
    )

    stream.foreachRDD(
      data => {
        val now = Calendar.getInstance()
        val hour = now.get(Calendar.HOUR_OF_DAY)
        val min = now.get(Calendar.MINUTE)
        if(hour <=6 || (hour >= 22 && min>= 30)){
          data.foreachPartitionAsync(
            it => {
              it.foreach(
                e => send_black_records(e._1,e._2)
              )
            }
          )
        }
      }
    )

    ssc.start()
  }

}
