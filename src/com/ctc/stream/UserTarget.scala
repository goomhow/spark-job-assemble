package com.ctc.stream


import org.apache.spark.sql.functions.{col, from_json, window}
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.SparkSession
import com.ctc.util.AppUtils.createDefaultSession

class UserTarget(@transient spark:SparkSession) {
  import spark.implicits._

  def windowJob(topic:String,schema:StructType): Unit ={
    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "133.0.6.88:9092")
      .option("subscribe",topic)
      .option("offset","earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]


    val wx = lines.select("value").select(from_json(col("value"),schema).as("value")).select(
      schema.map(f => col("value").getField(f.name).as(f.name)):_*
    ).groupBy(window($"CreateTime","10 minutes","10 minutes"),$"FromUserName")
      .count()

    // Start running the query that prints the running counts to the console
    val query = wx.writeStream.outputMode(OutputMode.Complete()).option("checkpointLocation","checkpoint/window")
      .format("console")
      .start()

    query.awaitTermination()
  }

  def sinkJob(topic:String,schema:StructType,sinkPath:String): Unit ={
    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "133.0.6.88:9092")
      .option("subscribe",topic)
      .option("offset","earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]


    val wx = lines.select("value").select(from_json(col("value"),schema).as("value")).select(
      schema.map(f => col("value").getField(f.name).as(f.name)):_*
    )

    // Start running the query that prints the running counts to the console
    val query = wx.writeStream.outputMode(OutputMode.Append()).option("checkpointLocation","checkpoint/sink")
      .format("parquet")
      .option("path",sinkPath)
      .start()

    query.awaitTermination()
  }

  def consoleJob(topic:String,schema:StructType): Unit = {
    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "133.0.6.88:9092")
      .option("subscribe",topic)
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]


    val wx = lines.select("value").select(from_json(col("value"),schema).as("value")).select(
      schema.map(f => col("value").getField(f.name).as(f.name)):_*
    )

    // Start running the query that prints the running counts to the console
    val query = wx.writeStream.outputMode(OutputMode.Append()).option("checkpointLocation","tmp/checkpoint/console")
      .format("console")
      .start()

    query.awaitTermination()
  }

}


object UserTarget{

  val wx_topic = "weixin"
  """
    {'CreateTime': 1552874541,
     'FromUserName': '@b858d7977f07a8e59c0dfe45d68ccc710962533cefd8b47081a9f765f4e89378',
     'IsAdmin': None,
     'IsAt': False,
     'MemberCount': 4,
     'MsgId': '5234825756251786863',
     'MsgType': 1,
     'ToUserName': '@@7fec2994a55f9daf6da25ef2e40cd32adc5e1b9aebe8447b103d84def04d34c8',
     'groupname': '测试-',
     'keys': ['下次', '一定', '及时'],
     'mtype': 'G',
     'sent': 0.5689851767388825,
     'source': '下次一定及时'}
  """.stripMargin

  val wx2_schema = StructType(Array(
    StructField("MsgId",StringType),
    StructField("FromUserName",StringType),
    StructField("ToUserName",StringType),
    StructField("CreateTime",TimestampType),
    StructField("IsAdmin",BooleanType),
    StructField("IsAt",BooleanType),
    StructField("MsgType",IntegerType),
    StructField("source",StringType),
    StructField("keys",ArrayType(StringType)),
    StructField("sent",FloatType),
    StructField("mtype",StringType),
    StructField("groupname",StringType),
    StructField("MemberCount",IntegerType)

  ))


  val wx_schema = StructType(Array(
    StructField("MsgId",StringType),
    StructField("FromUserName",StringType),
    StructField("ToUserName",StringType),
    StructField("Content",StringType),
    StructField("CreateTime",TimestampType),
    StructField("IsAdmin",BooleanType),
    StructField("IsAt",BooleanType),
    StructField("NickName",StringType)
  ))

  val qq_topic = "qqzx"
  val qq_schema = StructType(Array(
    StructField("message_id",IntegerType),
    StructField("self_id",LongType),
    StructField("user_id",LongType),
    StructField("message",StringType),
    StructField("message_type",StringType),
    StructField("time",TimestampType)
  ))


  def apply(spark: SparkSession): UserTarget = new UserTarget(spark)


  def main(args: Array[String]): Unit = {
    val spark = createDefaultSession("UserTarget")
    val target = UserTarget(spark)
    target.sinkJob(wx_topic,wx2_schema,"wx_data")
  }
}