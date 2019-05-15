package com.ctc

import com.ctc.FxTrainData._
import com.ctc.util.INSTANTCE.{AREA_LATN,CTC_NBR}
import com.ctc.util.DateUtil.{get_prev_seven_day,next_day,today}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{count, countDistinct, lit, sum,udf,when}
import org.apache.spark.sql.types._

class FxTrainData(cityCode :String,day:String,nbrs:DataFrame=null) extends Serializable {
  val days = get_prev_seven_day(day).toArray
  val (hbHcode,cityHcode) = {
    val cityCodes = AREA_LATN.map(_._3.toInt).mkString(",")
    val areaHcode = spark.read.parquet("info/hcode").where(s"city_code in ($cityCodes)")
    (areaHcode.select("PREFIX"),
      areaHcode.where(s"city_code=${cityCode.toInt}").select("PREFIX").withColumn("is_area",lit(1)))
  }
  def genFeatures(): DataFrame = {
    val dayDF = day7()
    val monDF = month()
    val df = dayDF.join(monDF,Seq("call_type","self_number"),"outer").na.fill(0)
    val zj = df.where(df("call_type")===1).drop("call_type")
    val bj = df.where(df("call_type")===0).drop("call_type")
    val z = zj.toDF("self_number"+:zj.columns.drop(1).map(c => c+"_zj").toList:_*)
    val b = bj.toDF("self_number"+:bj.columns.drop(1).map(c => c+"_bj").toList:_*)
    val data = z.join(b,Seq("self_number"),"outer").na.fill(0).withColumn("day_id",lit(day))
    data.write.mode("append").parquet("fx/data/train")
    data
  }
  def is_dx_nbr = udf((nbr:String) => nbr.matches(CTC_NBR))

  def day7(): DataFrame ={

    val months = days.map(_.take(6)).toSet.toArray

    val df = months match {
      case Array(m) => {
        spark.read.schema(EP_SCHEMA).parquet(dataDir.format(m))
      }
      case Array(m1,m2) => {
        spark.read.schema(EP_SCHEMA).parquet(dataDir.format(m1))
          .unionAll(spark.read.schema(EP_SCHEMA).parquet(dataDir.format(m2)))
      }
    }

    val data = df.repartition(100).where(
      df("START_TIME").substr(0,8).between(days(6),days(0)) &&
        is_dx_nbr(df("OPP_NUMBER")) &&
        is_dx_nbr(df("SELF_NUMBER"))
    )
      .join(hbHcode,hbHcode("prefix") === df("OPP_NUMBER").substr(0,7),"inner")
      .join(cityHcode,Seq("prefix"),"left")
      .na
      .fill(0,Seq("is_area"))
      .withColumn("call_type",when(df("RECORD_TYPE")===1,0).otherwise(1))
      .withColumn("day_id",df("START_TIME").substr(0,8))
      .drop("prefix")
      .drop("RECORD_TYPE")
      .drop("START_TIME")

    data.cache()
    val hb = day_feature(data,"hb_")
    val area = day_feature(data.where("is_area=1"),"area_")
    data.unpersist()
    hb.join(area,Seq("call_type","self_number"),"left").na.fill(0)
  }

  def month(): DataFrame = {
    val end = next_day(day,-8)
    val start = next_day(day,-38)
    val df = if(end.take(6) == start.take(6)){
      spark.read.schema(EP_SCHEMA).parquet(dataDir.format(end.take(6)))
    }else{
      spark.read.schema(EP_SCHEMA).parquet(dataDir.format(end.take(6)))
        .unionAll(spark.read.schema(EP_SCHEMA).parquet(dataDir.format(start.take(6))))
    }

    val data = df.where(df("START_TIME").substr(0,6).between(start,end) &&
      is_dx_nbr(df("OPP_NUMBER")) &&
      is_dx_nbr(df("SELF_NUMBER")))
      .join(hbHcode,hbHcode("prefix") === df("OPP_NUMBER").substr(0,7),"inner")
      .join(cityHcode,Seq("prefix"),"left")
      .na
      .fill(0,Seq("is_area"))
      .withColumn("call_type",when(df("RECORD_TYPE")===1,0).otherwise(1))
      .drop("prefix")
      .drop("RECORD_TYPE")
      .drop("START_TIME")

    data.cache()
    val hb = month_feature(data,"hb_")
    val area = month_feature(data.where("is_area=1"),"area_")
    data.unpersist()
    hb.join(area,Seq("call_type","self_number"),"left").na.fill(0)
  }

  def day_feature(data:DataFrame,col_prefix:String=""): DataFrame = {
    val df = data.groupBy("call_type","self_number").pivot("day_id")
      .agg(
        countDistinct("opp_number").as(col_prefix+"nbr_cnt_d"),
        sum("call_duration").as(col_prefix+"duration_d"),
        count("opp_number").as(col_prefix+"call_cnt_d")
      ).na.fill(0)
    val cols = df.columns.map(col => {
      if(col.equalsIgnoreCase("call_type")||col.equalsIgnoreCase("self_number")){
        col
      }else{
        val Array(day,suffix) = col.split("_",2)
        days.indexOf(day) + "_" + suffix
      }
    })
    df.toDF(cols:_*)
  }

  def month_feature(df:DataFrame,col_prefix:String=""): DataFrame ={
    df.groupBy("call_type","self_number").agg(
      countDistinct("opp_number").as(col_prefix+"nbr_cnt_m"),
      sum("call_duration").as(col_prefix+"duration_m"),
      count("opp_number").as(col_prefix+"call_cnt_m")
    )
  }
}


object FxTrainData {
  val sparkConf = new SparkConf()
    .setAppName("fx")
    .set("spark.dynamicAllocation.enabled","true")
    .set("spark.default.parallelism","300")
    .setJars(
      Seq("ojdbc7.jar",
        "spark-examples-1.6.3-hadoop2.6.0.jar",
        "CustomerRelationship-2.0.0.jar").map("/var/lib/spark/"+_)
    )
  val sc = SparkContext.getOrCreate(sparkConf)
  val spark = new HiveContext(sc)
  val dataDir = "billing/ep/cdr_cdma_nic_%s"
  /**
    *
    *                 回省前每7天    回省前8-37合计
    *                     主叫          被叫
    *                   湖北               地市
    *
    * index_nbr     |   号码数    时长      次数
    * 电信非湖北号码  |
    *
    * */
  val EP_SCHEMA = StructType(
    Seq(
      StructField("RECORD_TYPE",StringType,true),
      StructField("SELF_NUMBER",StringType,true),
      StructField("OPP_NUMBER",StringType,true),
      StructField("START_TIME",StringType,true),
      StructField("CALL_DURATION",StringType,true)
    )
  )


  def main(args: Array[String]): Unit = {
    val day = if(args.length==1) args(0) else next_day(today(),-1)
    val fx = new FxTrainData("0718",day)
    val x = fx.genFeatures()
    x.show()
  }
}
