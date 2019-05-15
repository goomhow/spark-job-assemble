package com.ctc.kaoqin

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import com.ctc.util.INSTANTCE._
import com.ctc.util.HDFSUtil.exists
class GenPosition(@transient spark:SparkSession) extends Serializable{
  val p = spark.read.parquet("position/tm_lte_cell").unionByName(spark.read.parquet("position/tm_voi_cell").drop("AREA_NAME"))
  val kaoqin = spark.read.parquet("kaoqin").toDF("NAME","BILLING_NBR")
  kaoqin.cache()
  p.cache()

  def backUpWhUrMobVoice(month:String): Unit = {
    val path = s"ur_mob_voice/wh/${month}"
    val table = s"ls6_acct_wh.ur_mob_voice_${month}_t"
    spark.read.jdbc(anti_url,table,"partition_id_day",1,32,31,properties).write.mode("overwrite").parquet(path)
  }

  def getWhUrMobVoiceDay(day:String): DataFrame = {
    val month = day.dropRight(2)
    val date = day.takeRight(2).toInt
    val path = s"ur_mob_voice/wh/${month}/${date}"
    if(!exists(path)){
      val table = s"(SELECT * FROM ls6_acct_wh.ur_mob_voice_${month}_t WHERE PARTITION_ID_DAY=${date})x"
      spark.read.jdbc(anti_url,table,"PARTITION_ID_SERV",0,10,10,properties).write.mode(SaveMode.Overwrite).parquet(path)
    }
    spark.read.parquet(path)
  }

  def backUpWhUrMobLte(month:String): Unit = {
    val path = s"ur_mob_lte/wh/${month}"
    val table = s"ls6_acct_wh.ur_mob_lte_${month}_t"
    spark.read.jdbc(anti_url,table,"PARTITION_ID_DAY",1,32,31,properties).write.mode("overwrite").parquet(path)
  }

  def getWhUrMobLteDay(day:String): DataFrame = {
    val month = day.dropRight(2)
    val date = day.takeRight(2)
    val path = s"ur_mob_lte/wh/${month}/${date}"
    if(!exists(path)){
      val table = s"(SELECT * FROM ls6_acct_wh.ur_mob_lte_${month}_t WHERE PARTITION_ID_DAY=${date})x"
      spark.read.jdbc(anti_url,table,"PARTITION_ID_SERV",0,10,10,properties).write.mode(SaveMode.Overwrite).parquet(path)
    }
    spark.read.parquet(path)
  }

  def saveTianYanChaDataMonth(month:String): Unit = {
    val kaoqin = spark.read.parquet("kaoqin").toDF("NAME","BILLING_NBR")
    val lte = spark.read.parquet(s"ur_mob_lte/wh/${month}").select("BILLING_NBR", "START_DAY", "START_HOUR", "START_MINSEC","CELLID").withColumnRenamed("CELLID","CELL_ID")
    val mob = spark.read.parquet(s"ur_mob_voice/wh/${month}").select("BILLING_NBR", "START_DAY", "START_HOUR", "START_MINSEC","SELF_CELL_ID").withColumnRenamed("SELF_CELL_ID","CELL_ID")
    val k_lte = kaoqin.join(lte,Seq("BILLING_NBR"),"left").where(lte("START_HOUR").isin(8,17)).withColumn("DELTA_ABS",abs(lte("START_MINSEC")-3000))
    val k_mob = kaoqin.join(mob,Seq("BILLING_NBR"),"left").where(mob("START_HOUR").isin(8,17)).withColumn("DELTA_ABS",abs(mob("START_MINSEC")-3000))
    val merge = k_lte.unionByName(k_mob)
    val data = merge.orderBy(merge("DELTA_ABS").asc_nulls_last)
    val result = data.groupBy("NAME","BILLING_NBR","START_DAY", "START_HOUR").agg(first("START_MINSEC").as("START_MINSEC"),first("CELL_ID").as("CELL_ID")).where("CELL_ID IS NOT NULL")
    val r = result.join(p,Seq("CELL_ID"),"left")
    r.write.mode(SaveMode.Append).jdbc(anti_url,"TM_KAO_QIN",properties)
  }

  def saveTianYanChaDataDay(day:String): Unit = {
    val lte = getWhUrMobLteDay(day).select("BILLING_NBR", "START_DAY", "START_HOUR", "START_MINSEC","CELLID").withColumnRenamed("CELLID","CELL_ID")
    val mob = getWhUrMobVoiceDay(day).select("BILLING_NBR", "START_DAY", "START_HOUR", "START_MINSEC","SELF_CELL_ID").withColumnRenamed("SELF_CELL_ID","CELL_ID")
    val k_lte = kaoqin.join(lte,Seq("BILLING_NBR"),"left").where(lte("START_HOUR").isin(8,17)).withColumn("DELTA_ABS",abs(lte("START_MINSEC")-3000))
    val k_mob = kaoqin.join(mob,Seq("BILLING_NBR"),"left").where(mob("START_HOUR").isin(8,17)).withColumn("DELTA_ABS",abs(mob("START_MINSEC")-3000))
    val merge = k_lte.unionByName(k_mob)
    val data = merge.orderBy(merge("DELTA_ABS").asc_nulls_last)
    val result = data.groupBy("NAME","BILLING_NBR","START_DAY", "START_HOUR").agg(first("START_MINSEC").as("START_MINSEC"),first("CELL_ID").as("CELL_ID")).where("CELL_ID IS NOT NULL")
    val r = result.join(p,Seq("CELL_ID"),"left")
    r.withColumn("MONTH",r("START_DAY") * 0 + day.dropRight(2).toInt).write.mode(SaveMode.Append).jdbc(anti_url,"TM_KAO_QIN",properties)
  }

}