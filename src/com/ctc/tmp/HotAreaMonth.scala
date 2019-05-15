package com.ctc.tmp



import com.ctc.util.AppUtils.createDefaultSession
import com.ctc.util.INSTANTCE.{ALL_LOCALNETS, anti_url, normalizePhoneNumber, properties}
import com.ctc.util.DateUtil._
import com.ctc.util.HDFSUtil
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{min,col, collect_set, count, countDistinct, date_format, lit, size, substring, to_timestamp, udf}

import scala.collection.mutable.WrappedArray

object HotAreaMonth {
  val HOT_AREA_MONTH_PATH = "hotarea_month"

  val spark = createDefaultSession("StopOneHour")

  val hcode = spark.read.jdbc(anti_url, "(select lower_mobile_prefix, area_code as OPP_AREA_CODE from  ls7_router.code_mobile_prefix@to_onep)a", properties).
    select(substring(col("LOWER_MOBILE_PREFIX"), 0, 7).as("opp_num_suffix"),
      col("OPP_AREA_CODE")).cache

  val whitelist = spark.read.jdbc(anti_url, "(select distinct acc_nbr from white_list_t)a", properties).cache

  val arraytos = udf((ary: WrappedArray[String]) => ary.mkString(","))
  val arraytohot = udf((ary: WrappedArray[String]) =>
    Set("0396","0412", "0595", "0596", "0597", "0662", "0668", "0738", "0759", "0768", "0771", "0775", "0838","0898").intersect(ary.toSet).size)
  val norm_nbr = udf((x: String, y: String, z: String) => normalizePhoneNumber(x,y,z))

  def persist(month:String): Unit ={

    for (localnet <- ALL_LOCALNETS) {
      for (stype <- Array("ur_ocs_voice_full_", "ur_mob_voice_full_")) {
        val path = s"billing/mob/${month}/${stype}${localnet}_${month}"
        println(path)

        val train = if (stype == "ur_mob_voice_full_") {
          spark.read.parquet(path).select("BILLING_NBR",
            "ORG_TRM_ID",
            "START_TIME",
            "VISIT_AREA_CODE",   // 出差频次 关键人 决策人
            "RAW_DURATION",      // 个人通信行为
            "OPP_NUMBER_SUFFIX",  // 关系圈
            "OPP_AREA_CODE",      // 关系圈、关键人
            "SELF_CELL_ID"        // 工作性质 内勤、外侵 快递、外美
          ).withColumn("START_TIME", to_timestamp(col("START_TIME"), "yyyyMMddHHmmss")).
            withColumn("day",date_format(col("START_TIME"), "yyyyMMdd")).distinct
        } else {
          spark.read.parquet(path).select("BILLING_NBR", "SERVICE_SCENARIOUS", "START_TIME",
            "CALLING_PARTY_VISITED_CITY", "RAW_DURATION", "TERM_NBR","TERM_AREA_CODE",
            "CALLING_PARTY_CELLID" // CALLING_PARTY_LOCATION_CITY
          ).selectExpr("BILLING_NBR",
            "case when SERVICE_SCENARIOUS in (200, 202) then '0' else '1' end as ORG_TRM_ID",
            "START_TIME",
            "CALLING_PARTY_VISITED_CITY as VISIT_AREA_CODE",
            "RAW_DURATION", "TERM_NBR as OPP_NUMBER_SUFFIX",
            "TERM_AREA_CODE as OPP_AREA_CODE1",
            "CALLING_PARTY_CELLID as SELF_CELL_ID"
          ).withColumn("START_TIME", to_timestamp(col("START_TIME"), "yyyyMMddHHmmss")).
            withColumn("OPP_NUMBER_SUFFIX", norm_nbr(col("OPP_NUMBER_SUFFIX"),col("OPP_AREA_CODE1"),
              lit(""))).
            withColumn("day", date_format(col("START_TIME"), "yyyyMMdd")).distinct().
            withColumn("opp_num_suffix", substring(col("OPP_NUMBER_SUFFIX"), 0, 7)).
            join(hcode, Seq("opp_num_suffix"), "left").select("BILLING_NBR", "ORG_TRM_ID", "START_TIME",
            "VISIT_AREA_CODE", "RAW_DURATION", "OPP_NUMBER_SUFFIX", "OPP_AREA_CODE",
            "SELF_CELL_ID", "day"
          ).distinct
        }

        train.cache()
        val target = train.groupBy("BILLING_NBR", "day").
          agg(collect_set("VISIT_AREA_CODE").as("areas"),
            countDistinct("OPP_NUMBER_SUFFIX").alias("rate_opp"),
            countDistinct("OPP_AREA_CODE").alias("rate_opp_area"),
            count("OPP_NUMBER_SUFFIX").alias("cnt_opp")
          ).
          withColumn("hotareanum", arraytohot(col("areas"))).
          withColumn("areadtail", arraytos(col("areas"))).
          withColumn("areanum", size(col("areas"))).
          select("BILLING_NBR", "day", "areadtail","hotareanum", "areanum", "cnt_opp","rate_opp", "rate_opp_area").
          distinct()

        val cnt = target.count()
        val partition = cnt match {
          case a if a == 0 => 0
          case a if a<=1000 => 1
          case a if a<=10000 => 10
          case a if a<=100000 => 50
          case a if a<=1000000 => 100
          case a  => 300
        }
        if(partition > 0)
          target.coalesce(partition).write.mode("append").parquet(HOT_AREA_MONTH_PATH)
        println("入库成功")
        train.unpersist()
      }
    }
  }

  def loadLocalData(monthOrDay:String)(localnet:String): DataFrame ={
    val month = if(monthOrDay.size == 6) monthOrDay else monthOrDay.take(6)

    val ocs_path = s"billing/mob/${month}/ur_ocs_voice_full_${localnet}_${month}"
    val mob_path = s"billing/mob/${month}/ur_mob_voice_full_${localnet}_${month}"

    val mob = spark.read.parquet(mob_path).select("BILLING_NBR",
        "ORG_TRM_ID",
        "START_TIME",
        "VISIT_AREA_CODE",   // 出差频次 关键人 决策人
        "RAW_DURATION",      // 个人通信行为
        "OPP_NUMBER_SUFFIX",  // 关系圈
        "OPP_AREA_CODE",      // 关系圈、关键人
        "SELF_CELL_ID"        // 工作性质 内勤、外侵 快递、外美
      ).withColumn("START_TIME", to_timestamp(col("START_TIME"), "yyyyMMddHHmmss")).
        withColumn("day",date_format(col("START_TIME"), "yyyyMMdd"))

    val ocs =  spark.read.parquet(ocs_path).select("BILLING_NBR", "SERVICE_SCENARIOUS", "START_TIME",
        "CALLING_PARTY_VISITED_CITY", "RAW_DURATION", "TERM_NBR","TERM_AREA_CODE",
        "CALLING_PARTY_CELLID" // CALLING_PARTY_LOCATION_CITY
      ).selectExpr("BILLING_NBR",
        "case when SERVICE_SCENARIOUS in (200, 202) then '0' else '1' end as ORG_TRM_ID",
        "START_TIME",
        "CALLING_PARTY_VISITED_CITY as VISIT_AREA_CODE",
        "RAW_DURATION", "TERM_NBR as OPP_NUMBER_SUFFIX",
        "TERM_AREA_CODE as OPP_AREA_CODE1",
        "CALLING_PARTY_CELLID as SELF_CELL_ID"
      ).withColumn("START_TIME", to_timestamp(col("START_TIME"), "yyyyMMddHHmmss")).
        withColumn("OPP_NUMBER_SUFFIX", norm_nbr(col("OPP_NUMBER_SUFFIX"),col("OPP_AREA_CODE1"),
          lit(""))).
        withColumn("day", date_format(col("START_TIME"), "yyyyMMdd")).distinct().
        withColumn("opp_num_suffix", substring(col("OPP_NUMBER_SUFFIX"), 0, 7)).
        join(hcode, Seq("opp_num_suffix"), "left").select("BILLING_NBR", "ORG_TRM_ID", "START_TIME",
        "VISIT_AREA_CODE", "RAW_DURATION", "OPP_NUMBER_SUFFIX", "OPP_AREA_CODE",
        "SELF_CELL_ID", "day"
      )

    val df = mob.unionByName(ocs).distinct()
    if(monthOrDay.size == 6) df else df.where(df("day") === monthOrDay)
  }

  def persistData(monthOrDay:String): Unit ={
    def loadData = loadLocalData(monthOrDay)_
    for (localnet <- ALL_LOCALNETS) {

        val train = loadData(localnet)
        train.cache()
        val target = train.groupBy("BILLING_NBR", "day").
          agg(collect_set("VISIT_AREA_CODE").as("areas"),
            countDistinct("OPP_NUMBER_SUFFIX").alias("rate_opp"),
            countDistinct("OPP_AREA_CODE").alias("rate_opp_area"),
            count("OPP_NUMBER_SUFFIX").alias("cnt_opp")
          ).
          withColumn("hotareanum", arraytohot(col("areas"))).
          withColumn("areadtail", arraytos(col("areas"))).
          withColumn("areanum", size(col("areas"))).
          select("BILLING_NBR", "day", "areadtail","hotareanum", "areanum", "cnt_opp","rate_opp", "rate_opp_area").
          distinct()

        val cnt = target.count()
        val partition = cnt match {
          case a if a == 0 => 0
          case a if a<=1000 => 1
          case a if a<=10000 => 10
          case a if a<=100000 => 20
          case a if a<=1000000 => 50
          case a if a<=5000000 => 100
          case _  => 200
        }
        if(partition > 0)
          target.coalesce(partition).write.mode("append").parquet(HOT_AREA_MONTH_PATH)
        println("入库成功")
        train.unpersist()
      }

  }

  def persistDay(day:String=null): Unit ={
    val date = if(day == null) get_yestoday() else day
    persistData(date)
  }

  def initData(m:String=null): Unit = {
    HDFSUtil.delete("hotarea_month",true)
    val month = if(m==null) getCurrentMonth() else m
    for(i <- Array(next_month(month,-2),next_month(month,-1),month)){
      persistData(i)
    }
  }

  def cronJob(): Unit ={
    val today = getCurrentDay()
    val df = spark.read.parquet(HOT_AREA_MONTH_PATH)
    val min_day = df.select(min("day")).collect().apply(0).getString(0)
    val interval =(yyyyMMdd.parse(today).getTime - yyyyMMdd.parse(min_day).getTime)/(24*3600)
    if(interval >= 75){
      val tmpPath = "tmp/hotarea_month"
      val prev_60 = next_day(today,-61)
      df.where(s"day >= '${prev_60}'").repartition(600).write.mode("overwrite").parquet(tmpPath)
      HDFSUtil.delete(HOT_AREA_MONTH_PATH,true)
      HDFSUtil.rename(tmpPath,HOT_AREA_MONTH_PATH)
    }
    persistDay()
  }

  def main(args: Array[String]): Unit = {
    args match {
      case Array() => cronJob()
      case Array(a) if a.length == 6 => initData(a)
      case Array(a) if a.length == 8 => persistDay(a)
    }
  }

}