package com.ctc.tmp


import java.text.SimpleDateFormat
import java.util.Date
import java.sql.{Connection, DriverManager, PreparedStatement}

import scala.annotation.meta.param
import scala.collection.mutable.WrappedArray

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, collect_set, count, countDistinct, date_format, lit, size, substring, sum, to_timestamp, udf}
import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator

import com.ctc.util.INSTANTCE.{ALL_LOCALNETS, anti_url, anti_url2, msg_url, normalizePhoneNumber, properties}
import com.ctc.util.AppUtils.createDefaultSession
import com.ctc.util.DateUtil._
import com.ctc.tmp.StopOneHour._

class StopOneHour(@transient @param spark: SparkSession) extends Serializable {
  Class.forName("oracle.jdbc.driver.OracleDriver")

  def sendMsg(count: Long): Unit = {
    val msg = s"今日截止${fmt.format(new Date())},共预测可疑用户${count}个"
    logger.info(s"准备发送短信，短信内容：$msg")
    val target_nbr = Array("18907191059", "18907192073", "18907186517", "18907186569", "18907186127", "18907186488")
    var conn: Connection = null
    var ps: PreparedStatement = null
    try {
      conn = DriverManager.getConnection(msg_url, properties)
      conn.setAutoCommit(false)
      ps = conn.prepareStatement("insert into info.short_msg_cdma (nbr, msg, create_date, sts, remark) values (?, ?, sysdate, 0, '一小时关停系统')")
      for (nbr <- target_nbr) {
        ps.setString(1, nbr)
        ps.setString(2, msg)
        ps.addBatch()
      }
      val result = ps.executeBatch()
      conn.commit()
      if (result.toSeq.exists(_ == 0)) {
        conn.rollback()
        throw new Exception("发送短信失败")
      }
      logger.info("成功发送短信")
    } catch {
      case e: Exception => logger.error(s"发送短信失败，失败信息：${e.getMessage}");
    } finally {
      if (ps != null) ps.close()
      if (conn != null) conn.close()
    }
  }

  def doJob(): Long = {
    logger.info("开始计算灰名单数据")
    val today = getCurrentDay()
    val prev60 = next_day(today, -60)
    val table =
      s"""SELECT A.ACC_NBR FROM
        (SELECT DISTINCT BILLING_NBR AS ACC_NBR FROM SPARK_HOTAREA_ONE_HOUR WHERE BATCHNUM LIKE '${today}%')A
        INNER JOIN
        (SELECT DISTINCT ACC_NBR FROM HOT_AREA_GREY_LIST WHERE SHEET_NBR LIKE '${today}%')B
        ON A.ACC_NBR=B.ACC_NBR
      """.stripMargin

    val df = spark.read.jdbc(anti_url2, s"($table)X", properties)

    val month = spark.read.parquet("hotarea_month").where(col("day").between(prev60, today)
      && col("rate_opp_area") > 3
      && col("hotareanum") > 0
      && col("rate_opp") / col("cnt_opp") > 0.5
      && col("cnt_opp") > 20
    ).withColumnRenamed("BILLING_NBR", "ACC_NBR")

    val callRateDF = spark.read.parquet(HotArea.RESULT_PATH).where("RATE_OPP > 0.5")

    var r = df.select("ACC_NBR").join(month, Seq("ACC_NBR"), "inner")
      .groupBy("ACC_NBR")
      .agg(
        count(col("hotareanum")).as("hotarea_day_cnt"),
        sum(col("hotareanum")).as("hotarea_sum")
      )
      .join(callRateDF, Seq("ACC_NBR"), "INNER")
      .join(df, Seq("ACC_NBR"), "left")
      .coalesce(50)

    val schema = r.schema

    for (i <- schema) {
      if (i.dataType.isInstanceOf[DecimalType]) {
        r = r.withColumn(i.name, col(i.name).cast(LongType))
      }
    }
    r.cache()
    val result_size = r.count()
    r.coalesce(10).write.mode("append").jdbc(anti_url2, "SPARK_HOT_AREA_RESULT_3", properties)
    r.unpersist()
    logger.info(s"成功计算灰名单数据 $result_size 条")
    result_size
  }

}

object StopOneHour {

  val logger = Logger.getLogger(this.getClass)
  PropertyConfigurator.configure("/home/spark/log4j.properties")
  val DAY_30 = 30 * 24 * 3600 * 1000L
  val fmt = new SimpleDateFormat("yyyy年MM月dd日hh时mm分")

  val month = getCurrentMonth()
  val daylong = today(SECONDS_FMT)
  val curr_day = daylong.take(8)

  val hotareas = Array("0396","0412", "0595", "0596", "0597", "0662", "0668", "0738", "0759", "0768", "0771", "0775", "0838","0898")
  val arraytos = udf((ary: WrappedArray[String]) => ary.mkString(","))
  val arraytohot = udf((ary: WrappedArray[String]) => ary.intersect(hotareas).size)
  val norm_nbr = udf((x: String, y: String, z: String) => normalizePhoneNumber(x, y, z))


  def main(args: Array[String]): Unit = {
    logger.info("开始生成SPARK_HOTAREA_ONE_HOUR表")
    val spark = createDefaultSession("StopOneHour")

    val antifraudV2 = new StopOneHour(spark)

    val hcode = spark.read.jdbc(anti_url, "(select lower_mobile_prefix, area_code as OPP_AREA_CODE from  ls7_router.code_mobile_prefix@to_onep)a", properties).
      select(substring(col("LOWER_MOBILE_PREFIX"), 0, 7).as("opp_num_suffix"),
        col("OPP_AREA_CODE"))

    val whitelist = spark.read.jdbc(anti_url, "(select distinct acc_nbr from white_list_t)a", properties).cache
    var flag = 0
    val dfs = for (localnet <- ALL_LOCALNETS){
      val path = s"billing/mob/${month}/%s${localnet}_${month}"

      val mob = spark.read.parquet(path.format("ur_mob_voice_full_")).select("BILLING_NBR",
        "ORG_TRM_ID",
        "START_TIME",
        "VISIT_AREA_CODE", // 出差频次 关键人 决策人
        "RAW_DURATION", // 个人通信行为
        "OPP_NUMBER_SUFFIX", // 关系圈
        "OPP_AREA_CODE", // 关系圈、关键人
        "SELF_CELL_ID" // 工作性质 内勤、外侵 快递、外美
      ).withColumn("START_TIME", to_timestamp(col("START_TIME"), "yyyyMMddHHmmss")).
        withColumn("day", date_format(col("START_TIME"), "yyyyMMdd")).distinct()

      val ocs = spark.read.parquet(path.format("ur_ocs_voice_full_")).select("BILLING_NBR", "SERVICE_SCENARIOUS", "START_TIME",
        "CALLING_PARTY_VISITED_CITY", "RAW_DURATION", "TERM_NBR", "TERM_AREA_CODE",
        "CALLING_PARTY_CELLID" // CALLING_PARTY_LOCATION_CITY
      ).
        selectExpr("BILLING_NBR",
          "case when SERVICE_SCENARIOUS in (200, 202) then '0' else '1' end as ORG_TRM_ID",
          "START_TIME",
          "CALLING_PARTY_VISITED_CITY as VISIT_AREA_CODE",
          "RAW_DURATION", "TERM_NBR as OPP_NUMBER_SUFFIX",
          "TERM_AREA_CODE as OPP_AREA_CODE1",
          "CALLING_PARTY_CELLID as SELF_CELL_ID"
        ).withColumn("START_TIME", to_timestamp(col("START_TIME"), "yyyyMMddHHmmss")).
        withColumn("OPP_NUMBER_SUFFIX", norm_nbr(col("OPP_NUMBER_SUFFIX"), col("OPP_AREA_CODE1"),
          lit(""))).
        withColumn("day", date_format(col("START_TIME"), "yyyyMMdd")).distinct().
        withColumn("opp_num_suffix", substring(col("OPP_NUMBER_SUFFIX"), 0, 7)).
        join(hcode, Seq("opp_num_suffix"), "left").select("BILLING_NBR", "ORG_TRM_ID", "START_TIME",
        "VISIT_AREA_CODE", "RAW_DURATION", "OPP_NUMBER_SUFFIX", "OPP_AREA_CODE",
        "SELF_CELL_ID", "day"
      ).distinct

      val train = ocs.unionByName(mob)

      train.select("BILLING_NBR").
        except(whitelist.select(col("acc_nbr").as("BILLING_NBR"))).
        join(train, Seq("BILLING_NBR")).filter(s"day = $curr_day").
        groupBy("BILLING_NBR").
        agg(collect_set("VISIT_AREA_CODE").as("areas"),
          countDistinct("OPP_AREA_CODE").alias("rate_opp_area")).
        withColumn("hotareanum", arraytohot(col("areas"))).
        withColumn("areanum", size(col("areas"))).
        filter("hotareanum between 1 and 3 and areanum < 3 and rate_opp_area > 3").
        select("BILLING_NBR").
        withColumn("BATCHNUM", lit(daylong)).
        join(train.filter(s"day = $curr_day").select("BILLING_NBR","AREAS","SELF_CELL_ID"), Seq("BILLING_NBR"),"LEFT").
        coalesce(10).
        write.
        mode(if (flag == 0)"overwrite" else "append").
        jdbc(anti_url, "SPARK_HOTAREA_ONE_HOUR", properties)
      flag = flag + 1
    }

    logger.info("成功生成SPARK_HOTAREA_ONE_HOUR表")

    val cnt = antifraudV2.doJob()

    //antifraudV2.sendMsg(cnt)

    spark.stop()

  }

}