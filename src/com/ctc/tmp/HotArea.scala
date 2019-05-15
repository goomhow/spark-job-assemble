package com.ctc.tmp

import com.ctc.tmp.HotArea._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable
import com.ctc.util.INSTANTCE.{anti_url, properties}
import com.ctc.util.DateUtil.{next_day, today}
import com.ctc.util.AppUtils._
import com.ctc.util.HDFSUtil.{delete, exists}

import scala.annotation.meta.param

class  HotArea(@(transient @param) spark: SparkSession) extends Serializable{

  def arraytos = udf((ary: mutable.WrappedArray[String]) => ary match {
    case null => ""
    case _ => ary.toSet.diff(HB_AREA).mkString(",").take(250)
  })

  def allHotAreaPhones(day:String=null): Unit = {
    val month = day.take(6)
    val last_30 = next_day(day, -30)
    val lastmonth = last_30.take(6)
    if(exists(RESULT_PATH)) delete(RESULT_PATH,true)
    for (localnet <- ALL_LOCALNETS) hotarea_phones(localnet, day, last_30, month, lastmonth)
    spark.read.parquet(RESULT_PATH).coalesce(50).write.mode("overwrite").jdbc(anti_url, RESULT_TABLE, properties)
  }


  def hotarea_phones(local_net: String, day: String, lastday: String, month: String, lastmonth: String,saveMode:String="append") = {
    val df = if (month == lastmonth) {
      load_data(local_net, month, lastday, day)
    } else {
      load_data(local_net, month, lastday, day).unionByName(load_data(local_net, lastmonth, lastday, day))
    }
    df.cache()
    val all_agg = df.groupBy("acc_nbr").agg(
      collect_set("area_code").alias("areas"),
      countDistinct("OPP_NUMBER").alias("rate_opp"),
      sum("RAW_DURATION").alias("last_dur"),
      count("OPP_NUMBER").alias("cnt_opp"),
      countDistinct("SELF_CELL_ID").alias("cells")
    ).select(col("acc_nbr"), col("rate_opp"), col("last_dur"), col("cnt_opp"), col("cells"), arraytos(col("areas")).alias("areas"))
    val zj = df.where("ORG_TRM_ID = 0").groupBy("acc_nbr").agg(countDistinct("OPP_NUMBER").alias("zj_rate_opp"),
      count("OPP_NUMBER").alias("zj_cnt_opp"))
    all_agg.join(zj, Seq("acc_nbr"), "left").withColumn("day", lit(day)).coalesce(50).write.mode(saveMode).parquet(RESULT_PATH)
    df.unpersist(false)
  }


  def load_data(local_net: String, month: String, lastday: String = null, day: String = null): DataFrame = {
    val hdfs_file = s"${HDFS_BASEDIR}/${month}/ur_mob_voice_full_${local_net}_${month}"
    val hdfs_ocs_file = s"${HDFS_BASEDIR}/${month}/ur_ocs_voice_full_${local_net}_${month}"

    val df = spark.read.parquet(hdfs_file)
      .where("START_TIME between '%s' and '%s'".format(lastday, day))
      .select("BILLING_NBR", "ORG_TRM_ID", "START_TIME",
        "VISIT_AREA_CODE", "RAW_DURATION", "OPP_NUMBER",
        "SELF_CELL_ID"
      )

    val df1 = spark.read.parquet(hdfs_ocs_file)
      .where("START_TIME between '%s' and '%s'".format(lastday, day))
      .select(
        "BILLING_NBR", "SERVICE_SCENARIOUS", "START_TIME",
        "CALLING_PARTY_VISITED_CITY", "RAW_DURATION", "TERM_NBR",
        "CALLING_PARTY_CELLID"
      )
      .selectExpr("BILLING_NBR",
        "case when SERVICE_SCENARIOUS in (200, 202) then '0' else '1' end as ORG_TRM_ID",
        "START_TIME",
        "CALLING_PARTY_VISITED_CITY as VISIT_AREA_CODE",
        "RAW_DURATION", "TERM_NBR as OPP_NUMBER",
        "CALLING_PARTY_CELLID as SELF_CELL_ID"
      )

    return df.unionByName(df1).distinct()
      .withColumnRenamed("BILLING_NBR", "acc_nbr")
      .withColumnRenamed("VISIT_AREA_CODE", "area_code")
  }
}
object HotArea {
  val HDFS_BASEDIR = "/user/spark/billing/mob"
  val RESULT_PATH = "hot_area/anti_alluser_day"
  val RESULT_TABLE = "anti_alluser_day"
  val ALL_LOCALNETS = Array("snj", "yc", "tm", "xg", "hg", "sy", "es", "xf", "sz", "xn", "jm", "jz", "xt", "qj", "hs", "wh", "ez")
  val HB_AREA = Set("027", "0710", "0713", "0717", "0712", "0711", "0715", "0719", "0724", "0714", "0722", "0718", "0728", "0728", "0728", "0719", "0716")

  def apply(spark: SparkSession): HotArea = new HotArea(spark)

  def main(args: Array[String]): Unit = {
    val spark = createDefaultSession("hot_area")
    val hotArea = HotArea(spark)
    val day = if(args.length>0) args(0) else today()
    hotArea.allHotAreaPhones(day)
  }

}
