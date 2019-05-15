package com.ctc.backup

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import com.ctc.backup.BackUpVip.HDFS_BASEDIR
import com.ctc.util.NbrUtil.normalize_phone
import com.ctc.juan.VipHBaseRDD
class BackUpVip(@transient spark:SparkSession) {
  @transient val sc = spark.sparkContext
  def parse_data_zj(local_net:String, month:String, day:String=null,day_table:String = "vip:mldata_day"): Unit = {
    """统计主叫"""
    val hdfs_file = HDFS_BASEDIR.format(month, s"ur_mob_voice_full_${local_net}_${month}")
    val hdfs_ocs_file = HDFS_BASEDIR.format(month, s"ur_ocs_voice_full_${local_net}_${month}")

    //    start_time = time.time()
    //    logger.info("Parsing HDFS file %s ..", hdfs_file)

    val normalize_3 = udf((a: String, b: String, c: String) => normalize_phone(a, b, c))
    val normalize_2 = udf((a: String, b: String) => normalize_phone(a, b))

    var df = spark.read.parquet(hdfs_file).filter("ORG_TRM_ID='0'")
    if (day != null) df = df.filter(df("START_TIME").startsWith(day))
    df = df.select(
      df("START_TIME"),
      df("BILLING_NBR"),
      normalize_3(df("OPP_NUMBER"), df("OPP_AREA_CODE"), df("OPP_NUMBER_SUFFIX")).alias("OPP_NUMBER"),
      df("RAW_DURATION")
    )

    var df_ocs = spark.read.parquet(hdfs_ocs_file).filter("SERVICE_SCENARIOUS in (200, 202)")
    if (day != null)
      df_ocs = df_ocs.filter(df_ocs("START_TIME").startsWith(day))
    df_ocs = df_ocs.select(
      df_ocs("START_TIME"),
      df_ocs("BILLING_NBR"),
      normalize_2(df_ocs("TERM_NBR"), df_ocs("TERM_AREA_CODE")).alias("OPP_NUMBER"),
      df_ocs("RAW_DURATION")
    )

    // 合并预付费和后付费数据，按天+主叫+被叫统计次数和时长，为Top N统计做准备
    df.union(df_ocs).registerTempTable("billing_data")

    val df_day_full = spark.sql(
      """select concat(substr(START_TIME, 1, 8), "_", BILLING_NBR) day_billing_nbr,
  OPP_NUMBER, count(1) as cnt,
  sum(if(RAW_DURATION <= 15, 1, 0)) small_cnt,
  cast(sum(RAW_DURATION) as bigint) as total_duration
from billing_data
  group by concat(substr(START_TIME, 1, 8), "_", BILLING_NBR), OPP_NUMBER""")

    val df_day_work = spark.sql(
      """select concat(substr(START_TIME, 1, 8), "_", BILLING_NBR) day_billing_nbr,
  OPP_NUMBER, count(1) as cnt,
  sum(if(RAW_DURATION <= 15, 1, 0)) small_cnt,
  cast(sum(RAW_DURATION) as bigint) as total_duration
from billing_data
  where substr(START_TIME, 9, 2) between "09" and "16"
  group by concat(substr(START_TIME, 1, 8), "_", BILLING_NBR), OPP_NUMBER""")

    val df_day_free = spark.sql(
      """select concat(substr(START_TIME, 1, 8), "_", BILLING_NBR) day_billing_nbr,
  OPP_NUMBER, count(1) as cnt,
  sum(if(RAW_DURATION <= 15, 1, 0)) small_cnt,
  cast(sum(RAW_DURATION) as bigint) as total_duration
from billing_data
  where substr(START_TIME, 9, 2) not between "09" and "16"
  group by concat(substr(START_TIME, 1, 8), "_", BILLING_NBR), OPP_NUMBER""")
    val hbase = new VipHBaseRDD(sc)
    for ((df_day, family) <- Seq((df_day_full, "full"), (df_day_work, "work"), (df_day_free, "free"))) {
      val rdd = df_day.rdd.map{
        case Row(day_billing_nbr:String,opp_number:String,cnt:Int,small_cnt:Int,total_duration:java.math.BigDecimal) => {
          (day_billing_nbr,(List((opp_number,cnt)),cnt,total_duration.longValue(),small_cnt))
        }
      }.groupByKey().mapValues(
        arr => {
          val active_day_cnt = arr.size
          val data = if(active_day_cnt>1){
            arr.reduce((a,b)=>{
              (a._1:::b._1,a._2+b._2,a._3 + b._3,a._4 + b._4)
            })
          }else{
            arr.toList.head
          }
          val topN = data._1
            .groupBy(_._1)
            .mapValues(_.map(_._2).sum)
            .toList.sortWith(_._2>_._2)
            .map(r => s"${r._1}:${r._2}").mkString(",")
          val base_billing = s"${data._2},${data._3},${data._4},${active_day_cnt}"
          Map(
            s"${family}:top_cnt" -> topN,
            s"${family}:base_billing" -> base_billing
          )
        }
      )
      hbase.save_rdd(rdd,day_table)
    }
  }

}
object BackUpVip{
  def apply(@transient spark: SparkSession): BackUpVip = new BackUpVip(spark)
  val HDFS_BASEDIR = "/user/spark/billing/mob/%s/%s"

}