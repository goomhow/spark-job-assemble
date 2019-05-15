package com.ctc.juan

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{collect_set, count}
import com.ctc.util.HDFSUtil
class PhoneSerialProcess (@transient spark: SparkSession) extends Serializable {
  @transient val sc = spark.sparkContext
  def get_same_phone_nbr(fname:String="info/PRD_TMN_MOB_SERV_DAY"):RDD[Set[String]] = {
    val saved = "info/phone_nbr_set"
    if (HDFSUtil.exists(saved))
      sc.textFile(saved).map(_.split(",").toSet)
    else
      spark.read.parquet(fname)
        .select("SERIAL_NO", "ACCS_NBR")
        .groupBy("SERIAL_NO")
        .agg(collect_set("ACCS_NBR").as("nbrs"), count("SERIAL_NO").as("cnt"))
        .where("cnt > 1")
        .select("nbrs")
        .rdd
        .map(_.getList(0).toArray.map(_.toString).toSet).filter(s => s.size > 1 && s.size < 7)
  }

}