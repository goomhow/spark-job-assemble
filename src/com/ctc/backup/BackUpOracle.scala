package com.ctc.backup


import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession,Row}
import org.apache.spark.sql.functions._
import com.ctc.util.INSTANTCE._
import com.ctc.util.DateUtil._
import com.ctc.util.HDFSUtil._
import com.ctc.util.AppUtils.createDefaultSession


class BackUpOracle (@transient spark:SparkSession) extends DataBase{

  def importByCsv(path:String,table:String): Unit = {
    val data = spark.read.option("header","true").csv(path)
    data.repartition(50).write.mode("append").jdbc(anti_url,table,properties)
  }

  override def exportByCsv(path: String, table: String, partitionColumn: String, start: Int, end: Int): Unit = {
    val data = spark.read.jdbc(anti_url,table,partitionColumn,start,end,end-start,properties)
    data.write.mode(SaveMode.Overwrite).option("header","true").csv(path)
  }

  override def simpleExportByCsv(path: String, table: String): Unit = {
    val data = spark.read.jdbc(anti_url,table,properties)
    data.write.mode(SaveMode.Overwrite).option("header","true").csv(path)
  }
  val sql_template = "SELECT * FROM ls65_sid.%s@to_sc_sid union all SELECT * FROM ls65_sid2.%s@to_sc_sid"
  def backUpCustContactInfo(): Unit ={
    val t_sql = sql_template.format("cust_contact_info_t","cust_contact_info_t")
    spark.read.jdbc(anti_url,s"(${t_sql})a",properties).write
    .mode("overwrite")
    .parquet("info/cust_contact_info_t")
  }

  def backUpProductOfferDetail(): Unit = {
    val t_sql = "SELECT * FROM ls65_sid.product_offer_detail_t@to_sc_sid union all SELECT * FROM ls65_sid2.product_offer_detail_t@to_sc_sid"
    spark
      .read
      .jdbc(anti_url,s"(${t_sql})a",properties)
      .write
      .mode("overwrite")
      .parquet("info/product_offer_detail_t")
  }

  def backUpCustT():Unit = {
    val t_sql = "SELECT * FROM ls65_sid.cust_t@to_sc_sid union all SELECT * FROM ls65_sid2.cust_t@to_sc_sid"
    spark.read.jdbc(anti_url,s"($t_sql)A","PARTITION_ID_REGION",1001,1019,18,properties)
      .repartition(300)
      .write.mode("overwrite").parquet("info/cust_t")
  }

  def backUpServT():Unit = {
    val t_sql = "SELECT * FROM ls65_sid.serv_t@to_sc_sid union all SELECT * FROM ls65_sid2.serv_t@to_sc_sid"
    spark.read.jdbc(anti_url,s"($t_sql)A","PARTITION_ID_REGION",1001,1019,18,properties)
      .repartition(300)
      .write.mode("overwrite").parquet("info/serv_t")
  }

  def backUpServS():Unit = {
    val t_sql = "SELECT * FROM ls65_sid.serv_subscriber_t@to_sc_sid union all SELECT * FROM ls65_sid2.serv_subscriber_t@to_sc_sid"
    spark.read.jdbc(anti_url,s"($t_sql)A","PARTITION_ID_REGION",1001,1019,18,properties)
      .repartition(300)
      .write.mode("overwrite").parquet("info/serv_subscriber")
  }

  def backUpTable(tableName:String): Unit ={
    val path = "financial/"+tableName.split("_").takeRight(1)(0).toUpperCase
    val months = 201707.to(201712).toList:::201801.to(201808).toList
    val data = spark.read.jdbc(anti_url,tableName,months.map("MONTH=%d".format(_)).toArray,properties)
    data.write.mode(SaveMode.Overwrite).parquet(path)
  }

  def backUpObject(): Unit ={
    val df = spark.read.jdbc(anti_url,"ls65_sid.object_to_tax_t@to_sc_sid","PARTITION_ID_MONTH",1,25,24,properties)
    df.write.mode("overwrite").parquet("info/object_to_tax")
    val df2 = spark.read.jdbc(anti_url,"ls65_sid2.object_to_tax_t@to_sc_sid","PARTITION_ID_MONTH",1,25,24,properties)
    df2.write.mode("append").parquet("info/object_to_tax")
  }
  /**
    *
  --后付费
SELECT billing_nbr,start_time,end_time,raw_duration,flux,recv_bytes,send_bytes,account_ip FROM ls6_acct_wh.ur_inet_201807_t WHERE service_type='/s/i/kd';

--后付费校院
SELECT billing_nbr ,start_time ,end_time ,raw_duration ,raw_flux,recv_bytes,send_bytes,mip_home_agent
 FROM ls6_acct_wh.ur_mob_stream_201807_t WHERE  serv_id= 19200893220 AND service_type='/s/i/kd';

--预付费
SELECT billing_nbr,start_time,end_time,raw_duration,nas_ip
 FROM ls6_acct_wh.ur_ocs_dsl_201807_t WHERE serv_id=19122864910
    */
  val end_sql = s"""SELECT START_DAY,
	%s REGION_ID,
	SERV_ID,
	START_TIME,
	END_TIME,
	RAW_DURATION AS DURA,
	FLUX,
	RECV_BYTES/1024 RECV,
	SEND_BYTES/1024 SEND,
	ACCOUNT_IP IP
FROM
	%s.UR_INET_%s_T
WHERE
	SERVICE_TYPE = '/s/i/kd'"""
  val school_end_sql = s"""SELECT START_DAY,
	%s REGION_ID,
	SERV_ID,
	START_TIME,
	END_TIME,
	RAW_DURATION AS DURA,
	RAW_FLUX AS FLUX,
	RECV_BYTES/1024 RECV,
	SEND_BYTES/1024 SEND,
	MIP_HOME_AGENT  IP
FROM
	%s.UR_MOB_STREAM_%s_T
WHERE
	SERVICE_TYPE = '/s/i/kd'"""
  val pre_sql = s"""SELECT START_DAY,
	%s REGION_ID,
	SERV_ID,
	START_TIME,
	END_TIME,
	RAW_DURATION DURA,
	- 2 FLUX,
	- 1 RECV,
	- 1 SEND,
	NAS_IP IP
FROM
	%s.UR_OCS_DSL_%s_T
WHERE
	SERVICE_TYPE = '/s/i/kd'"""
  def __readFluxTable(sql:String):DataFrame={
    println(sql)
    spark.read.jdbc(anti_url,s"($sql)x","START_DAY",1,32,31,properties)
  }
  def getWhBroadbandFlume(month:String):DataFrame={
    __readFluxTable(end_sql.format("1001","ls6_acct_wh",month))
      .unionAll(__readFluxTable(school_end_sql.format("1001","ls6_acct_wh",month)))
      .unionAll(__readFluxTable(pre_sql.format("1001","ls6_acct_wh",month)))
  }

  def getNoWhBroadbandFlume(region_id:String,tabel:String,month:String):DataFrame={
    __readFluxTable(end_sql.format(region_id,tabel,month))
      .unionAll(__readFluxTable(pre_sql.format(region_id,tabel,month)))
  }

  def backUpBroadbandFlume(month:String): Unit ={
    val path = BROAD_BAND_FLUX_PATH.format(month)
    val y = Array(("1003","ls65_acct_xf"), ("1004","ls65_acct_hg"), ("1005","ls65_acct_yc"),
      ("1006","ls65_acct_xg"), ("1007","ls65_acct_ez"), ("1008","ls65_acct_xn"), ("1009","ls65_acct_sy"),
      ("1010","ls65_acct_jm"), ("1011","ls65_acct_hs"), ("1012","ls65_acct_sz"), ("1013","ls65_acct_es"),
      ("1014","ls65_acct_xt"), ("1015","ls65_acct_tm"), ("1016","ls65_acct_qj"), ("1017","ls65_acct_snj"),
      ("1018","ls65_acct_jz"))
    getWhBroadbandFlume(month).write.mode("overwrite").parquet(path)
    for(i <- y)
      getNoWhBroadbandFlume(i._1,i._2,month).write.mode("append").parquet(path)
  }
/**
  *START_DAY, REGION_ID, SERV_ID, START_TIME, END_TIME, DURA, FLUX, RECV, SEND, IP
  *  * */
  import java.math.BigDecimal
  /**
    * @param month 计费账务月份
    * */
  def transformBroadBandFlux(month:String):DataFrame = {
    // free 节假日 或者 平日 18时-早5点前
    val holidays = get_holiday(month).toSet
      .union(get_holiday(next_month(month,1)).take(2).toSet)
      .union(get_holiday(next_month(month,-1)).takeRight(2).toSet)
    val yyyyMMdd = new SimpleDateFormat("yyyyMMdd")
    val fpath = BROAD_BAND_FLUX_PATH.format(month)
    if(!exists(fpath)){
      backUpBroadbandFlume(month)
    }
    val df = spark.read.parquet(fpath).na.fill(0.0)
    /**
      * 计算休息时间段的各个指标
      * _1 FREE时段持续时间
      * _2 FREE时段总流量
      * _3 FREE时段上行流量
      * _4 FREE时段下行流量
      * */
    def free_time_size = udf((start:Timestamp,end:Timestamp,flux:BigDecimal,recv:BigDecimal,send:BigDecimal)=>{
      val start_str = yyyyMMdd.format(new Date(start.getTime))
      val dura = (end.getTime-start.getTime)/1000
      if(holidays.contains(start_str)){
        //节假日
        Array(BigDecimal.valueOf(dura),flux,recv,send)
      }else{
        //是否18点后
        val start_hour = start.getHours
        if(start_hour>=18){
          Array(BigDecimal.valueOf(dura),flux,recv,send)
        }else{
          start.setHours(18)
          val split_dura = (end.getTime-start.getTime)/1000
          val frac = BigDecimal.valueOf(split_dura.toDouble/dura)
          Array(BigDecimal.valueOf(split_dura),flux.multiply(frac),recv.multiply(frac),send.multiply(frac))
        }
      }
    })
    val Rcolumns=free_time_size(df("START_TIME"),df("END_TIME"),df("FLUX"),df("RECV"),df("SEND")).as("RESULT")
    val columns = df.columns.map(df.col(_)).toList:+Rcolumns
    val r = df.select(columns:_*)
    val result_col = r("RESULT")
    val r_cols = r.columns.map(r(_)):+
      result_col.getItem(0).as("FREE_DURA"):+
      result_col.getItem(1).as("FREE_FLUX"):+
      result_col.getItem(2).as("FREE_RECV"):+
      result_col.getItem(3).as("FREE_SEND")
    val data = r.select(r_cols:_*).drop("RESULT")
    data.groupBy("SERV_ID").agg(
      countDistinct(data("START_DAY")).as("USE_DAYS"),
      sum(data("DURA")).as("DURA"),
      sum(data("FLUX")).as("FLUX"),
      sum(data("RECV")).as("RECV"),
      sum(data("SEND")).as("SEND"),
      sum(data("FREE_DURA")).as("FREE_DURA"),
      sum(data("FREE_FLUX")).as("FREE_FLUX"),
      sum(data("FREE_RECV")).as("FREE_RECV"),
      sum(data("FREE_SEND")).as("FREE_SEND")
    )
  }

  def getBroadBandFlux(month:String):DataFrame = {
    val path = BROAD_BAND_AGG_PATH.format(month)
    if(!exists(path)){
      transformBroadBandFlux(month).write.parquet(path)
    }
    spark.read.parquet(path)
  }

  def saveTycAndCustObj(use_new:Boolean=false): Unit = {
    if(!exists("info/tm_tyc_pos") || use_new){
      val df = spark.read.jdbc(anti_url,"TM_TYC_POS",properties)
      df.repartition(50).write.mode("overwrite").parquet("info/tm_tyc_pos")
    }
    val tyc = spark.read.parquet("info/tm_tyc_pos").select("ID","CERT_NO").toDF("CORP_ID","TAX_REGISTER_ID")
    val obj = spark.read.parquet("info/object_to_tax").select("TAX_REGISTER_ID","TAX_CUST_ID")
    tyc.join(obj,Seq("TAX_REGISTER_ID"),"inner").write.mode("overwrite").jdbc(anti_url, "TM_TYC_OBJ_ID",properties)
  }
}

object BackUpOracle{
  def main(args:Array[String]): Unit ={
    val spark = createDefaultSession()
    val ora = new BackUpOracle(spark)
    args match {
      case Array("CUST_T") => ora.backUpCustT()
      case Array("SERV_T") => ora.backUpServT()
      case Array("SERV_S") => ora.backUpServS()
      case Array() => {
        ora.backUpCustT()
        ora.backUpServT()
        ora.backUpServS()
      }
    }
  }

}