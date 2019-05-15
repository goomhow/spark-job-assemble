package com.ctc.juan


import com.ctc.redis.RelationShipQueue
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.graphx.lib

import scala.io.Source
import scala.util.parsing.json.JSON
import com.ctc.util.INSTANTCE._
class GraphRelationship(@transient spark: SparkSession, sheet:String){
  @transient val sc = spark.sparkContext
  spark.read.parquet("info/tc_tmp_hyxf_t").where(s"SHEET_NBR='$sheet'").registerTempTable("tc_tmp_hyxf_t")
  //高通话不平衡的用户号码清单
  val black_nbr = sc.textFile("user_dink/black_nbrs/201711").collect().toSet.filter(_.matches("\\d+")).map(_.toLong)
  //基于工作圈的用户关系图
  val graph = new GraphJuan(spark).parseWorkJuanToGraph("workday_juan/201806_201809")
  //hcode信息
  val json = JSON.parseFull(Source.fromFile("hcode.json").mkString) match {
    case Some(x:Map[String,List[String]]) => x
  }
  val hcode = sc.broadcast(json).value
  @transient val redis = RelationShipQueue()
  /**
    * 筛选出主动拨打该公司至少一次并且收到该公司来话至少2次的号码
    *
    * @param seed BOSS中某公司的号码集合
    * */
  def get_new_seed(seed:Set[Long]):Set[Long] = {
    val secondary = graph.subgraph(e => seed.contains(e.srcId)||seed.contains(e.dstId))
    secondary.outDegrees.filter(_._2>0).map(_._1).union(
      secondary.inDegrees.filter(_._2>1).map(_._1)
    ).collect().toSet.union(seed)
  }


  def predict(corp:String,area_code:String):(DataFrame,Int) = {
    println("*" * 10 + corp + "*" * 10)
    val mob_sql = s"select a.ACC_NBR from  tc_tmp_hyxf_t a where a.CUST_NAME='$corp' and a.SERVICE_TYPE='/s/t/mob'"
    val fix_sql = s"select a.ACC_NBR,a.AREA_CODE from tc_tmp_hyxf_t a where a.CUST_NAME='$corp' and a.SERVICE_TYPE='/s/t/fix'"
    val mob = spark.sql(mob_sql).collect().map(_.getString(0).stripMargin).toSet
    val fix = spark.sql(fix_sql).collect().map(r => r.getString(1).stripMargin + r.getString(0).stripMargin).toSet
    val seed = mob.union(fix).filter(_.matches("\\d+")).map(_.toLong).diff(black_nbr)
    val r = get_new_seed(get_new_seed(seed)).filter(_.toString.matches("^1[3-9]\\d{9}$")).map(r=>{
      val nbr = r.toString
      val i = hcode.getOrElse(nbr.take(7),Nil)
      val net = if(nbr.matches(CTC_NBR))"0" else "1"
      if(i.size==3) {
        Row(nbr, net, corp, "0" + i.head, i(1))
      }else{
        Row(nbr, net, corp, "", "")
      }
    }).filter(r => r.getString(3)==area_code || r.getString(1)=="1")
    println("*"*10+s"predict_size:${r.size}"+"*"*10)
    (spark.createDataFrame(sc.parallelize(r.toSeq),UM_STRUCT),r.size)
  }

  def process(isFirst:String): Unit = {
    val tableName=s"TM_UD_MODEL_$sheet"

    isFirst match {
      case "0" => {
        val corp_sql = s"SELECT DISTINCT CUST_NAME,AREA_CODE FROM tc_tmp_hyxf_t WHERE SHEET_NBR='$sheet'"
        val jobs = spark.sql(corp_sql).collect().map(r=>s"${r.getString(0)}:${r.getString(1)}")
        redis.add_jobs(jobs)
      }
      case "2"=>{
        val finished = spark.read.jdbc(anti_url,"(SELECT DISTINCT CUST_NAME from TM_UD_MODEL_1030)A",properties).collect().map(_.getString(0)).toSet
        val corp_sql = s"SELECT DISTINCT CUST_NAME,AREA_CODE FROM tc_tmp_hyxf_t WHERE SHEET_NBR='$sheet'"
        val jobs = spark.sql(corp_sql).collect().filter(r => ! finished.contains(r.getString(0))).map(r=>s"${r.getString(0)}:${r.getString(1)}")
        redis.add_jobs(jobs)
      }
      case _ => None
    }

    while(redis.has_job) {
      val job = redis.get_job()
      val (corp,area_code) = job.split(":") match {
        case Array(a,"null") => (a,null)
        case Array(a,b) => (a,b)
        case Array(a) => (a,"")
      }
      try{
        val (df,cnt) = predict(corp,area_code)
        if(cnt > 0)
          df.write.mode("append").jdbc(anti_url,tableName,properties)
      }catch {
        case e:Exception => {
          e.printStackTrace()
          redis.add_job(job)
        }
      }
    }
  }

}
