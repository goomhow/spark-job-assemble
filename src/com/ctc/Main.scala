package com.ctc

import com.ctc.backup.{BackUpModelData, BackUpOracle, BackUpTD}
import com.ctc.juan.{GraphRelationship, SeedsGenerator}
import com.ctc.location.UserPosition
import com.ctc.loss.{BroadBandModel, LteModel}
import com.ctc.util.{DateUtil, HDFSUtil}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main{
  val conf = new SparkConf()
    .setAppName("lpa_juan")
    .set("spark.network.timeout","3600")
    .set("spark.executor.heartbeatInterval","3000")
  val spark =SparkSession.builder().config(conf).getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  val path_ocs = "billing/mob/%s/ur_ocs_voice_full_wh_%s"
  val path_mob = "billing/mob/%s/ur_mob_voice_full_wh_%s"

  def get_data(nbr:String,month:String): Long ={
      val c1 = spark.read.parquet(path_mob.format(month,month)).select(
        "BILLING_NBR",
        "ORG_TRM_ID", "START_TIME",
        "VISIT_AREA_CODE", "RAW_DURATION", "OPP_NUMBER",
        "SELF_CELL_ID"
      ).where(s"BILLING_NBR = '{nbr}' and  VISIT_AREA_CODE in ('027','0710','0713','0717','0712','0711','0715','0719','0724','0714','0722','0718','0728','0728','0728','0719','0716')").count()
      val c2 = spark.read.parquet(path_ocs.format(month,month)).select("BILLING_NBR", "SERVICE_SCENARIOUS", "START_TIME",
        "CALLING_PARTY_VISITED_CITY", "RAW_DURATION", "TERM_NBR",
        "CALLING_PARTY_CELLID"
      ).selectExpr("BILLING_NBR",
        "case when SERVICE_SCENARIOUS in (200, 202) then '0' else '1' end as ORG_TRM_ID",
        "START_TIME",
        "CALLING_PARTY_VISITED_CITY as VISIT_AREA_CODE",
        "RAW_DURATION", "TERM_NBR as OPP_NUMBER",
        "CALLING_PARTY_CELLID as SELF_CELL_ID"
      ).where(s"BILLING_NBR = '{nbr}' and  VISIT_AREA_CODE in ('027','0710','0713','0717','0712','0711','0715','0719','0724','0714','0722','0718','0728','0728','0728','0719','0716')").count()
    c1+c2
  }

  def get_visit_area(nbr:String,month:String): DataFrame = {
    val df1 = spark.read.parquet(path_mob.format(month, month)).select(
      "BILLING_NBR",
      "VISIT_AREA_CODE"
    )
    val df2 = spark.read.parquet(path_ocs.format(month, month)).selectExpr(
      "BILLING_NBR",
      "CALLING_PARTY_VISITED_CITY as VISIT_AREA_CODE"
    )
    df1.union(df2).where(s"BILLING_NBR like '%${nbr}'").select("VISIT_AREA_CODE").distinct()
  }

  def get_all_area(nbr:String,start:String): Unit ={
    DateUtil.month_range_now(start).map(get_visit_area(nbr,_)).reduce(_.union(_)).distinct().show(1000)
  }

  def is_hb(nbr:String,start:String): Long ={
    var cur = DateUtil.getCurrentMonth()
    var i = 0l
    while (cur >= start){
      val j = get_data(nbr,cur)
      i = i+j
      cur = DateUtil.next_month(cur,-1)
    }
    i
  }


  def backupModelData(spark:SparkSession): Unit = {
    val td = new BackUpModelData(spark)
    try{
      val month = "201802"
      println("*"*10+s"开始备份${month}数据"+"*"*10)
      td.getCdmaMonBase(month)
      td.getBroadbandMonBase(month)
      println("*"*10+s"成功备份${month}数据"+"*"*10)
    }catch {
      case e:Exception => e.printStackTrace();
    }
  }

  def generat_broadband_flux(spark:SparkSession): Unit ={
    val orc = new BackUpOracle(spark)
    for(i <- 201803 to 201806){
      try
        orc.getBroadBandFlux(i.toString)
      catch{
        case e:Exception => e.printStackTrace()
      }
    }
  }

  def generator_cdma_data(spark:SparkSession): Unit ={
    val lte = new LteModel(spark)
    println(lte.process("201806"))
  }

  def generator_broadband_data(spark:SparkSession): Unit ={
    val bd = new BroadBandModel(spark)
    println(bd.process("201806"))
  }

  def load_fastunfolding_vertics(spark:SparkSession): Unit ={
    def parse(pair:String):(String,String) ={
      val x = pair.split(",")
      val p1 = x(0).drop(1)
      val p2 = x(1).split(":")(1)
      (p2,p1)
    }
    def fullConnect(nbrs:Iterable[String]): Array[((String,String),Int)] ={
      val list = scala.collection.mutable.Queue[((String,String),Int)]()
      for(i<-nbrs){
        for(j<-nbrs){
          if(i!=j)
            list.enqueue(((i,j),1))
        }
      }
      list.toArray
    }
    val rdd = spark.read.textFile("juan/fastUnfolding/level_0_vertices").rdd.map(parse).groupByKey().flatMap{
      case (k,v) => fullConnect(v.toList:+k)
    }
    rdd.cache()
    val phone = spark.sparkContext.textFile("old_phone_nbr.csv").map(_.split("\t")).filter(_.length==2).map{
      case Array(a,b) => ((a,b),0)
    }
    phone.join(rdd).mapValues(t => t._1+t._2)
  }

  def main(): Unit ={
    val conf = new SparkConf().setAppName("lpa_juan")
      .set("spark.network.timeout","3600")
      .set("spark.executor.heartbeatInterval","3000")
    //      .setMaster("spark://133.0.6.96:7077")
    //      .set("spark.default.parallelism","300")
    //      .set("spark.executor.memory","4G")
    //      .set("spark.driver.memory","2G")
    val spark =SparkSession.builder().config(conf).getOrCreate()
    //spark.sparkContext.setLogLevel("WARN")
    try{
      val userPosition = new UserPosition(spark)
      userPosition.getUserPositionByMax("201806")
      //      val graphJuan = new GraphJuan(spark)
      //      val graph = graphJuan.juan2graph(LPA_ENSEMBLE_JUAN)
      //      new HDFSLouvainRunner(1,10,"juan/fastUnfolding").run(spark.sparkContext,graph)
    }finally {
      spark.close()
    }
  }

  def out(spark:SparkSession,args:Array[String]): Unit ={
    val Array(db_type, path, table) = args.take(3)
    val x = if(db_type.equalsIgnoreCase("TD"))
      new BackUpTD(spark)
    else
      new BackUpOracle(spark)
    val tmpPath = HDFSUtil.getTmpPath(path)
    if(args.length==3)
      x.simpleExportByCsv(tmpPath,table)
    else{
      val Array(col,start,end)=args.takeRight(3)
      x.exportByCsv(tmpPath,table,col,start.toInt,end.toInt)
    }
    HDFSUtil.downloadHDFSFile(tmpPath,path)
  }

  def main(args:Array[String]): Unit = {
    //-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005
    //"-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005"
    //    try{
    //      val sheet = args(0)
    //      val isFirst = args(1)
    //      if(isFirst=="0"){
    //        val seedsGenerator = new SeedsGenerator(spark)
    //        val  _table = s"TM_ZQ_$sheet"
    //        seedsGenerator.generate_seeds(_table,sheet)
    //      }
    //      new GraphRelationship(spark,sheet).process(isFirst)
    //    }finally {
    //      spark.close()
    //    }

  }
}