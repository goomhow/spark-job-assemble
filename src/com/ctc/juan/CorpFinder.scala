package com.ctc.juan

import com.ctc.juan.CorpFinder._
import com.ctc.util.{AppUtils, HDFSUtil}
import com.ctc.util.INSTANTCE._
import org.apache.spark.graphx.{EdgeTriplet, Graph, Pregel, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.annotation.meta.param
import scala.io.Source
import scala.reflect.ClassTag
import scala.util.parsing.json.JSON

class CorpFinder (@transient @param spark:SparkSession) extends Serializable{
  val NBR = "^\\D*(\\d+)\\D*$".r
  val json = JSON.parseFull(Source.fromFile("hcode.json").mkString) match {
    case Some(x:Map[String,List[String]]) => x
  }
  val hcode = spark.sparkContext.broadcast(json).value
  val get_real_nbr = udf((area_code:String, nbr:String) => {
    val area = if(area_code != null) area_code else ""
    nbr match {
      case NBR(a) if a.matches(MOB_NBR) => nbr
      case NBR(a) if a.matches("^\\d{5,}$") => area_code + a
      case _ => null
    }
  })

  def persistToOracle(sheet:String,maxIterations:Int = 3): Unit = {
    val ud = getUdByLpa(sheet,maxIterations).groupBy("CUST_NAME","IS_OTHER_NET").count()
    val hyxf = getHyxf(sheet).select("REGION_NAME","CUST_NAME").groupBy("CUST_NAME","REGION_NAME").count().withColumnRenamed("count","seedCnt")
    val b = ud.where("IS_OTHER_NET = 0").select("CUST_NAME","count").toDF("CUST_NAME","bcnt")
    val o = ud.where("IS_OTHER_NET = 1").select("CUST_NAME","count").toDF("CUST_NAME","ocnt")
    val r = b.join(o,Seq("CUST_NAME"),"outer").join(hyxf,Seq("CUST_NAME"),"left").na.fill(0)
    r.withColumn("sheng_tou_lv",r("bcnt") / (r("bcnt")+r("ocnt")))
      .write
      .mode("overwrite")
      .jdbc(anti_url,"TMP_STATIC_ZQ",properties)
  }

  def getNewestJuan():Graph[String,Int]={
    if(HDFSUtil.exists(VipJuan.WORKDAY_UNION_PATH)){
      if(! HDFSUtil.isNewOnMonth(VipJuan.WORKDAY_UNION_PATH))
        HDFSUtil.delete(VipJuan.WORKDAY_UNION_PATH,true)
    }
    new GraphJuan(spark).parseWorkJuanToGraph(VipJuan.WORKDAY_UNION_PATH)
  }

  def getUdByLpa(sheet:String,maxIterations:Int = 3): DataFrame = {
    val path = TM_UD_MODEL.format(sheet,maxIterations)
    if(!HDFSUtil.exists(path)){
      val juan = new GraphJuan(spark).parseWorkJuanToGraph("workday_juan/201806_201809")
      val data = juan.outerJoinVertices(getFormatSeeds(sheet)){
        case (a:Long,b:String,None) => Corp("",a.toString)
        case (a:Long,b:String,Some(c:Corp)) => c
      }
      val rdd = lpa(data,maxIterations)
        .vertices
        .filter{case (r:Long,corp:Corp) => r.toString.matches(MOB_NBR) && corp.areaCode!=""}
        .map{
          case (r:Long,corp:Corp) => {
            val nbr = r.toString
            val i = hcode.getOrElse(nbr.take(7),Nil)
            val net = if(nbr.matches(CTC_NBR))"0" else "1"
            if(i.size==3) {
              Row(nbr, net, corp.corpName, "0" + i.head, i(1))
            }else{
              Row(nbr, net, corp.corpName, "", "")
            }
          }
        }
      spark.createDataFrame(rdd,UM_STRUCT).write.mode("overwrite").parquet(path)
    }
    spark.read.parquet(path)
  }

  def getHyxf(sheet:String): DataFrame ={
    val path = TC_TMP_HYXF_T.format(sheet)
    val table = s"TM_ZQ_${sheet}"
    if(!HDFSUtil.exists(path)){
      spark.read.jdbc(anti_url,table,properties).createTempView(table)
      spark.read.parquet("info/serv_t").createTempView("SERV_T")
      spark.read.parquet("info/cust_t").createTempView("CUST_T")
      spark.sql(
        s"""
           |SELECT '$sheet' AS SHEET_NBR,C.AREA_NAME AS REGION_NAME,C.CUST_NAME,A.REGION_ID,A.ACC_NBR,A.SERVICE_TYPE,A.SERV_ID,A.AREA_CODE
           |FROM  SERV_T A,CUST_T B ,$table C
           |WHERE A.CUST_ID=B.CUST_ID
           |AND B.CUST_NAME = C.CUST_NAME
           |AND B.STATE='70A'
           |AND A.STATE='F0A'
           |AND A.SERV_STATE<>'F1R'
           |AND A.SERVICE_TYPE IN ('/s/t/fix','/s/t/mob')
      """.stripMargin).write.mode("overwrite").parquet(path)
    }
    spark.read.parquet(path)
  }

  def getFormatSeeds(sheet:String): RDD[(Long,Corp)] = {
    val df = getHyxf(sheet)
    df.select(df("AREA_CODE"),df("CUST_NAME"),get_real_nbr(df("AREA_CODE"),df("ACC_NBR")))
      .rdd
      .filter{
        case Row(area:String,corp:String,null) => false
        case Row(area:String,corp:String,nbr:String) => nbr!=null && nbr.matches("^\\d{8,}$")
      }
      .map{
        case Row(area:String,corp:String,nbr:String) => (nbr.toLong, Corp(area,corp))
      }
  }

  def predictAll(juan_path:String="workday_juan/201812_201903"): Unit ={
    val sheet = "all"
    val maxIter = 5
    spark.read.parquet("info/serv_t").createTempView("SERV_T")
    spark.read.parquet("info/cust_t").createTempView("CUST_T")
    spark.read.parquet("info/serv_subscriber").createTempView("CUST_S")
    spark.sql("""SELECT 'ALL' AS SHEET_NBR,'' AS REGION_NAME,B.CUST_NAME,A.REGION_ID,A.ACC_NBR,A.SERVICE_TYPE,A.SERV_ID,A.AREA_CODE
          FROM  SERV_T A,CUST_T B,CUST_S C
          WHERE A.CUST_ID=B.CUST_ID
          AND A.CUST_ID=C.CUST_ID
          AND C.CERTIFICATE_TYPE != '2BA'
          AND length(B.CUST_NAME) > 4
          AND B.STATE='70A'
          AND A.STATE='F0A'
          AND A.SERV_STATE<>'F1R'
          AND A.SERVICE_TYPE IN ('/s/t/fix','/s/t/mob')""".stripMargin).
      write.mode("overwrite").parquet(TC_TMP_HYXF_T.format(sheet))
    val seeds = spark.read.parquet(TC_TMP_HYXF_T.format(sheet)).
      select(col("AREA_CODE"),
        col("CUST_NAME"),
        get_real_nbr(col("AREA_CODE"),
        col("ACC_NBR"))
      ).rdd.filter{
        case Row(area:String,corp:String,null) => false
        case Row(area:String,corp:String,nbr:String) => nbr!=null && nbr.matches("^\\d{8,}$")
      }.map{
        case Row(area:String,corp:String,nbr:String) => (nbr.toLong, Corp(area,corp))
      }
    val juan = new GraphJuan(spark).parseWorkJuanToGraph(juan_path)
    val data = juan.outerJoinVertices(seeds){
      case (a:Long,b:String,None) => Corp("",a.toString)
      case (a:Long,b:String,Some(c:Corp)) => c
    }
    val rdd = lpa(data,maxIter)
      .vertices
      .filter{case (r:Long,corp:Corp) => r.toString.matches(MOB_NBR) && corp.areaCode!=""}
      .map{
        case (r:Long,corp:Corp) => {
          val nbr = r.toString
          val i = hcode.getOrElse(nbr.take(7),Nil)
          val net = if(nbr.matches(CTC_NBR))"0" else "1"
          if(i.size==3) {
            Row(nbr, net, corp.corpName, "0" + i.head, i(1))
          }else{
            Row(nbr, net, corp.corpName, "", "")
          }
        }
      }
    spark.createDataFrame(rdd,UM_STRUCT).write.mode("overwrite").parquet(TM_UD_MODEL.format(sheet,maxIter))
  }
}

object CorpFinder {
  val TC_TMP_HYXF_T = "info/tc_tmp_hyxf_t/%s"
  val TM_UD_MODEL = s"info/tm_ud_model/%s/%s"

  case class Corp(areaCode:String,corpName:String)

  def apply(@transient @param spark: SparkSession): CorpFinder = new CorpFinder(spark)

  def lpa[Corp:ClassTag, ED: ClassTag](graph: Graph[Corp, ED], maxSteps: Int): Graph[Corp, ED] = {
    require(maxSteps > 0, s"Maximum of steps must be greater than 0, but got ${maxSteps}")

    def sendMessage(e: EdgeTriplet[Corp, ED]): Iterator[(VertexId, Map[Corp, Long])] = {
      Iterator((e.srcId, Map(e.dstAttr -> 1L)), (e.dstId, Map(e.srcAttr -> 1L)))
    }
    def mergeMessage(count1: Map[Corp, Long], count2: Map[Corp, Long]): Map[Corp, Long] = {
      (count1.keySet ++ count2.keySet).map { i =>
        val count1Val = count1.getOrElse(i, 0L)
        val count2Val = count2.getOrElse(i, 0L)
        i -> (count1Val + count2Val)
      }(collection.breakOut)
    }
    def vertexProgram(vid: VertexId, attr: Corp, message: Map[Corp, Long]): Corp = {
      if (message.isEmpty) attr else message.maxBy(_._2)._1
    }
    val initialMessage = Map[Corp, Long]()
    Pregel(graph, initialMessage, maxIterations = maxSteps)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage
    )
  }

  def main(args: Array[String]): Unit = {
    val spark = AppUtils.createDefaultSession("lap_juan")
    val finder = new CorpFinder(spark)
    if(args.length == 1)
      finder.predictAll(args(0))
    else
      finder.predictAll()
  }
}