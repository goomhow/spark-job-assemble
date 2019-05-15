package com.ctc.juan

import com.ctc.util.HDFSUtil
import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.graphx._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.sum
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.rdd.RDD
import com.ctc.juan.Juan._
import com.ctc.util.INSTANTCE.juan_struct
import com.ctc.util.DateUtil.{getCurrentMonth, next_month}

import scala.annotation.meta.param

class GraphJuan (@transient @param spark:SparkSession) extends Serializable{

  val vipJuan = new VipJuan(spark)
  val userJuan = new UserInfoJuan(spark)
  /** 根据工作圈生成用户关系图
    * @param name 工作圈存放路径
    **/
  def parseWorkJuanToGraph(name:String=null):Graph[String,Int] = {
    val fname = if(name==null){
      val current = getCurrentMonth()
      val end = next_month(current,-1)
      val start = next_month(end,-3)
      s"workday_juan/${start}_${end}"
    }else{
      name
    }
    println("*"*10+"READ WORK JUAN"+"*"*10)
    println(s"juan_path:${fname}")
    if(!HDFSUtil.exists(fname)){
      println(s"${fname} does not exists,we are going to create in next few munitues!")
      val Array(start,end)=fname.split("/")(1).split("_")
      vipJuan.generateWorkdayJuanByEnd(next_month(end,1))
    }
    val df = spark.read.parquet(fname)
    val edge = EdgeRDD.fromEdges(
      df.rdd.filter{
        case Row(x:String,y:String,z:Int) => x.matches(VipJuan.NBR_REG) && y.matches(VipJuan.NBR_REG)
      }.map{
        case Row(x:String,y:String,z:Int) => Edge(x.toLong,y.toLong,z)
      }
    )
    Graph.fromEdges(edge,"")
  }

  def resetLpaJuan(): Unit = {
    vipJuan.removeUnionDirs()
    HDFSUtil.removeDir(LPA_HOLIDAY_JUAN.path)
    lpaHolidayGrapha()
    HDFSUtil.removeDir(LPA_WORK_JUAN.path)
    lpaWorkGrapha()
    HDFSUtil.removeDir(LPA_FREE_JUAN.path)
    lpaFreeGrapha()
  }

  def juan2graph(juan:Juan):Graph[String,Long] = {
    val edge:RDD[Edge[Long]] =  (juan match {
      case LPA_WORK_JUAN => vipJuan.getUnionWorkdayData().rdd.filter{case Row(x:String, y:String,_) => x.matches(VipJuan.NBR_REG) && y.matches(VipJuan.NBR_REG)}
      case LPA_FREE_JUAN => vipJuan.getUnionFreeTimeData().rdd.filter{case Row(x:String, y:String,_) => x.matches(VipJuan.MOB_REG) && y.matches(VipJuan.MOB_REG)}
      case LPA_HOLIDAY_JUAN => vipJuan.getUnionHolidayData().rdd.filter{case Row(x:String, y:String,_) => x.matches(VipJuan.MOB_REG) && y.matches(VipJuan.MOB_REG)}
      case LPA_ENSEMBLE_JUAN => ensembleUnion().rdd.filter{case Row(x:String, y:String,_) => x.matches(VipJuan.MOB_REG) && y.matches(VipJuan.MOB_REG)}
    }).map{
      case Row(x:String,y:String,z:Int) => Edge(x.toLong,y.toLong,z.toLong)
      case Row(x:String,y:String,z:Long) => Edge(x.toLong,y.toLong,z)
    }
    Graph.fromEdges(edge,"")
  }

  private[this] def lpaJuan(juan:Juan):Graph[Long,Long] = {
    if(!HDFSUtil.exists(juan.path)){
      LabelPropagation.run(juan2graph(juan),5).triplets.saveAsObjectFile(juan.path)
    }
    val edgeTriple:RDD[EdgeTriplet[Long,Long]] = spark.sparkContext.objectFile(juan.path)
    val edge:EdgeRDD[Long] = EdgeRDD.fromEdges(edgeTriple.mapPartitions(
      _.map(t => Edge(t.srcId,t.dstId,t.attr))
    ))
    val vertexRDD:VertexRDD[Long] = VertexRDD(edgeTriple.mapPartitions(
      _.flatMap(t => {
        val x = t.toTuple
        Array(x._1,x._2)
      })
    ))
    val graph = GraphImpl(vertexRDD,edge).subgraph()
    graph
  }

  def lpaWorkGrapha() = lpaJuan(LPA_WORK_JUAN)
  def lpaFreeGrapha() = lpaJuan(LPA_FREE_JUAN)
  def lpaHolidayGrapha() = lpaJuan(LPA_HOLIDAY_JUAN)
  def lpaEnsembleGrapha() = lpaJuan(LPA_ENSEMBLE_JUAN)

  def ensembleUnion() = {
    val path = VipJuan.ENSEMBLE_UNION_PATH
    if(!HDFSUtil.exists(path)){
      val free_union = vipJuan.getUnionFreeTimeData()
      val product_juan = userJuan.ensembleJuan()
      free_union.unionByName(product_juan)
        .groupBy(juan_struct(0).name,juan_struct(1).name)
        .agg(sum("CNT").as("CNT"))
        .write
        .mode(SaveMode.Overwrite)
        .parquet(path)
    }
    spark.read.parquet(path)
  }
}
