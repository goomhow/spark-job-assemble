package com.ctc

import com.ctc.util.AppUtils
import com.ctc.util.INSTANTCE._
import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DatasetHolder, Row}
import org.apache.spark.sql.functions.collect_set

object LpaFamilyJuan {
  case class X(a:String,b:String)
  val spark = AppUtils.createDefaultSession("es_family_juan")

  val graph = {
    val juan11 = spark.read.parquet("free_time_juan/201808_201811").filter(r => {
      val nbr = r.getString(0)
      val opp_nbr = r.getString(1)
      nbr.matches(MOB_NBR) && opp_nbr.matches(MOB_NBR)
    })
    val juan10 = spark.read.parquet("free_time_juan/201807_201810").filter(r => {
      val nbr = r.getString(0)
      val opp_nbr = r.getString(1)
      nbr.matches(MOB_NBR) && opp_nbr.matches(MOB_NBR)
    })

    val juan = juan10.unionByName(juan11)
      .groupBy("NBR","OPP_NBR")
      .sum("CNT")
      .toDF("NBR","OPP_NBR","CNT")
      .rdd
      .map{
        case Row(a:String,b:String,c:Long) => Edge(a.toLong,b.toLong,c.toInt)
      }
    Graph.fromEdges(juan,1L)
  }

  def hbSureJuan() ={
    /**
      * 紧捆绑家庭圈
      * */
    val hb_tight = spark.read.parquet("info/serv_t")
      .where("state = 'F0A' and serv_state <> 'F1R' and service_type = '/s/t/mob'")
      .select("product_offer_instance_id","acc_Nbr")
      .groupBy("product_offer_instance_id")
      .agg(collect_set("acc_Nbr").as("tight")).rdd.flatMap(r =>{
        val nbrs = r.getSeq[String](1).filter(_.matches(MOB_NBR))
        nbrs.find(_.matches(CTC_NBR)) match {
          case None => Seq()
          case Some(a:String) => {
            val index = a.toLong
            nbrs.map(n => (index,n.toLong))
          }
        }
      })
    /**
      * 松捆绑家庭圈
      * +---------------+-------------------+-----------------+
      * |        ACC_NBR|PARTITION_ID_REGION|COMBO_INSTANCE_ID|
      * +---------------+-------------------+-----------------+
      * |        3528525|               1003|     666004711000|
      * +---------------+-------------------+-----------------+
      * */

    val hb_combo = spark.read.parquet("haizheng/combo_hb")
      .withColumnRenamed("ACC_NBR","NBR")
      .groupBy("COMBO_INSTANCE_ID")
      .agg(collect_set("NBR").alias("loose"))
      .rdd.flatMap(r =>{
        val nbrs = r.getSeq[String](1).filter(_.matches(MOB_NBR))
        nbrs.find(_.matches(CTC_NBR)) match {
          case None => Seq()
          case Some(a:String) => {
            val index = a.toLong
            nbrs.map(n => (index,n.toLong))
          }
        }
      }
    )

    hb_tight.union(hb_combo).distinct(200)
  }

  def esSureJuan(): RDD[(VertexId,Long)] = {
    val loose_tight_juan = spark.read.parquet("tmp/loose_tight_juan")
    val loose_tight_rdd = loose_tight_juan.drop("PARTITION_ID_REGION").rdd
      .flatMap(r => {
        val nbr = r.getString(0)
        val juan = r.getSeq[String](1).toSet.dropWhile(_.equals(nbr))
        val tight = if(r.get(2)==null)Set[String]() else r.getSeq[String](2).toSet
        val loose = if(r.get(3)==null)Set[String]() else r.getSeq[String](3).toSet
        val sure:Set[String] = (tight | loose).dropWhile(_.equalsIgnoreCase(nbr))
        val attr = nbr.toLong
        sure.map{
          case a:String => (a.toLong,attr)
        }.toList:+(attr,attr)
      }).distinct()
    loose_tight_rdd
  }

  def evaluate(graph: Graph[Long,Int]): Map[Long,Int] = {
    val data = graph.vertices.map(v => (v._2,v._1)).groupByKey().mapValues(_.size).collectAsMap().toMap
    val familyCnt = data.size
    val nbrCnt = data.map(_._2).sum
    println(s"family cnt:${familyCnt}\ntotal nbr:${nbrCnt}\nmean menber:${nbrCnt.toDouble/familyCnt}")
    data
  }

  def main(args: Array[String]): Unit = {
    val attrs = hbSureJuan()
    val data = graph.outerJoinVertices(attrs){
      case (id,oAttr,Some(nAttr:Long)) => (id,nAttr)
      case (id,1L,None) => (id,id)
      case (id,oAttr,None) => (id,oAttr)
    }
    val rdd = LabelPropagation.run(data,3)
    val juan = rdd.aggregateMessages[Set[VertexId]](
      context => {
        if(context.srcAttr == context.dstAttr){
          context.sendToDst(Set(context.srcId))
        }
      },
      (m1,m2) => {
        m1 | m2
      },
      TripletFields.All
    )

    val e = evaluate(rdd)

  }
}
