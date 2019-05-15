package com.ctc

import com.ctc.util.AppUtils
import com.ctc.util.INSTANTCE._
import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object GraphFamilyJuan {
  val spark = AppUtils.createDefaultSession("es_family_juan")
  val es_serv_t = spark.read.parquet("info/serv_t")
    .where("state = 'F0A' and serv_state <> 'F1R' and service_type = '/s/t/mob' and PARTITION_ID_REGION = '1013'")
    .select("product_offer_instance_id","serv_id","acc_Nbr")
    .withColumnRenamed("acc_Nbr","NBR")
    .filter(r => r.getAs[String]("NBR").matches(MOB_NBR))
  es_serv_t.cache()
  val es_mob_offer = es_serv_t.select("product_offer_instance_id","NBR")
  val es_mob = es_mob_offer.select("NBR")
  //NBR 为恩施号码，OPP_NBR为恩施号码对应的freetime通讯圈
  val esGraph = {
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
        case Row(a:String,b:String,c:Long) => Edge(a.toLong,b.toLong,c)
      }
    Graph.fromEdges(juan,1)
  }

  def getSureJuan(): RDD[(VertexId,Long)] ={
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

  def main(args: Array[String]): Unit = {
    val attrs = getSureJuan()
    val data = esGraph.outerJoinVertices(attrs){
      case (id,oAttr,Some(nAttr:Long)) => (id,nAttr)
      case (id,0,None) => (id,id)
      case (id,oAttr,None) => (id,oAttr)
    }
    val rdd = LabelPropagation.run(esGraph,3)
  }
}
