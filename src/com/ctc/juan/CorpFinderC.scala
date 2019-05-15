package com.ctc.juan

import com.ctc.util.AppUtils

object CorpFinderC {
  val spark = AppUtils.createDefaultSession("lpa")


  import org.apache.spark.graphx._
  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql.functions.{udf,lit}
  import org.apache.spark.sql.types.{StringType, StructField, StructType,IntegerType}
  import org.apache.spark.sql.{DataFrame, Row}

  import scala.reflect.ClassTag
  import spark.implicits._

  val SEED_PATH = "corp_finder_seed.txt"
  val JUAN_PATH = "juan"
  val RESULT_TABLE = "lhz_pro2_role2.role_corp_finder"

  val UM_STRUCT = StructType(
    StructField("ACC_NBR", StringType, true) ::
      StructField("IS_OTHER_NET", StringType, true) ::
      StructField("CUST_NAME", StringType, true) ::
      StructField("NBR_AREA_CODE", StringType, true) ::
      StructField("NBR_AREA_NAME", StringType, true) :: Nil)

  val JUAN_STRUCT = StructType(StructField("NBR", StringType) ::
    StructField("OPP_NBR", StringType) ::
    StructField("CNT", IntegerType) :: Nil)

  case class Corp(areaCode:String,corpName:String)

  val NBR = "^\\D*(\\d+)\\D*$".r
  val MOB_NBR = "^1[2-9]\\d{9}$"
  val CTC_NBR = "^(133|153|170|173|177|180|181|189|199)\\d{8}$"
  val NBR_REG = "^0?\\d{6,12}$"
  val MOB_REG = "^0?0?(86)?1[3-9]\\d{9}$"

  val get_real_nbr = udf((area_code:String, nbr:String) => {
    val area = if(area_code != null) area_code else ""
    nbr match {
      case NBR(a) if a.matches(MOB_NBR) => nbr
      case NBR(a) if a.matches("^\\d{5,}$") => area_code + a
      case _ => null
    }
  })

  def getFormatSeeds(): RDD[(Long,Corp)] = {
    val df = spark.read.option("sep","\t").option("header","true").csv(SEED_PATH)
    df.select(df("AREA_CODE"),df("CUST_NAME"),get_real_nbr(df("AREA_CODE"),df("ACC_NBR"))).rdd.
      filter{
        case Row(area:String,corp:String,null) => false
        case Row(area:String,corp:String,nbr:String) => nbr!=null && nbr.matches("^\\d{8,}$")
      }.
      map{
        case Row(area:String,corp:String,nbr:String) => (nbr.toLong, Corp(area,corp))
      }
  }

  def juanGraph():Graph[String,Int] = {
    val df = spark.read.option("sep",",").option("header","false").schema(JUAN_STRUCT).csv(JUAN_PATH)
    val edge = EdgeRDD.fromEdges(
      df.rdd.filter{
        case Row(x:String,y:String,z:Int) => x.matches(NBR_REG) && y.matches(NBR_REG)
      }.map{
        case Row(x:String,y:String,z:Int) => Edge(x.toLong,y.toLong,z)
      }
    )
    Graph.fromEdges(edge,"")
  }

  def lpa[Corp:ClassTag, ED: ClassTag](graph: Graph[Corp, ED], maxSteps: Int): Graph[Corp, ED] = {
    require(maxSteps > 0, "迭代次数（maxSteps） 必须大于0")

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

  def getUdByLpa(maxIterations:Int = 3): DataFrame = {
      val juan = juanGraph()
      val seeds = getFormatSeeds()
      val data = juan.outerJoinVertices(seeds){
        case (a:Long,b:String,None) => Corp("",a.toString)
        case (a:Long,b:String,Some(c:Corp)) => c
      }
      val rdd = lpa(data,maxIterations)
        .vertices
        .filter{case (r:Long,corp:Corp) => r.toString.matches(MOB_NBR) && corp.areaCode!=""}
        .map{
          case (r:Long,corp:Corp) => {
            val nbr = r.toString
            val net = if(nbr.matches(CTC_NBR))"0" else "1"
            Row(nbr, net, corp.corpName, "", "")
          }
        }
      spark.createDataFrame(rdd,UM_STRUCT)
  }

  def persistToOracle(maxIterations:Int = 3): Unit = {
    val df = getUdByLpa(maxIterations)
    val ud = df.groupBy("CUST_NAME","IS_OTHER_NET").count()
    val hyxf = spark.read.option("sep","\t").option("header","true").csv(SEED_PATH).
      select("REGION_NAME","CUST_NAME").
      groupBy("CUST_NAME","REGION_NAME").
      count().
      withColumnRenamed("count","SEED_CNT")
    val b = ud.where("IS_OTHER_NET = 0").select("CUST_NAME","count").toDF("CUST_NAME","BCNT")
    val o = ud.where("IS_OTHER_NET = 1").select("CUST_NAME","count").toDF("CUST_NAME","OCNT")
    val r = b.join(o,Seq("CUST_NAME"),"outer").join(hyxf,Seq("CUST_NAME"),"left").na.fill(0)
    r.withColumn("SHENG_TOU_LV",r("BCNT") / (r("BCNT")+r("OCNT"))).
      withColumn("month_id",lit("201904")).
      write.
      mode("overwrite").
      saveAsTable(RESULT_TABLE)
  }


}