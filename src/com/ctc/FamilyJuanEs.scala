package com.ctc

import com.ctc.util.{AppUtils, HDFSUtil}
import com.ctc.util.INSTANTCE._
import org.apache.spark.sql.functions.{collect_set, mean, size}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.io.Source
import scala.util.parsing.json.JSON


object FamilyJuanEs {
  val spark = AppUtils.createDefaultSession("es_family_juan")
  case class X(NBR:String,OPP_NBR:String,CNT_CALL:Int,CNT_DAY:Int)
  case class BillingQuan(NBR:String,OPP_NBR:String)
  case class FlatQuan(index:String,NBR:String,OPP_NBR:String)
  case class Quan(nbr:String,callJuan:Set[String],offerJuan:Set[String]){
    def flat(): Seq[FlatQuan] = {
      callJuan.flatMap(call => {
        offerJuan.flatMap(o => Seq(FlatQuan(nbr,call,o),FlatQuan(nbr,o,call)))
      }).toSeq
    }
    def isNotEmputy(): Boolean = {
      callJuan.size>0 && offerJuan.size>0
    }
  }
  case class HcodeInfo(OPP_NBR:String,CITY_CODE:String,CITY_NAME:String,IS_OUR:String,COMPANY:String)

  val json = JSON.parseFull(Source.fromFile("hcode.json").mkString) match {
    case Some(x:Map[String,List[String]]) => x
  }
  val hcode = spark.sparkContext.broadcast(json).value
  val TM_ES_DAN_JUAN = "TM_ES_DAN_JUAN"
  val TM_ES_MER_JUAN = "TM_ES_MER_JUAN"
  val TM_ES_OLD_MEN_JUAN = "TM_ES_OLD_MEN_JUAN"

  def nbrHcodeInfo(table:String):Unit = {
    val path = s"tmp/es_juan/${table}.CSV"
    val df = spark.read.option("header","true").csv(path).repartition(100)
    val nbr = df.select("OPP_NBR").rdd.map{
      case Row(opp_nbr:String) => {
        val i = hcode.getOrElse(opp_nbr.take(7),Nil)
        val net = if(opp_nbr.matches(CTC_NBR))"YES" else "NO"
        if(i.size==3) {
          HcodeInfo(opp_nbr,"0" +i(0),i(1), net, i(2))
        }else{
          HcodeInfo(opp_nbr,"","", net, "")
        }
      }
    }
    val nbr_df = spark.createDataFrame(nbr)
    df.join(nbr_df,Seq("OPP_NBR"),"LEFT")
      .repartition(100)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"tmp/es_juan/${table}")
    spark.read.parquet(s"tmp/es_juan/${table}").write.mode(SaveMode.Overwrite).jdbc(anti_url,table,properties)
  }
  
  def getEsJuan():DataFrame={
    val es_mob = spark.read.parquet("info/serv_t")
      .where("state = 'F0A' and serv_state <> 'F1R' and service_type = '/s/t/mob' and PARTITION_ID_REGION = '1013'")
      .select("acc_Nbr")
      .toDF("NBR")
  //NBR 为恩施号码，OPP_NBR为恩施号码对应的freetime通讯圈
    val juan11 = spark.read.parquet("free_time_juan/201808_201811").select("NBR","OPP_NBR").filter(r => r match {
      case Row(nbr:String,oppnbr:String) =>nbr.matches(MOB_NBR) && oppnbr.matches(MOB_NBR)
    })
    val juan10 = spark.read.parquet("free_time_juan/201807_201810").select("NBR","OPP_NBR").filter(r => r match {
      case Row(nbr:String,oppnbr:String) =>nbr.matches(MOB_NBR) && oppnbr.matches(MOB_NBR)
    })
    val es_juan11 = es_mob.join(juan11.unionByName(juan11.toDF("OPP_NBR","NBR")).distinct(),Seq("NBR"),"left")
    val es_juan10 = es_mob.join(juan10.unionByName(juan10.toDF("OPP_NBR","NBR")).distinct(),Seq("NBR"),"left")
    es_juan10.unionByName(es_juan11).distinct()
  }

  def familyJuanCert(): Unit ={
    val es_serv_s = spark.read.parquet("info/serv_subscriber").select("certificate_no","certificate_type","serv_id","state")
      .where("state='00A' and certificate_type = '2BA' and PARTITION_ID_REGION = '1013'").select("certificate_no","serv_id")
    val es_serv_t = spark.read.parquet("info/serv_t")
      .where("state = 'F0A' and serv_state <> 'F1R' and service_type = '/s/t/mob' and PARTITION_ID_REGION = '1013'")
      .select("product_offer_instance_id","serv_id","acc_Nbr")
      .withColumnRenamed("acc_Nbr","NBR")
    es_serv_t.cache()
    val es_mob_offer = es_serv_t.select("product_offer_instance_id","NBR")
    val es_mob_cert = es_serv_t.join(es_serv_s,Seq("serv_id"),"left").select("certificate_no","NBR")
    val es_mob = es_mob_offer.select("NBR")
    //NBR 为恩施号码，OPP_NBR为恩施号码对应的freetime通讯圈
    val es_juan = {
      val juan11 = spark.read.parquet("free_time_juan/201808_201811").select("NBR","OPP_NBR")
      val juan10 = spark.read.parquet("free_time_juan/201807_201810").select("NBR","OPP_NBR")
      val es_juan11 = es_mob.join(juan11.unionByName(juan11.toDF("OPP_NBR","NBR")).distinct(),Seq("NBR"),"left")
      val es_juan10 = es_mob.join(juan10.unionByName(juan10.toDF("OPP_NBR","NBR")).distinct(),Seq("NBR"),"left")
      es_juan10.unionByName(es_juan11).distinct()
    }
    es_juan.cache()

    val es_juan_set = es_juan.groupBy("NBR").agg(collect_set("OPP_NBR").as("OPP_NBR_SET"))

    /**
      *
      **/
    val cert_juan = es_mob_cert.join(spark.read.parquet("juan/cert"),Seq("certificate_no"),"inner").drop("certificate_no")

    /**
      * 紧捆绑家庭圈
      * */
    val es_tight = es_mob_offer.groupBy("product_offer_instance_id").agg(collect_set("NBR").as("tight"))
    val tight_juan = es_juan_set
      .join(cert_juan,Seq("NBR"),"left")
      .join(es_mob_offer,Seq("NBR"),"left")
      .join(es_tight,Seq("product_offer_instance_id"),"left")
      .drop("product_offer_instance_id")
    /**
      * 松捆绑家庭圈
      * +---------------+-------------------+-----------------+
      * |        ACC_NBR|PARTITION_ID_REGION|COMBO_INSTANCE_ID|
      * +---------------+-------------------+-----------------+
      * |        3528525|               1003|     666004711000|
      * +---------------+-------------------+-----------------+
      * */

    val combo_hb = spark.read.parquet("haizheng/combo_hb").withColumnRenamed("ACC_NBR","NBR").filter(r => {
      r.getString(0).matches(MOB_NBR) && 1013 == r.getDecimal(1).toBigInteger.intValue()
    })
    val es_loose = combo_hb.groupBy("COMBO_INSTANCE_ID").agg(collect_set("NBR").alias("loose"))
    val loose_tight_juan = tight_juan
      .join(combo_hb,Seq("NBR"),"left")
      .join(es_loose,Seq("COMBO_INSTANCE_ID"),"left")
      .drop("COMBO_INSTANCE_ID")
    loose_tight_juan.cache()
    loose_tight_juan.write.mode("overwrite").parquet("tmp/loose_tight_juan")

    es_mob_offer.unpersist()

    val loose_tight_rdd = loose_tight_juan.drop("PARTITION_ID_REGION").rdd
      .map(r => {
        val nbr = r.getString(0)
        val juan = r.getSeq[String](1).toSet.dropWhile(_.equals(nbr))

        val cert = if(r.get(2)==null)Set[String]() else r.getSeq[String](2).toSet
        val tight = if(r.get(3)==null)Set[String]() else r.getSeq[String](3).toSet
        val loose = if(r.get(4)==null)Set[String]() else r.getSeq[String](4).toSet
        val sure = cert.union(tight).union(loose).dropWhile(_.equals(nbr))
        Quan(nbr,juan.diff(sure),sure)
      })
      .filter(_.isNotEmputy)
      .flatMap(_.flat)
    val data = spark.createDataFrame(loose_tight_rdd).join(es_juan.unionByName(es_juan.toDF("OPP_NBR","NBR")),Seq("NBR","OPP_NBR"),"inner").rdd.flatMap{
      case Row(a:String,b:String,c:String) => {
        Seq(BillingQuan(a,b),BillingQuan(a,c))
      }
    }
    spark.createDataFrame(data)
      .distinct()
      .filter(r => {
        val a= r.getString(0)
        val b = r.getString(1)
        (!a.equalsIgnoreCase(b)) && a.matches(CTC_NBR) && (!b.matches(CTC_NBR))
      })
      .write
      .mode("overwrite")
      .parquet("es/juan_0102")
  }

  def familyJuan(): Unit = {
    val es_serv_t = spark.read.parquet("info/serv_t")
      .where("state = 'F0A' and serv_state <> 'F1R' and service_type = '/s/t/mob' and PARTITION_ID_REGION = '1013'")
      .select("product_offer_instance_id","serv_id","acc_Nbr")
      .withColumnRenamed("acc_Nbr","NBR")
    es_serv_t.cache()
    val es_mob_offer = es_serv_t.select("product_offer_instance_id","NBR")
    val es_mob = es_mob_offer.select("NBR")
    //NBR 为恩施号码，OPP_NBR为恩施号码对应的freetime通讯圈
    val es_juan = {
      val juan11 = spark.read.parquet("free_time_juan/201808_201811").select("NBR","OPP_NBR")
      val juan10 = spark.read.parquet("free_time_juan/201807_201810").select("NBR","OPP_NBR")
      val es_juan11 = es_mob.join(juan11.unionByName(juan11.toDF("OPP_NBR","NBR")).distinct(),Seq("NBR"),"left")
      val es_juan10 = es_mob.join(juan10.unionByName(juan10.toDF("OPP_NBR","NBR")).distinct(),Seq("NBR"),"left")
      es_juan10.unionByName(es_juan11).distinct()
    }
    es_juan.cache()

    val es_juan_set = es_juan.groupBy("NBR").agg(collect_set("OPP_NBR").as("OPP_NBR_SET"))

    /**
      * 紧捆绑家庭圈
      * */
    val es_tight = es_mob_offer.groupBy("product_offer_instance_id").agg(collect_set("NBR").as("tight"))
    val tight_juan = es_juan_set
      .join(es_mob_offer,Seq("NBR"),"left")
      .join(es_tight,Seq("product_offer_instance_id"),"left")
      .drop("product_offer_instance_id")
    /**
      * 松捆绑家庭圈
      * +---------------+-------------------+-----------------+
      * |        ACC_NBR|PARTITION_ID_REGION|COMBO_INSTANCE_ID|
      * +---------------+-------------------+-----------------+
      * |        3528525|               1003|     666004711000|
      * +---------------+-------------------+-----------------+
      * */

    val combo_hb = spark.read.parquet("haizheng/combo_hb").withColumnRenamed("ACC_NBR","NBR").filter(r => {
      r.getString(0).matches(MOB_NBR) && 1013 == r.getDecimal(1).toBigInteger.intValue()
    })
    val es_loose = combo_hb.groupBy("COMBO_INSTANCE_ID").agg(collect_set("NBR").alias("loose"))
    val loose_tight_juan = tight_juan
      .join(combo_hb,Seq("NBR"),"left")
      .join(es_loose,Seq("COMBO_INSTANCE_ID"),"left")
      .drop("COMBO_INSTANCE_ID")
    loose_tight_juan.cache()
    loose_tight_juan.write.mode("overwrite").parquet("tmp/loose_tight_juan")

    es_mob_offer.unpersist()

    val loose_tight_rdd = loose_tight_juan.drop("PARTITION_ID_REGION").rdd
      .map(r => {
        val nbr = r.getString(0)
        val juan = r.getSeq[String](1).toSet.dropWhile(_.equals(nbr))
        val tight = if(r.get(2)==null)Set[String]() else r.getSeq[String](2).toSet
        val loose = if(r.get(3)==null)Set[String]() else r.getSeq[String](3).toSet
        val sure = (tight | loose).dropWhile(_.equalsIgnoreCase(nbr))
        Quan(nbr,juan.diff(sure),sure)
      })

    val merge_rdd = loose_tight_rdd.flatMap{
      case Quan(a,b,c) => c.map(BillingQuan(a,_))
    }

    val data = spark.createDataFrame(loose_tight_rdd.filter(_.isNotEmputy).flatMap(_.flat))
      .join(es_juan,Seq("NBR","OPP_NBR"),"inner")
      .rdd
      .flatMap(r => {
        val index = r.getAs[String]("index")
        val a = r.getAs[String]("NBR")
        val b = r.getAs[String]("OPP_NBR")
        Seq(BillingQuan(index,a),BillingQuan(index,b))
      }).union(merge_rdd).distinct()

    spark.createDataFrame(data)
      .distinct()
      .filter(r => {
        val a= r.getString(0)
        val b = r.getString(1)
        (!a.equalsIgnoreCase(b)) && a.matches(MOB_NBR) && b.matches(MOB_NBR)
      })
      .write
      .mode("overwrite")
      .parquet("es/juan_0102_2")
  }

  def familyDanC(): Unit ={
    val es_serv_t = spark.read.parquet("info/serv_t")
      .where("state = 'F0A' and serv_state <> 'F1R' and service_type = '/s/t/mob' and PARTITION_ID_REGION = '1013'")
      .select("product_offer_instance_id","serv_id","acc_Nbr")
      .withColumnRenamed("acc_Nbr","NBR")
    es_serv_t.cache()
    val es_mob_offer = es_serv_t.select("product_offer_instance_id","NBR")
    val es_mob = es_mob_offer.select("NBR")
    //NBR 为恩施号码，OPP_NBR为恩施号码对应的freetime通讯圈
    val es_juan = {
      val juan11 = spark.read.parquet("free_time_juan/201808_201811").select("NBR","OPP_NBR")
      val juan10 = spark.read.parquet("free_time_juan/201807_201810").select("NBR","OPP_NBR")
      val es_juan11 = es_mob.join(juan11.unionByName(juan11.toDF("OPP_NBR","NBR")).distinct(),Seq("NBR"),"left")
      val es_juan10 = es_mob.join(juan10.unionByName(juan10.toDF("OPP_NBR","NBR")).distinct(),Seq("NBR"),"left")
      es_juan10.unionByName(es_juan11).distinct()
    }
    val merge_nbr = spark.sparkContext.broadcast(
      spark.read.parquet("es/juan_0102_2").select("NBR").distinct().collect().map(_.getString(0)).toSet
    ).value
    val dan = es_juan.filter(r => !merge_nbr.contains(r.getAs[String]("NBR")))
    val action = spark.read.parquet("20180701_20181231")
    val r = dan.join(action,Seq("NBR","OPP_NBR"),"inner")
  }

  def familyMergeOffer(): DataFrame = {
    val es_serv_t = spark.read.parquet("info/serv_t")
      .where("state = 'F0A' and serv_state <> 'F1R' and service_type = '/s/t/mob' and PARTITION_ID_REGION = '1013'")
      .select("product_offer_instance_id","serv_id","acc_Nbr")
      .withColumnRenamed("acc_Nbr","NBR")
    val es_mob_offer = es_serv_t.select("product_offer_instance_id","NBR")
    val es_mob = es_mob_offer.select("NBR")
    /**
      * 紧捆绑家庭圈
      * */
    val es_tight = es_mob_offer.groupBy("product_offer_instance_id").agg(collect_set("NBR").as("tight"))
    val tight_juan = es_mob_offer
      .join(es_tight,Seq("product_offer_instance_id"),"left")
      .drop("product_offer_instance_id")
    /**
      * 松捆绑家庭圈
      * +---------------+-------------------+-----------------+
      * |        ACC_NBR|PARTITION_ID_REGION|COMBO_INSTANCE_ID|
      * +---------------+-------------------+-----------------+
      * |        3528525|               1003|     666004711000|
      * +---------------+-------------------+-----------------+
      * */

    val combo_hb = spark.read.parquet("haizheng/combo_hb").withColumnRenamed("ACC_NBR","NBR").filter(r => {
      r.getString(0).matches(MOB_NBR) && 1013 == r.getDecimal(1).toBigInteger.intValue()
    })
    val es_loose = combo_hb.groupBy("COMBO_INSTANCE_ID").agg(collect_set("NBR").alias("loose"))
    val loose_tight_juan = tight_juan
      .join(combo_hb,Seq("NBR"),"left")
      .join(es_loose,Seq("COMBO_INSTANCE_ID"),"left")
      .drop("COMBO_INSTANCE_ID")
    loose_tight_juan.cache()
    loose_tight_juan.write.mode("overwrite").parquet("tmp/loose_tight_juan")

    val loose_tight_rdd = loose_tight_juan.drop("PARTITION_ID_REGION").rdd
      .flatMap(r => {
        val nbr = r.getString(0)
        val tight = if(r.get(1)==null)Set[String]() else r.getSeq[String](1).toSet
        val loose = if(r.get(2)==null)Set[String]() else r.getSeq[String](2).toSet
        val sure = (tight | loose).dropWhile(_.equalsIgnoreCase(nbr))
        sure.map(BillingQuan(nbr,_))
      })
    val sure = spark.createDataFrame(loose_tight_rdd)
    val billingAction = spark.read.parquet("20180701_20181231")
    val r = sure.join(billingAction.unionByName(billingAction.toDF("OPP_NBR","NBR","CNT_CALL","CNT_DAY")),Seq("NBR","OPP_NBR"),"left")
    r
  }

  def imei(file:String="es/haizheng/es_term_pair.txt"): DataFrame = {
    val imei = spark.read.option("sep",",").option("header","false").schema(
      StructType(Seq(StructField("NBR",StringType),StructField("OPP_NBR",StringType)))
    ).csv(file)
    val billingAction = spark.read.parquet("20180701_20181231")
    val data = imei.join(billingAction,Seq("NBR","OPP_NBR"),"inner").unionByName(
      imei.join(billingAction.toDF("OPP_NBR","NBR","CNT_CALL","CNT_DAY"),Seq("NBR","OPP_NBR"),"inner")
    ).distinct().rdd.map{
      case Row(nbr:String,oppNbr:String,call:Int,day:Int) =>(nbr,(oppNbr,call,day))
    }.groupByKey().flatMap{
      case (nbr,opps) => opps.toList.sortWith((a,b)=>a._2>b._2).take(5).map(r =>X(nbr,r._1,r._2,r._3))
    }
    val df = spark.createDataFrame(data)
    val save = "es/imei_juan"
    df.write.mode("overwrite").parquet(save)
    df
  }

  def oldMan(file:String="es/haizheng/oldman_comm_habit.csv"): DataFrame = {
    val main = spark.read.option("sep",",").option("header","false").csv(file)
      .toDF("NBR","NBR_CNT","BJ","ZJ","MONTH")
      .groupBy("NBR")
      .agg(
        mean("NBR_CNT").as("NBR_CNT"),
        mean("BJ").as("BJ"),
        mean("ZJ").as("ZJ")
      )
      .where("NBR_CNT <= 10")
    val nbr = main.select("NBR")
    val juan = getEsJuan()
    val billingAction = spark.read.parquet("20180701_20181231").join(juan,Seq("NBR","OPP_NBR"),"INNER")
    val right = nbr.join(billingAction,Seq("NBR"),"left")
    val left = right.where("OPP_NBR IS NULL").select("NBR").join(
      billingAction.toDF("OPP_NBR","NBR","CNT_CALL","CNT_DAY"),Seq("NBR"),"inner"
    )
    val data = right.where("OPP_NBR IS NOT NULL").unionByName(left).distinct().rdd.map{
      case Row(nbr:String,oppNbr:String,call:Int,day:Int) =>(nbr,(oppNbr,call,day))
    }.groupByKey().flatMap{
      case (index,opps) => opps.toList.sortWith((a,b)=>a._2>b._2).take(5).map(r =>X(index,r._1,r._2,r._3))
    }
    val old_man = main.join(spark.createDataFrame(data),Seq("NBR"),"INNER")
    old_man.repartition(200).write.mode("overwrite").parquet("es/oldman")
    old_man
  }

  def main(args: Array[String]): Unit = {
    familyJuan()
  }

}