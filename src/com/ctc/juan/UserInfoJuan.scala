package com.ctc.juan

import com.ctc.backup.BackUpOracle
import org.apache.spark.sql.functions.{collect_set, size, udf}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.WrappedArray
import com.ctc.util.HDFSUtil._
import com.ctc.util.INSTANTCE.juan_struct
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.types._
import com.ctc.juan.Juan._
import com.ctc.juan.UserInfoJuan.SERV_T_STRUCT

import scala.annotation.meta.param
class UserInfoJuan (@transient @param spark:SparkSession) extends Serializable{

  /**
    *  ---宽带联系信息
 select cust_relation_info from ls65_sid.serv_subscriber_t@to_sc_sid where serv_id =
 (select b.serv_id from ls65_sid.serv_t@to_sc_sid b where b.ACC_NBR='15392960172@ADSL' and b.state='F0A')
 and state='00A';

  --宽带同客户下的手机
  select TO_CHAR(wm_concat(acc_Nbr)) from ls65_sid.serv_t@to_sc_sid a where a.cust_id in (
  select b.cust_id from ls65_sid.serv_t@to_sc_sid b where b.ACC_NBR='15392960172@ADSL' and b.state='F0A')
  and a.state='F0A' and a.product_id=10501110
    * */

  def get_mdn_contact_nbr(df:DataFrame):DataFrame={

    def join_way = udf((x:String,y:String) => {
      if(x.matches("^\\d+$")){
        y.contains("@")&&x.equalsIgnoreCase(y.split("@")(0))
      }else{
        x.equalsIgnoreCase(y)
      }
    })

    val mdn_info = get_mdn_info().select("ACC_NBR")
    df.join(mdn_info,join_way(df("MDN"),mdn_info("ACC_NBR")),"left")
  }

  def get_mdn_info(generate_new:Boolean=false,fname:String="juan2/mdn_info"): DataFrame = {
    var is_do = generate_new
    if(!exists(fname))
      is_do=true
    if(generate_new){
      def get_phone = udf((x: String) => {
        val phone_reg = "^.*(1[2-9]\\d{9}).*$".r
        if (x != null && x.matches(phone_reg.toString())) {
          val phone_reg(a) = x
          a
        } else {
          ""
        }
      })
      val serv_t = spark.read.parquet("info/serv_t").where("STATE='F0A'").select("SERV_ID", "CUST_ID", "ACC_NBR", "PRODUCT_ID")
      val serv_s = spark.read.parquet("info/serv_subscriber").where("STATE='00A'")
      val contact = serv_s.select(serv_s("SERV_ID"), get_phone(serv_s("CUST_RELATION_INFO")).as("CONTACTOR")).where("CONTACTOR <> ''")
      val secd_nbr = serv_t.where("PRODUCT_ID=10501110").select("CUST_ID","ACC_NBR").groupBy("CUST_ID").agg(collect_set("ACC_NBR").as("SECD_NBR"))
      val df = serv_t.join(contact,Seq("SERV_ID"),"left").join(secd_nbr,Seq("CUST_ID"),"left")
      df.write.mode("overwrite").parquet(fname)
      df
    }else{
      spark.read.parquet(fname)
    }
  }

  def getContactor(): Unit = {
    def to_set = udf((e:String, list:WrappedArray[String])=>{
      list match {
        case null => if(e==null) Array[String]() else Array(e)
        case a => if(e==null) a.toArray else (e:+a.toList).toSet.toArray
      }
    })
    val df = get_mdn_info().select("CONTACTOR","SECD_NBR").dropDuplicates()
    df.select(to_set(df("CONTACTOR"),df("SECD_NBR")).as("juan"))
  }

  /***
    * 获取serv_t中有效的个人手机号码信息
    * */
   def getServT(): DataFrame ={
    spark.read.schema(SERV_T_STRUCT).parquet("info/serv_t").where("state='F0A' AND service_type='/s/t/mob' AND user_kind_id=0")
  }

  /**
    * 假设：同个人产品用户下的多个手机号为家庭群
    * */
  def getFamilyByProduct(serv_t:DataFrame):DataFrame = {
    val df = serv_t.groupBy("product_offer_instance_id")
      .agg(collect_set("acc_nbr").as("product_juan"))
    df.where(size(df("product_juan"))>1)
  }

  /**
    * 假设：同个人账户下的多个手机号为家庭群
    * */
  def getFamilyByAcct(serv_t:DataFrame):DataFrame = {
    val df = serv_t.groupBy("acct_id").agg(collect_set("acc_nbr").as("acct_juan"))
    df.where(size(df("acct_juan"))>1)
  }

  /**
    * 假设：同身份证下的多个个人手机号为家庭群
    * */
  def getFamilyByCert(serv_t:DataFrame): DataFrame = {
     val serv_s = spark.read.parquet("info/serv_subscriber").select("certificate_no","serv_id","state")
      .where("state='00A' and certificate_type = '2BA'").select("certificate_no","serv_id")
    val df = serv_t.select("serv_id","acc_nbr")
    val t = df.join(serv_s,Seq("serv_id"),"inner")
      .groupBy("certificate_no").agg(collect_set("acc_nbr").as("cert_juan"))
    t.where(size(t("cert_juan"))>1)
  }

  /**
    * 将多种Juan集成的Juan
    * */
  def getFamilyByEnsemble():DataFrame = {
    val holiday_juan = holidayJuan()
    val product_juan = productJuan()
    val acct_juan = acctJuan()
    val cert_juan = certJuan()
    val juans = Seq(holiday_juan,product_juan,acct_juan,cert_juan)
    juans.reduce(_.unionByName(_).dropDuplicates(juan_struct(0).name,juan_struct(1).name))
  }

  /**
    * 按照最新的原始数据更新所有Juan
    * */
  def renewJuan():Unit = {
    //更新基础表
    val backup = new BackUpOracle(spark)
    backup.backUpServS()
    backup.backUpServT()
    backup.backUpCustT()
    //更新Juan
    val serv_t = getServT()
    serv_t.cache()
    getFamilyByProduct(serv_t).write.mode("overwrite").parquet(PRODUCT_JUAN.path)
    getFamilyByAcct(serv_t).write.mode("overwrite").parquet(ACCT_JUAN.path)
    getFamilyByCert(serv_t).write.mode("overwrite").parquet(CERT_JUAN.path)
    getFamilyByEnsemble().write.mode("overwrite").parquet(ENSEMBLE_JUAN.path)
    serv_t.unpersist()
    val vipJuan = new VipJuan(spark)
    vipJuan.generateHolidayJuanByEnd(m=null,path = HOLIDAY_JUAN.path)
    vipJuan.generateWorkdayJuanByEnd(m=null,path = WORKDAY_JUAN.path)
  }

  def renewSingleJuan(juan:Juan): Unit ={
    val serv_t = getServT()
    juan match {
      case PRODUCT_JUAN => getFamilyByProduct(serv_t).write.mode("overwrite").parquet(PRODUCT_JUAN.path)
      case ACCT_JUAN => getFamilyByAcct(serv_t).write.mode("overwrite").parquet(ACCT_JUAN.path)
      case CERT_JUAN => getFamilyByCert(serv_t).write.mode("overwrite").parquet(CERT_JUAN.path)
    }
  }

  /***
    * 假设一个家庭圈内的成员会互相打电话，且通话次数为INFO_WEIGHT
    */
  val INFO_WEIGHT = 9999
  def fullConnect(nbrs:Iterable[String]): Array[Row] ={
    val list = scala.collection.mutable.Queue[Row]()
    for(i<-nbrs){
      for(j<-nbrs){
        if(i!=j)
          list.enqueue(Row(i,j,INFO_WEIGHT))
      }
    }
    list.toArray
  }

  /** 将Set类型的Juan转换为标准结构（juan_struct）的Juan
    * @param juan Juan对象的实例，path为hdfs存放目录，name为SetJuan的juan的字段名
    * */
  def juanDataFrame(juan:Juan):DataFrame = {
   val rdd =  spark.read.parquet(juan.path).select(juan.name).rdd.
      mapPartitions(_.map(_.getSeq[String](0))).
      flatMap(fullConnect(_))
    spark.createDataFrame(rdd,juan_struct)
  }

  //依照不同的圈生成不同的图
  def juan2graph(juan:Juan) = {
    val df = juan match {
      case HOLIDAY_JUAN => holidayJuan()
      case WORKDAY_JUAN => workdayJuan()
      case PRODUCT_JUAN => productJuan()
      case ACCT_JUAN => acctJuan()
      case CERT_JUAN => certJuan()
      case ENSEMBLE_JUAN => ensembleJuan()
    }
    Graph.fromEdges(
      df.rdd.filter{
        case Row(a:String,b:String,_) => a.matches(VipJuan.MOB_REG) && b.matches(VipJuan.MOB_REG)
      }.map{
        case Row(a:String,b:String,c:Int) => Edge(a.toLong,b.toLong,c)
      },
      ""
    )
  }

  def holidayJuan() = spark.read.parquet(HOLIDAY_JUAN.path)
  def workdayJuan() = spark.read.parquet(WORKDAY_JUAN.path)
  def productJuan() = juanDataFrame(PRODUCT_JUAN)
  def acctJuan()= juanDataFrame(ACCT_JUAN)
  def certJuan() = juanDataFrame(CERT_JUAN)
  def ensembleJuan() = spark.read.parquet(ENSEMBLE_JUAN.path)

}

object UserInfoJuan{
  val SERV_T_STRUCT = StructType(StructField("SERV_ID",DecimalType(12,0),true)::
    StructField("ACC_NBR",StringType,true)::
    StructField("ACCT_ID",DecimalType(12,0),true)::
    StructField("PRODUCT_OFFER_INSTANCE_ID",DecimalType(12,0),true)::
    StructField("STATE",StringType,true)::
    StructField("SERVICE_TYPE",StringType,true)::
    StructField("USER_KIND_ID",DecimalType(9,0),true)::Nil)
}

//def getJuanByUserOfr(month:String): DataFrame = {
//  val map = AREA_LATN.map(t => (t._1.toInt,t._3)).toMap
//  def parseNbr = udf((nbr:String,latn_id:Int)=>{
//  if(nbr.startsWith("1"))
//  nbr
//  else
//  map.getOrElse(latn_id,"")+nbr
//})
//  val path = BAS_PRD_INST.format(month)
//  val df = spark.read.parquet(path)
//  val r = df.select(df("Ofr_Inst_Id"),parseNbr(df("Accs_Nbr"),df("Latn_Id")).as("NBR")).
//  groupBy("Ofr_Inst_Id").
//  agg(collect_set("NBR").as("juan"))
//  r.where(size(r("juan"))>1)
//}
//
//  def getJuanByUserOfrNoFix(month:String): DataFrame = {
//  val path = BAS_PRD_INST.format(month)
//  val df = spark.read.parquet(path)
//  .select("Ofr_Inst_Id","Accs_Nbr")
//  .where("Accs_Nbr like '1%'")
//  .groupBy("Ofr_Inst_Id")
//  .agg(collect_set("Accs_Nbr").as("juan"))
//  df.select("juan").where(size(df("juan"))>1)
//}
