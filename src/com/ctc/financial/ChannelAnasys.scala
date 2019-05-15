package com.ctc.financial

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import com.ctc.util.DateUtil.{get_prev_six_month, next_month}
import com.ctc.util.INSTANTCE._
import com.ctc.util.HDFSUtil.exists
import org.apache.spark.sql.functions._

import scala.collection.mutable

class ChannelAnasys (@transient spark:SparkSession) extends Serializable {
  val area_latn_map = AREA_LATN.map(t => (t._1.toInt,t._3)).toMap
  val NBR = "^\\D*(\\d+)\\D*$".r
  val get_real_nbr = udf((latn_id:Int, nbr:String) => {
    nbr match {
      case NBR(a) if a.matches(MOB_NBR) => nbr
      case NBR(a) if a.matches("^\\d{5,}$") => area_latn_map.getOrElse(latn_id,"") + a
      case _ => null
    }
  })

  val get_real_nbr_str = udf((latn_id:java.math.BigDecimal, nbr:String) => {
    nbr match {
      case NBR(a) if a.matches(MOB_NBR) => a
      case NBR(a) if a.matches("^0\\d{7,}$") => a
      case NBR(a) if a.matches("^[1-9]\\d{4,}$") => area_latn_map.getOrElse((latn_id.longValue()/1000000).toInt,"") + a
      case _ => null
    }
  })

  def backUpBrdOut1(): Unit ={
    val data = spark.read.jdbc(TD_URL,"td_mart.brd_xin","innet_billing_cycle_id",201801,201808,7,TD_PROPERTY)
    data.write.mode("overwrite").parquet("td_mart/brd_xin")
  }

  def get_prom_nbr(month:String):DataFrame={
    val path = s"financial/prom_nbr/${month}"
    if(!exists(path)){
      val last_month = next_month(month,-1)
      val prom_nbr_sql = s"""
      sel a.prd_inst_id,a.latn_id,b.Fix_Accs_Nbr,b.Cdma_Nbr from
      (sel prd_inst_id,latn_id from td_mart.brd_out where Outnet_Billing_Cycle_Id = ${month} and Std_Prd_Name='宽带')a
      left join
      pv_mart_z.BAS_SERV_BRD_MON b
      on b.Billing_Cycle_Id=${last_month} and b.prd_inst_id=a.prd_inst_id
    """.stripMargin
      println(prom_nbr_sql)
      val prom_nbr = spark.read.jdbc(TD_URL,s"(${prom_nbr_sql})x",TD_PROPERTY)
      prom_nbr.write.mode(SaveMode.Overwrite).parquet(path)
    }
    spark.read.parquet(path)
  }

  def get_cert_nbr(month:String):DataFrame = {
    val path = s"financial/cert_nbr/${month}"
    if(!exists(path)){
      val last_six_month = next_month(month,-3)
      val prom_nbr_sql = s"""
      sel x.prd_inst_id,y.* from
      (sel a.prd_inst_id,b.cert_nbr from
      (sel prd_inst_id from td_mart.brd_out where Outnet_Billing_Cycle_Id = ${month} and Std_Prd_Name='宽带')a
      left join
      pv_mart_z.BAS_PRD_INST_CUR b
      on b.prd_inst_id=a.prd_inst_id)x
      left join
      (sel cert_nbr,Latn_Id,Accs_Nbr from pv_mart_z.BAS_PRD_INST_CUR n where n.serv_type_id in('/s/t/mob','/s/t/fix') and Innet_Date <= ${month} and Outnet_Date>=${last_six_month}) y
      on x.cert_nbr=y.cert_nbr
    """.stripMargin
      println(prom_nbr_sql)
      val prom_nbr = spark.read.jdbc(TD_URL,s"(${prom_nbr_sql})x",TD_PROPERTY)
      prom_nbr.write.mode(SaveMode.Overwrite).parquet(path)
    }
    spark.read.parquet(path)
  }

  def get_ensemble_phone(month:String): Unit = {
    val path = s"financial/nbr/$month"
    if(!exists(path)){
      val prom = get_prom_nbr(month)
      val cert = get_cert_nbr(month)
      val cert_a = cert.select(cert("Prd_Inst_Id").as("kd_id"),get_real_nbr(cert("Latn_Id"),cert("Accs_Nbr")).as("nbr")).where("nbr is not null")
      val prom_x = prom.select(prom("Prd_Inst_Id").as("kd_id"),get_real_nbr(prom("Latn_Id"),prom("Fix_Accs_Nbr")).as("nbr")).where("nbr is not null")
      val prom_y = prom.select(prom("Prd_Inst_Id").as("kd_id"),get_real_nbr(prom("Latn_Id"),prom("Cdma_Nbr")).as("nbr")).where("nbr is not null")
      val nbr = prom_x.unionByName(prom_y).unionByName(cert_a).distinct()
      nbr.write.mode(SaveMode.Overwrite).parquet(path)
    }
    spark.read.parquet(path)
  }

  def get_q_5_1_1(month:String): Unit ={
    val path = s"financial/q511/$month"
    if(!exists(path)){
      val month_6 = next_month(month,-6)
      val x = spark.read.parquet("td_mart/brd_out1").where(s"outnet_billing_cycle_id=$month").select("PRD_INST_ID","ACCS_NBR","MERGE_PROM_INST_ID").cache()
      val mobile = x.where("std_prd_name='移动'")
      val broadband = x.where("std_prd_name='宽带'")
      val data = mobile.join(broadband,Seq("MERGE_PROM_INST_ID"),"left").select(broadband("PRD_INST_ID").as("KD_ID"),mobile("ACCS_NBR"))
      val phone = spark.read.parquet("financial/cdma_voice").where(s"MONTH BETWEEN ${month_6} and ${month}").groupBy("BILLING_NBR","OPP_NUMBER","ORG_TRM_ID").sum("CNT").toDF("BILLING_NBR","OPP_NUMBER","ORG_TRM_ID","CNT")
      data.join(phone,data("ACCS_NBR") === phone("BILLING_NBR"),"left").write.parquet(path)
    }
    spark.read.parquet(path)
  }

  def renew_q511(month:String): Unit ={
    val data = spark.read.parquet(s"financial/q511/$month").where("BILLING_NBR IS NOT NULL").select("KD_ID","ACCS_NBR","OPP_NUMBER","CNT")
    val zj = data.where("ORG_TRM_ID = 0").toDF("KD_ID","ACCS_NBR","OPP_NUMBER","ZJ_CNT")
    val bj = data.where("ORG_TRM_ID = 1").toDF("KD_ID","ACCS_NBR","OPP_NUMBER","BJ_CNT")
    zj.join(bj,Seq("KD_ID","ACCS_NBR","OPP_NUMBER"),"outer").write.mode(SaveMode.Overwrite).parquet(s"financial/q511/cdma/$month")
  }

  def q_4(): Unit ={
    val date = new Timestamp(117,0,1,0,0,0,0)
    val x = spark.read.parquet("info/serv_subscriber").where("ROAD_CODE IS NOT NULL AND ROAD_CODE NOT IN ('--','-1','0')")
    val info = x.where(x("SERV_ID").isNotNull).select("SERV_ID","ROAD_CODE").distinct().repartition(1000)
    //val info = x.where(x("EFF_DATE") > date && x("SERV_ID").isNotNull).select("SERV_ID","ROAD_CODE").distinct().repartition(1000)
    val kd_road = spark.read.parquet("financial/road_code").where("ROAD_CODE IS NOT NULL AND ROAD_CODE NOT IN ('--','-1','0')").distinct()
    val r = kd_road.join(info,Seq("ROAD_CODE"),"left")
    r.write.mode("overwrite").parquet("financial/same_road_code")
    r.write.mode("overwrite").jdbc(anti_url,"TM_SAME_ROAD_CODE",properties)
  }

  def q_5(): Unit ={
    import org.apache.spark.sql.functions.length
    val src_id = spark.read.parquet("td_mart/brd_out").where("std_prd_name = '宽带'").select("PRD_INST_ID").distinct().toDF("SERV_ID")
    val serv_s = spark.read.parquet("info/serv_subscriber")
    //serv_s.where(serv_s("CERTIFICATE_NO").isNotNull && length(serv_s("CERTIFICATE_NO")) > 5).select("CERTIFICATE_NO").distinct.orderBy(serv_s("CERTIFICATE_NO").desc_nulls_first).show
    val servs = serv_s.where(serv_s("CERTIFICATE_NO").isNotNull && length(serv_s("CERTIFICATE_NO")) > 5).select("SERV_ID","ROAD_CODE","CERTIFICATE_NO","PARTITION_ID_REGION","STATE","CUST_ID","EFF_DATE","EXP_DATE")
    val single_cert = serv_s.select("SERV_ID","CERTIFICATE_NO").distinct()
    val data = src_id.join(single_cert,Seq("SERV_ID"),"left").select("CERTIFICATE_NO").distinct()
    data.write.parquet("financial/cert_no_ids")
    val ids = spark.read.parquet("financial/cert_no_ids")
    val cert_ids = ids.join(servs,Seq("CERTIFICATE_NO"),"left").where("SERV_ID IS NOT NULL")
    val serv_t = spark.read.parquet("info/serv_t").select("SERV_ID","SERVICE_TYPE","NEW_DATE").where("SERVICE_TYPE = '/s/i/kd'").distinct()
    val r = cert_ids.join(serv_t,Seq("SERV_ID"),"left")
    r.write.mode("overwrite").parquet("financial/same_cert_diff_road")
    val result = spark.read.parquet("financial/same_cert_diff_road")
    result.write.jdbc(anti_url,"TM_SAME_CERT",properties)
  }

/*  def format_sms(): Unit ={
    case class Call(MONTH:String,ZJ:String,BJ:String,ZJ_CNT:Int,BJ_CNT:Int)extends Serializable
    val sms = spark.read.parquet("financial/SMS")
    val target_set = Set("10086","10010","10015","10016")
    val new_sms = sms.map{
      case Row(m:String,zj:String,bj:String,cnt:java.math.BigDecimal)=>{
        if(target_set.contains(zj)){
          Call(m,bj,zj,0,cnt.intValue())
        }else{
          Call(m,zj,bj,cnt.intValue(),0)
        }
      }
    }
    val data = new_sms.groupBy("MONTH","ZJ","BJ").agg(sum(new_sms("ZJ_CNT")).as("ZJ_CNT"),sum(new_sms("BJ_CNT")).as("BJ_CNT"))
    data.write.mode("overwrite").parquet("financial/sms")
  }

  def format_wjjs(): Unit ={
    case class Call(MONTH:String,ZJ:String,BJ:String,ZJ_CNT:Int,BJ_CNT:Int)extends Serializable
    val wjjs = spark.read.parquet("financial/WJJS")
    val target_set = Set("10086","10010","10015","10016")
    val new_wjjs = wjjs.map{
      case Row(zj:String,zj_area:String,bj:String,bj_area:String,cnt:java.math.BigDecimal,m:String)=>{
        val zp = if(zj.startsWith("1")) zj else zj_area+zj
        val bp = if(bj.startsWith("1")) bj else bj_area+bj
        if(target_set.contains(zj)){
          Call(m,bp,zp,0,cnt.intValue())
        }else{
          Call(m,zp,bp,cnt.intValue(),0)
        }
      }
    }
    val data = new_wjjs.groupBy("MONTH","ZJ","BJ").agg(sum(new_wjjs("ZJ_CNT")).as("ZJ_CNT"),sum(new_wjjs("BJ_CNT")).as("BJ_CNT"))
    data.write.mode("overwrite").parquet("financial/wjsj")
  }*/

  def get_q_5_1_1_wjjs(month:String): DataFrame ={
    val path = s"financial/q511/wjjs/$month"
    if(!exists(path)){
      val month_6 = next_month(month,-6)
      val x = spark.read.parquet("td_mart/brd_out1").where(s"outnet_billing_cycle_id=$month").select("PRD_INST_ID","ACCS_NBR","SUB_BUREAU_ID","MERGE_PROM_INST_ID")
      val mobile = x.where("std_prd_name in ('移动','固话')").select(x("PRD_INST_ID"),get_real_nbr_str(x("SUB_BUREAU_ID"),x("ACCS_NBR")).as("ACCS_NBR"),x("MERGE_PROM_INST_ID"))
      val broadband = x.where("std_prd_name='宽带'")
      val data = mobile.join(broadband,Seq("MERGE_PROM_INST_ID"),"left").select(broadband("PRD_INST_ID").as("KD_ID"),mobile("ACCS_NBR"))

      val cnt_data = spark.read.parquet(s"financial/wjjs").where(s"MONTH BETWEEN ${month_6} and ${month}")
      val cnt = cnt_data.groupBy("MONTH","ZJ","BJ").agg(sum(cnt_data("ZJ_CNT")).as("ZJ_CNT"),sum(cnt_data("BJ_CNT")).as("BJ_CNT"))
      data.join(cnt,data("ACCS_NBR") === cnt("ZJ"),"left").write.parquet(path)
    }
    spark.read.parquet(path)
  }

  def get_q_5_1_1_sms(month:String): DataFrame ={
    val path = s"financial/q511/sms/$month"
    if(!exists(path)){
      val month_6 = next_month(month,-6)
      val x = spark.read.parquet("td_mart/brd_out1").where(s"outnet_billing_cycle_id=$month").select("PRD_INST_ID","ACCS_NBR","MERGE_PROM_INST_ID")
      val mobile = x.where("std_prd_name = '移动'")
      val broadband = x.where("std_prd_name='宽带'")
      val data = mobile.join(broadband,Seq("MERGE_PROM_INST_ID"),"left").select(broadband("PRD_INST_ID").as("KD_ID"),mobile("ACCS_NBR"))
      val cnt_data = spark.read.parquet(s"financial/sms").where(s"MONTH BETWEEN ${month_6} and ${month}")
      val cnt = cnt_data.groupBy("MONTH","ZJ","BJ").agg(sum(cnt_data("ZJ_CNT")).as("ZJ_CNT"),sum(cnt_data("BJ_CNT")).as("BJ_CNT"))
      data.join(cnt,data("ACCS_NBR") === cnt("ZJ"),"left").write.parquet(path)
    }
    spark.read.parquet(path)
  }

  def run_q511(): Unit ={
    val m = 201801.to(201807).map(_.toString)
    for(i <- m) q511_ora(i,"sms")
    for(i <- m) q511_ora(i,"wjjs")
  }

  def q511_ora(month:String,sms_or_wjjs:String): Unit ={
    def m = udf(()=>{month})
    val data = spark.read.parquet(s"financial/q511/${sms_or_wjjs}/$month").where("ZJ IS NOT NULL")
    val yd = data.where("BJ = '10086'").groupBy("KD_ID","ACCS_NBR").agg(sum(data("ZJ_CNT")).as("YD_ZJ_CNT"),sum(data("BJ_CNT")).as("YD_BJ_CNT"))
    val lt = data.where("BJ != '10086'").groupBy("KD_ID","ACCS_NBR").agg(sum(data("ZJ_CNT")).as("LT_ZJ_CNT"),sum(data("BJ_CNT")).as("LT_BJ_CNT"))
    val r = yd.join(lt,Seq("KD_ID","ACCS_NBR"),"outer").na.fill(0).coalesce(1)
    r.withColumn("MONTH",m()).write.mode("append").jdbc(anti_url,s"TM_YSYX_${sms_or_wjjs}",properties)
  }

  import java.util.Calendar
  case class UserInfo(age:Int,sex:Int) extends Serializable
  def get_q_3() = {
    val x = spark.read.parquet("td_mart/brd_out1").select("PRD_INST_ID","ACCS_NBR","SUB_BUREAU_ID","MERGE_PROM_INST_ID")
    val mobile = x.where("std_prd_name in ('移动','固话')").select(x("PRD_INST_ID"),get_real_nbr_str(x("SUB_BUREAU_ID"),x("ACCS_NBR")).as("ACCS_NBR"),x("MERGE_PROM_INST_ID"))
    val broadband = x.where("std_prd_name='宽带'")
    val data = mobile.join(broadband,Seq("MERGE_PROM_INST_ID"),"left").select(mobile("PRD_INST_ID").as("SERV_ID"),broadband("PRD_INST_ID").as("KD_ID"),mobile("ACCS_NBR")).distinct()
    val serv_s = spark.read.parquet("info/serv_subscriber").select("SERV_ID","CERTIFICATE_NO").distinct()
    val info = data.join(serv_s,Seq("SERV_ID"),"left")
    val CERT_15 = "^\\s*\\d{6}(\\d{2})\\d{6}(\\d)\\s*$".r
    val CERT_18 = "^\\s*\\d{6}(\\d{4})\\d{6}(\\d)[0-9X]\\s*$".r
    val current_year = Calendar.getInstance().get(Calendar.YEAR)
    def age_sex = udf((cert_nbr:String) => {
      cert_nbr match {
        case CERT_15(birthYear,sex) => UserInfo(current_year-("19"+birthYear).toInt,sex.toInt%2)
        case CERT_18(birthYear,sex) => UserInfo(current_year-birthYear.toInt,sex.toInt%2)
        case _ => UserInfo(-1,-1)
      }
    })
    val res = info.select(info("SERV_ID"),info("KD_ID"),info("ACCS_NBR"),info("CERTIFICATE_NO"),age_sex(info("CERTIFICATE_NO")).as("age_sex"))
    val r = res.select(res("SERV_ID"),res("KD_ID"),res("ACCS_NBR"),res("CERTIFICATE_NO"),res("age_sex").getField("age").as("age"),res("age_sex").getField("sex").as("sex"))
    r.write.mode("overwrite").parquet("financial/user_info")
  }
  val user_info = spark.read.parquet("financial/user_info")
  def get_q_3_r(month:String): Unit ={
    val x = spark.read.parquet("td_mart/brd_out1").where(s"outnet_billing_cycle_id=$month")
      .where("std_prd_name in ('移动','固话')")
      .select("PRD_INST_ID","ACCS_NBR")
      .toDF("SERV_ID","ACCS_NBR")
    val data = x.join(user_info,Seq("SERV_ID"),"left")
    val next_m = next_month(month,-1)
    val next_4_m = next_month(month,-4)
    val juan = spark.read.parquet(s"holiday_juan/${next_4_m}_${next_m}").select("NBR","OPP_NBR")
    val phone = data.select("ACCS_NBR")
    val zx = phone.join(juan,phone("ACCS_NBR")===juan("NBR"),"inner").select("NBR","OPP_NBR")
    val fx = phone.join(juan,phone("ACCS_NBR")===juan("OPP_NBR"),"inner").select("OPP_NBR","NBR").toDF("NBR","OPP_NBR")
    def statics = udf((arr:scala.collection.mutable.WrappedArray[String])=>{
      val bw_cnt = arr.map{
        case p if p.startsWith("0") => 1
        case p if p.matches(CTC_NBR) => 1
        case _ => 0
      }.sum
      val juan_size = arr.length
      val o  = juan_size - bw_cnt
      Map("TOTAL_CNT" -> juan_size,"OTHER_NET_CNT" -> o)
    })
    def m = udf(()=>{month})
    val ex = zx.unionAll(fx).groupBy("NBR").agg(collect_list("OPP_NBR").as("OPP_NBR"))
    val m_r = ex.select(ex("NBR"),statics(ex("OPP_NBR")).as("OPP_NBR"))
    val nbr_r = m_r.select(m().as("MONTH"),m_r("NBR").as("ACCS_NBR"),m_r("OPP_NBR").getField("TOTAL_CNT").as("TOTAL_CNT"),m_r("OPP_NBR").getField("OTHER_NET_CNT").as("OTHER_NET_CNT"))
    val result = nbr_r.join(data,Seq("ACCS_NBR"),"inner")
    result.write.mode("append").jdbc(anti_url,"TM_CJYHSX",properties)
  }

  def get_q_3_ensemble(): Unit ={
    val data = spark.read.parquet("financial/user_info")
    val juan = spark.read.parquet("ensemble_union").select("NBR","OPP_NBR")
    val phone = data.select("ACCS_NBR")
    val zx = phone.join(juan,phone("ACCS_NBR")===juan("NBR"),"inner").select("NBR","OPP_NBR")
    val fx = phone.join(juan,phone("ACCS_NBR")===juan("OPP_NBR"),"inner").select("OPP_NBR","NBR").toDF("NBR","OPP_NBR")
    def statics = udf((arr:scala.collection.mutable.WrappedArray[String])=>{
      val bw_cnt = arr.map{
        case p if p.startsWith("0") => 1
        case p if p.matches(CTC_NBR) => 1
        case _ => 0
      }.sum
      val juan_size = arr.length
      val o  = juan_size - bw_cnt
      Map("TOTAL_CNT" -> juan_size,"OTHER_NET_CNT" -> o)
    })
    val ex = zx.unionAll(fx).groupBy("NBR").agg(collect_list("OPP_NBR").as("OPP_NBR"))
    val m_r = ex.select(ex("NBR"),statics(ex("OPP_NBR")).as("OPP_NBR"))
    val nbr_r = m_r.select(m_r("NBR").as("ACCS_NBR"),m_r("OPP_NBR").getField("TOTAL_CNT").as("TOTAL_CNT"),m_r("OPP_NBR").getField("OTHER_NET_CNT").as("OTHER_NET_CNT"))
    val result = nbr_r.join(data,Seq("ACCS_NBR"),"inner")
    result.write.mode("append").jdbc(anti_url,"TM_CJYHSX_ALL",properties)
  }


  def x(): Unit ={
    val cjx = spark.read.parquet("")
    val tx = spark.read.parquet("")
    val gl = spark.read.parquet("")
    val r2 = cjx.groupBy("latn_id").agg(
      count("latn_id").as("NBR_CNT"),
      sum("TOTAL_CNT").as("TOTAL_CNT_SUM"),
      sum("OTHER_NET_CNT").as("OTHER_NET_CNT_SUM"),
      avg("TOTAL_CNT").as("TOTAL_CNT_AVG"),
      avg("OTHER_NET_CNT").as("OTHER_NET_CNT_AVG")
    )
    tx.select(
      tx("latn_id"),
      when(tx("age").between(0,30),1).otherwise(0).as("a"),
      when(tx("age").between(30,40),1).otherwise(0).as("b"),
      when(tx("age").between(40,50),1).otherwise(0).as("c"),
      when(tx("age").between(50,200),1).otherwise(0).as("d")
    ).groupBy("latn_id").agg(Map(
      "a" -> "sum",
      "b" -> "sum",
      "c" -> "sum",
      "d" -> "sum"
    )).show

    tx.withColumn("a",tx("age").when(tx("age").between(0,30),1).otherwise(0))
    val l = gl.where("Outnet_Billing_Cycle_Id != '300012'")
    Array("grid_name","total","201705", "201801", "201802", "201803", "201804", "201805", "201806", "201807", "201808")
    val ms =  Array("201808", "201803", "201806", "201801", "201705", "201802", "201805", "201804", "201807")
    val dfs = for(i <- ms)yield{
      l.where(s"Outnet_Billing_Cycle_Id = ${i}").select("LATN_ID","GRID_ID","GRID_NAME","COUNT").withColumnRenamed("count",i)
    }
    val df = dfs.reduce(_.join(_,Seq("LATN_ID","GRID_NAME"),"outer"))
    val x = l.withColumn("cnt",l("count").cast("Integer")).drop("count").groupBy("LATN_ID","GRID_ID").sum("cnt").withColumnRenamed("sum(cnt)","total")
    val r = df.join(x,Seq("LATN_ID","GRID_ID"))
    r.cache()
    r.where("total is null").count()

  }


}
