package com.ctc.backup

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.udf
import com.ctc.util.INSTANTCE._
import com.ctc.util.HDFSUtil._
import com.ctc.util.DateUtil
import com.ctc.util.FeatureUtil._
class BackUpTD (@transient spark:SparkSession) extends DataBase {

  override def importByCsv(path:String,table:String): Unit = {
    val data = spark.read.option("header","true").csv(path)
    data.show()
    data.repartition(50).write.mode("append").jdbc(TD_URL,table,TD_PROPERTY)
  }

  override def exportByCsv(path: String, table: String, partitionColumn: String, start: Int, end: Int): Unit = {
    val data = spark.read.jdbc(TD_URL,table,partitionColumn,start,end,end-start,TD_PROPERTY)
    data.write.mode(SaveMode.Overwrite).option("header","true").csv(path)
  }

  override def simpleExportByCsv(path: String, table: String): Unit = {
    val data = spark.read.jdbc(TD_URL,table,TD_PROPERTY)
    data.write.mode(SaveMode.Overwrite).option("header","true").csv(path)
  }

  val logger = Logger.getLogger("BackUpTD")
  val orginal_tables = Array("PD_STAGE.Z04_NOC4_HLR",
    "PD_STAGE.Z04_NOC2_HLR_DF2",
    "PD_STAGE.Z04_NOC2_HLR_DF3",
    "PD_STAGE.Z04_NOC2_HLR_DF6",
    "PD_STAGE.Z04_NOC2_HLR_DF7",
    "PD_STAGE.Z04_NOC2_HLR_NPDF")
  def backupCloseMob(day:String,is_pm:String): Unit = {
    logger.info(s"START BACKUP day=$day is_pm=$is_pm PRD_INST_NOC_HLR")
    val suffix = if(is_pm.equals("1")) "_15"+day.takeRight(4) else day.takeRight(4)
    val tables = orginal_tables.map(_+suffix)
    val T_SQL =
      s"""(select $day AS Date_Id, Accs_Nbr,$is_pm AS IS_PM
         |from %s
         |where IS_OPEN_FLAG = '%s')A""".stripMargin
    val nbr_reg = "^.*(1[3-9]\\d{9}).*$".r
    val format_nbr = udf((nbr:String) => {
      if(nbr.matches(nbr_reg.toString)){
        val nbr_reg(real) = nbr
        real
      }else{
        nbr
      }
    })

    for(table <- tables){
      try{
        if(table.equalsIgnoreCase(orginal_tables(0)+suffix)){
          val r = spark.read.jdbc(TD_URL, T_SQL.format(table,"0"), TD_PROPERTY)
          r.select(r("Date_Id"),format_nbr(r("Accs_Nbr")).as("Accs_Nbr"),r("IS_PM"))
        }else{
          spark.read.jdbc(TD_URL, T_SQL.format(table,"N"), TD_PROPERTY).write.mode("append").parquet("td/PRD_INST_NOC_HLR")
        }
          logger.info("%s备份成功！".format(table))
      }catch {
        case e:Exception => logger.error("%s备份失败！".format(table),e)
      }
    }

  }

  def backupCloseMobYestoday(): Unit ={
    val DAY_MS = 86400000L
    val now = new Date()
    val is_pm = if(now.getHours < 14)"0" else "1"
    val yestoday = new Date(now.getTime-DAY_MS)
    val day = DateUtil.yyyyMMdd.format(yestoday)
    backupCloseMob(day,is_pm)
  }

  def backUpBroadBand(month:String,is_refresh:Boolean=false): Unit = {
    logger.info("*"*10+s"开始备份${month}宽带模型数据"+"*"*10)
    val table = s"(SELECT B.* FROM td_work.bns_kd_list_id   A LEFT JOIN td_work.bns_kd_list_$month  B ON A.Prd_Inst_Id = B.Prd_Inst_Id)C"
    val path = BD_DATA_PATH.format(month)
    val x = spark.read.jdbc(TD_URL,table,"Latn_Id",1001,1020,19,TD_PROPERTY)
    val df =  x.toDF(x.columns.map(_.toUpperCase).toSeq:_*)
    val names = df.columns.toList.dropWhile(_.endsWith("FLAG"))
    val split_index = names.indexOf("R3A_NET_DAYS")
    val new_name = "STD_PRD_INST_STAT_ID"+:names.drop(split_index)
    df.select("PRD_INST_ID",new_name:_*).na.fill(0.0).repartition(100)
      .write.mode("overwrite").parquet(path)
    logger.info("*"*10+s"备份${month}宽带模型数据成功"+"*"*10)
    if(is_refresh){
      logger.info("*"*10+s"开始更新${month}宽带模型COMMON数据"+"*"*10)
      val common_name = List("SERV_TYPE_ID",
        "STD_PRD_INST_STAT_ID",
        "PRD_ID",
        "COUNTRY_FLAG",
        "USER_TYPE_ID",
        "PAY_MODE_ID",
        "STRATEGY_SEGMENT_ID",
        "AGE",
        "VIP_FLAG",
        "CUST_TYPE_ID",
        "MARKET_MANAGER_ID",
        "INDUS_TYPE_ID",
        "CENTREX_FLAG",
        "LINE_RATE",
        "EXCHANGE_ID",
        "OFR_ID",
        "EFF_ACCT_MONTH",
        "EXP_ACCT_MONTH",
        "INNET_BILLING_CYCLE_ID",
        "OUTNET_BILLING_CYCLE_ID",
        "STOP_BILLING_CYCLE_ID",
        "REMOVE_TYPE_ID",
        "INNET_DUR",
        "INNET_DUR_LVL_ID",
        "OLD_PRD_INST_TYPE_ID",
        "MERGE_PROM_ID",
        "STOP_DUR",
        "BIL_FLAG",
        "INNET_FLAG",
        "PRD_INST_FLAG",
        "PRD_INST_NBR",
        "SUB_BUREAU_ID",
        "LATN_ID",
        "CTY_REG_FLG",
        "GROUP_REGION_ID",
        "VIP_MANAGER_ID",
        "GRID_ID",
        "ACT_FLAG",
        "CERT_TYPE_ID",
        "GENDER_ID",
        "SEND_RATE",
        "RECV_RATE",
        "OVER_PER_HOUR_AMT",
        "AVAILABLE_HOURS",
        "PRTL_MONS",
        "AVG_MON_AMT",
        "PRTL_AMT",
        "END_BILLING_CYCLE_ID",
        "PAY_FLAG",
        "PRO_PRTL_MONS",
        "PRICING_PLAN_ID",
        "PRICING_DESC",
        "UNIT_CHARGE",
        "CYCLE_CHARGE",
        "BEF_RENEW_EXP_MONTH",
        "LAST_RENEWAL_MONTH",
        "REF_TYPE",
        "ASSESS_BIL_FLAG",
        "RECV_AMT",
        "BROADBAND_TICKET_FLAG",
        "ACCS_MODE_CD",
        "ACC_TYPE",
        "CHARGE_2809",
        "CHARGE_BEFORE",
        "CHARGE_FT",
        "CHARGE_FT_BEFORE",
        "ACCT_BALANCE_AMOUNT",
        "OWE_DUR",
        "OWE_AMT",
        "FIN_OWE_AMT",
        "BILL_OWE_AMT",
        "PAY_CHARGE",
        "CCUST_BRD_CNT",
        "CCUST_FIX_CNT",
        "CCUST_CDMA_CNT",
        "USE_MONS",
        "STOP_CNT",
        "NET_USE_ZERO_MC",
        "IPTV_FLAG")
      fillWithZero(
        fillWithMost(
          df.select("PRD_INST_ID",common_name:_*)
        )
      ).repartition(100).write.mode("overwrite").parquet(BD_DATA_PATH.format("common"))
      logger.info("*"*10+s"更新${month}宽带模型COMMON数据成功"+"*"*10)
    }
  }

  def backUpCDMA(month:String,is_refresh:Boolean=false):Unit = {
    logger.info("*"*10+s"开始备份${month}CDMA模型数据"+"*"*10)
    val table = s"td_work.tc_cdma_model_${month}_t4"
    val path = CDMA_DATA_PATH.format(month)
    val x = spark.read.jdbc(TD_URL,table,"Latn_Id",1001,1020,19,TD_PROPERTY)
    val df =  x.toDF(x.columns.map(_.toUpperCase).toSeq:_*)
    val names = df.columns.toList
    val split_index = names.indexOf("CERT_NBR")
    val new_name = names.drop(split_index+1)
    fillWithZero(df.select("PRD_INST_ID",new_name:_*)).repartition(100)
      .write.mode("overwrite").parquet(path)
    logger.info("*"*10+s"备份${month}CDMA模型数据成功"+"*"*10)
    if(is_refresh){
      logger.info("*"*10+s"开始备份${month}CDMA模型COMMON数据"+"*"*10)
      val format = new SimpleDateFormat("yyyyMM")
      val now = new Date()
      def timedelta = udf((col:String)=>{
        if(col==null){
          60
        }else{
          val date = format.parse(col)
          (now.getTime-date.getTime)/(1000*60*60*24*30)
        }
      })

      val common_name = List("LATN_ID", "AGE", "GENDER_ID", "STRATEGY_SEGMENT_ID", "INNET_BILLING_CYCLE_ID",
        "YHFL", "CHANNEL_TYPE_NAME", "CHANNEL_TYPE_NAME_LVL1", "TERM_MOB_PRICE", "EXP_BILLING_CYCLE_ID",
        "CHARGE", "BILLING_TYPE_ID", "OFR_ID",  "HHMD", "CDE_MERGE_PROM_NAME",
        "STD_MERGE_PROM_TYPE_ID", "ZHRH",  "CARD_TYPE", "TERM_TYPE_ID", "HBBT",  "THSC", "YWQF", "CWQF", "NAME",
        "LOWER_CHARGE", "FKZS", "FZ_FLAG")

      fillWithZero(
        fillWithMost(
          df.select("PRD_INST_ID",common_name:_*)
            .withColumn("INNET_BILLING_CYCLE_ID",timedelta(df("INNET_BILLING_CYCLE_ID")))
            .withColumn("EXP_BILLING_CYCLE_ID",timedelta(df("EXP_BILLING_CYCLE_ID")))
        )
      ).repartition(100).write.mode("overwrite").parquet(CDMA_DATA_PATH.format("common"))
      logger.info("*"*10+s"备份${month}CDMA模型COMMON数据成功"+"*"*10)
    }
  }

  def backTdTable(table:String,partition_col:String,start:Int,end:Int): String ={
    val path ="td_work/" + table.split("\\.")(1)
    spark.read.jdbc(TD_URL,table,partition_col,start,end,end-start,TD_PROPERTY).write.csv(path)
    path
  }
  implicit def list2Array(list:List[String])=list.toArray[String]
  def backUpBasPrdInstMonth(month:String): Unit ={
    val path = BAS_PRD_INST.format(month)
    val t_sql = s"""SELECT
                  	A .Prd_Inst_Id,
                  	A .Accs_Nbr,
                  	A .Acct_Id,
                  	A .Cust_Id,
                  	A .Age,
                  	A .Gender_Id,
                  	A .Ofr_Id,
                  	A .Ofr_Inst_Id,
                  	A .Innet_Dur,
                  	A .Latn_Id
                  		FROM
                  			PV_MART_Z.BAS_PRD_INST_month A
                  		WHERE
                  		Serv_Type_Id in ('/s/t/mob','/s/t/fix') AND billing_cycle_id = $month"""
    val df = spark.read.jdbc(TD_URL,s"($t_sql)x",10.until(80,5).map(age => s"age>= $age and age< ${age+5}").toList:+"age<10":+"age>=80",TD_PROPERTY)
    df.write.mode("overwrite").parquet(path)
  }

  def backUpShop(): Unit ={
    val sql =
      """
        |SELECT
        |	Boss_Org_Id,
        |	Channel_Ct_Group_Cd,
        |	Channel_Name,
        |	Chnl_Longitud,
        |	Chnl_Latitude,
        |	Zysxtx_name,
        |	t4.Channel_Type_Name_Lvl3,
        |	t4.Channel_Type_Name_Lvl2,
        |	t4.Channel_Type_Name_Lvl1
        |FROM
        |	pv_data_z.pty_Channel_org t1
        |LEFT JOIN pv_mart_z.dmn_Zysxtx t3 ON t1.Zysxtx = t3.Zysxtx
        |LEFT JOIN pv_marT_z.dmn_channel_type_all t4 ON t1.Channel_Type_Lvl3_Id = t4.Channel_Type_Lvl3_Id
        |WHERE
        |	t1.Status_Cd = 1000
      """.stripMargin
    spark.read.jdbc(TD_URL,s"(${sql})a",TD_PROPERTY).write.mode("overwrite").jdbc(anti_url,"TD_SHOP_POS",properties)
  }
}
