package com.ctc.tmp

import com.ctc.util.AppUtils
import com.ctc.util.INSTANTCE._
import com.ctc.util.DateUtil

import org.apache.spark.sql.SparkSession

import scala.annotation.meta.param

class BackUpTD(@param spark: SparkSession) extends Serializable {
  val INIT_PRODUCT =
    """select * from  pv_mart_z.BWT_PRD_PD_INST_D
                   where   PD_INST_STATE_CD NOT IN ('110000', '130000', '140000', '119999')
                   and  day_Id = %s
                   and  OPEN_DATE/1+19000000 >= %s"""


  val SALE_OUTLETS_DAY = "SELECT SALE_OUTLETS_CD,ZQ_TYPE, CHANNEL_TYPE_CD_3 FROM pd_data.bwt_SALE_OUTLETS_DAY WHERE day_id  = %s"
  val SALE_STAFF_INFO_DAY = "SELECT STAFF_CD,POST FROM pd_data.BWT_SALE_STAFF_INFO_DAY WHERE day_id = %s"

  val PRD_PD_INST_DAY =
    """select * from  pv_mart_z.BWT_PRD_PD_INST_D
                          where   PD_INST_STATE_CD NOT IN ('110000', '130000', '140000', '119999')
                          and  day_Id = %s
                          and  OPEN_DATE/1+19000000 = day_id"""




  def writeToOracle(srcSql: String, dstTable: String, saveMode: String = "append"): Unit = {
    val srcTable = s"(${srcSql})A"
    println(srcTable)
    val df = spark.read.jdbc(TD_URL, srcTable, TD_PROPERTY)
    df.toDF(df.columns.map(_.toUpperCase): _*).write.mode(saveMode).jdbc(anti_url2, dstTable, properties)
  }

  def outlets(day: String): Unit = {
    writeToOracle(SALE_OUTLETS_DAY.format(day), "HT_SALE_OUTLETS")
  }

  def staff(day: String) = {
    writeToOracle(SALE_STAFF_INFO_DAY.format(day), "HT_SALE_STAFF_INFO")
  }

  def product(day: String) = {
    writeToOracle(PRD_PD_INST_DAY.format(day), "HT_PRD_PD_INST")
  }

  def echannel(day:String,prev_day:String): Unit ={
    val E_CHANNEL_DAY = s"""SELECT prd_inst_id,sum(Tdd_Flux)/1024 Tdd_Flux_30,sum(Tdd_Bil_Dur)/60 Tdd_Bil_Dur_30,
              sum(case when current_date - CAST(Date_Id AS DATE FORMAT'yyyymmdd') <= 7
              then Tdd_Flux else 0 end)/1024 Tdd_Flux_7,
              sum(case when current_date - CAST(Date_Id AS DATE FORMAT'yyyymmdd') <= 7
              then Tdd_Bil_Dur else 0 end)/60 Tdd_Bil_Dur_7
              					 from pv_mart_z.BEH_MOB_STR_DAY  where
                prd_inst_id in (
                SELECT a.prod_inst_id
                                      FROM (
                                        SELECT
                                             *
                                       FROM pv_mart_z.BWT_PRD_PD_INST_D
                                       WHERE PRODUCT_NBR LIKE ANY ('1015%')
                                        AND current_date - OPEN_DATE<=30
                                      AND day_Id = '${day}'   -----N-1
                                    AND PD_INST_STATE_CD NOT IN ('110000', '130000', '140000', '119999')
                                       )A
                                       LEFT JOIN (
                                        SELECT * FROM pd_data.bwt_SALE_OUTLETS_DAY WHERE day_id  = '${prev_day}'    -------------N-2
                                                 ) B
                                               ON A.SALE_OUTLETS_CD = B.SALE_OUTLETS_CD
                                         where  B.CHANNEL_TYPE_CD_3 IN ('120101', '120102','120103','120201',
                                                                       '120202','120301','120302','120303',
                                                                       '120304','120401','120402','120501',
                                                                       '120502','120503','120504','120505',
                                                                       '120506','120507','120508','120509',
                                                                       '120510','120511','120512','120513',
                                                                       '120514','120515','120516')
                )and current_date - CAST(Date_Id AS DATE FORMAT'yyyymmdd') <= 30
              			 group by prd_inst_id"""
    writeToOracle(E_CHANNEL_DAY, "TD_E_CHANNEL_DAY","overwrite")
  }

  def initProduct(day: String): Unit = {
    writeToOracle(INIT_PRODUCT.format(day, day.take(6) + "01"), "HT_PRD_PD_INST", "overwrite")
  }

  def dayJob(today: String = DateUtil.today()): Unit = {
    val yestoday = DateUtil.next_day(today, -1)
    val prev_2 = DateUtil.next_day(today, -2)
    outlets(prev_2)
    staff(prev_2)
    product(yestoday)
  }

  def init(today: String = DateUtil.today()): Unit = {
    val yestoday = DateUtil.next_day(today, -1)
    val prev_2 = DateUtil.next_day(today, -2)
    writeToOracle(SALE_OUTLETS_DAY.format(prev_2), "HT_SALE_OUTLETS", "overwrite")
    writeToOracle(SALE_STAFF_INFO_DAY.format(prev_2), "HT_SALE_STAFF_INFO", "overwrite")
    initProduct(yestoday)
  }

  def monthResult(today: String = DateUtil.today()): Unit = {
    val yestoday = DateUtil.next_day(today, -1)
    val prev_2 = DateUtil.next_day(today, -2)
    val RESULT_SQL =
      s"""SELECT
                       a. *,B.CHANNEL_TYPE_CD_3

                        FROM (
                         SELECT
                              *
                         FROM pv_mart_z.BWT_PRD_PD_INST_D
                          WHERE PRODUCT_NBR LIKE ANY ('1015%')
                          AND OPEN_DATE/1+19000000 >= ${yestoday.take(6) + "01"}
                          AND day_Id = ${yestoday}    -------------N-1
                          AND PD_INST_STATE_CD NOT IN ('110000', '130000', '140000', '119999')
                        )A
                        LEFT JOIN (
                         SELECT * FROM pd_data.bwt_SALE_OUTLETS_DAY WHERE day_id  = ${prev_2}    -------------N-2
                                  ) B
                                ON A.SALE_OUTLETS_CD = B.SALE_OUTLETS_CD
                          where  B.CHANNEL_TYPE_CD_3 IN ('120101',
                                                        '120102',
                                                        '120103',
                                                        '120201',
                                                        '120202',
                                                        '120301',
                                                        '120302',
                                                        '120303',
                                                        '120304',
                                                        '120401',
                                                        '120402',
                                                        '120501',
                                                        '120502',
                                                        '120503',
                                                        '120504',
                                                        '120505',
                                                        '120506',
                                                        '120507',
                                                        '120508',
                                                        '120509',
                                                        '120510',
                                                        '120511',
                                                        '120512',
                                                        '120513',
                                                        '120514',
                                                        '120515',
                                                        '120516')       """
    writeToOracle(RESULT_SQL, "HT_CHANNEL_RESULT", "overwrite")
  }

  def dayResult(today: String = DateUtil.today()): Unit = {
    val yestoday = DateUtil.next_day(today, -1)
    val prev_2 = DateUtil.next_day(today, -2)
    val RESULT_SQL =s"""SELECT
                        a. *,B.CHANNEL_TYPE_CD_3
                        FROM (
                          SELECT
                               *
                          FROM pv_mart_z.BWT_PRD_PD_INST_D
                           WHERE PRODUCT_NBR LIKE ANY ('1015%')
                           AND OPEN_DATE/1+19000000 = day_id
                           AND day_Id = ${yestoday}    -------------N-1
                           AND PD_INST_STATE_CD NOT IN ('110000', '130000', '140000', '119999')
                         )A
                         LEFT JOIN (
                          SELECT * FROM pd_data.bwt_SALE_OUTLETS_DAY WHERE day_id  = ${prev_2}    -------------N-2
                                   ) B
                                 ON A.SALE_OUTLETS_CD = B.SALE_OUTLETS_CD
                           where  B.CHANNEL_TYPE_CD_3 IN ('120101',
                                                         '120102',
                                                         '120103',
                                                         '120201',
                                                         '120202',
                                                         '120301',
                                                         '120302',
                                                         '120303',
                                                         '120304',
                                                         '120401',
                                                         '120402',
                                                         '120501',
                                                         '120502',
                                                         '120503',
                                                         '120504',
                                                         '120505',
                                                         '120506',
                                                         '120507',
                                                         '120508',
                                                         '120509',
                                                         '120510',
                                                         '120511',
                                                         '120512',
                                                         '120513',
                                                         '120514',
                                                         '120515',
                                                         '120516')"""
    writeToOracle(RESULT_SQL, "HT_CHANNEL_RESULT")

    echannel(yestoday,prev_2)
  }

}

object BackUpTD {

  def apply(@param spark: SparkSession): BackUpTD = new BackUpTD(spark)

  def main(args: Array[String]): Unit = {
    val spark = AppUtils.createDefaultSession("hot_area_backup_td")
    val backUpTD = BackUpTD(spark)
    if (args.length == 1)
      backUpTD.dayResult(args(0))
    else
      backUpTD.dayResult()
  }
}