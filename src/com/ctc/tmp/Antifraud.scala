package com.ctc.tmp

import java.sql.{Connection,CallableStatement,PreparedStatement,DriverManager}
import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, sum, date_format,when}
import org.apache.spark.sql.types._

import com.ctc.util.DateUtil.{getCurrentDay, next_day}
import com.ctc.util.AppUtils.createDefaultSession
import com.ctc.util.INSTANTCE.{anti_url2,properties}

class Antifraud(@transient spark: SparkSession) extends Serializable {
  val DAY_30 = 30 * 24 * 3600 * 1000L
  Class.forName("oracle.jdbc.driver.OracleDriver")

  def runProcedure(): String = {
    var conn: Connection = null
    var cs: CallableStatement = null
    var ps: PreparedStatement = null
    try {
      conn = DriverManager.getConnection(anti_url2, properties)
      ps = conn.prepareStatement("SELECT * FROM batch_t")
      val rs = ps.executeQuery()
      val batchNum = if (rs.next()) rs.getString(1) else ""
      cs = conn.prepareCall(s"call pro_anti_noc_list()")
      cs.execute()
      batchNum
    } catch {
      case e: Exception => e.printStackTrace(); ""
    } finally {
      if (ps != null) ps.close()
      if (cs != null) cs.close()
      if (conn != null) conn.close()
    }
  }

  def doJob(batchNum: String): Unit = {
    val today = getCurrentDay()
    val prev1 = next_day(today,-1)
    val prev60 = next_day(today, -60)
    val prev90 = next_day(today, -90)
    val prev150 = next_day(today, -150)
    //AND A.ACTIVE_DATE > TO_DATE('$prev90','yyyyMMdd')
    val table =
      s"""SELECT * FROM anti_noc_list_bak_t A
         |WHERE A.SHEET_NBR > $batchNum
         |AND A.WHITE_LIST_TYPE IS NULL
         |AND A.NEW_DATE > TO_DATE('$prev150','yyyyMMdd')""".stripMargin

    val df = spark.read.jdbc(anti_url2, s"($table)X", properties)

    val month = spark.read.parquet("hotarea_month").where(col("day").between(prev60, today)
        && col("rate_opp_area") > 3
        && col("hotareanum") > 0
        && col("rate_opp") / col("cnt_opp") > 0.5
        && col("cnt_opp") > 20
    ).withColumnRenamed("BILLING_NBR", "ACC_NBR")

    val callRateDF = spark.read.parquet(HotArea.RESULT_PATH).where("RATE_OPP > 0.5")


    var r = df.select("ACC_NBR").join(month, Seq("ACC_NBR"), "inner")
      .groupBy("ACC_NBR")
      .agg(
        count(col("hotareanum")).as("hotarea_day_cnt"),
        sum(col("hotareanum")).as("hotarea_sum")
      )
      .join(callRateDF,Seq("ACC_NBR"),"INNER")
      .join(df, Seq("ACC_NBR"), "left")
      .withColumn("NEW_DATE", date_format(col("NEW_DATE"), "yyyyMMdd"))
      .withColumn("ACTIVE_DATE", date_format(col("ACTIVE_DATE"), "yyyyMMdd"))
      .coalesce(50)

    val schema = r.schema

    for (i <- schema) {
      if (i.dataType.isInstanceOf[DecimalType]) {
        r = r.withColumn(i.name, col(i.name).cast(LongType))
      }
    }

    r.write.mode("overwrite").jdbc(anti_url2, "TMP_HOT_AREA_RESULT_3", properties)

  }

}

object Antifraud {

  def apply(spark: SparkSession): Antifraud = new Antifraud(spark)

  def main(args: Array[String]): Unit = {
    val spark = createDefaultSession("Antifraud")
    val antifraud = new Antifraud(spark)
    val batchnum = antifraud.runProcedure()
    if (batchnum != "") antifraud.doJob(batchnum)
  }

}