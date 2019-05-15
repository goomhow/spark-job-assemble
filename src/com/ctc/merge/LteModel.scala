package com.ctc.merge

import com.ctc.util.DateUtil.next_month
import com.ctc.util.HDFSUtil._
import com.ctc.util.INSTANTCE.{TD_PROPERTY, TD_URL}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}


class LteModel(@transient spark:SparkSession)extends Serializable {

  val ID_COLUMN = "PRD_INST_ID"
  val CUSTID_STRUCT =StructType(
    Seq(
      StructField("Acct_Id",DecimalType(12,0),true),
      StructField("Prd_Inst_Id",DecimalType(12,0),true)
    )
  )

  val ID_STRUCT =StructType(
    Seq(
      StructField("Prd_Inst_Id",DecimalType(12,0),true),
      StructField("Ofr_Id",DecimalType(12,0),true)
    )
  )

  val DAN_CDMA_LABEL_PATH = "dan/cdma_label/%s"
  def cdmaDanFlag(month:String): DataFrame ={
    val cdma = spark.read.schema(CUSTID_STRUCT).parquet(CDMA_DATA_PATH.format(month)).toDF("ACCT_ID","PRD_INST_ID_A")
    val bd = spark.read.schema(CUSTID_STRUCT).parquet(BD_DATA_PATH.format(month)).toDF("ACCT_ID","PRD_INST_ID_B")
    cdma.join(bd,Seq("ACCT_ID"),"left").where("PRD_INST_ID_B IS NULL").select("PRD_INST_ID_A").toDF(ID_COLUMN)
  }

  def getLabel(month:String,update:Boolean=false):DataFrame = {
    val path = DAN_CDMA_LABEL_PATH.format(month)
    if(update || !exists(path)){
      def f0 = udf(()=>0)
      def f1 = udf(()=>1)
      val m4 = next_month(month,1)
      val dan3 = cdmaDanFlag(month)
      val df =if(exists(BD_DATA_PATH.format(m4))){
        val dan4 = cdmaDanFlag(m4).withColumn("LABEL",f0())
        val merge4 = spark.read.schema(ID_STRUCT).parquet(PRD_PRD_INST_EXT_MON.format(m4)).toDF(ID_COLUMN).withColumn("LABEL",f1())
        dan3.join(dan4.union(merge4),Seq(ID_COLUMN),"left")
      }else{
        dan3.toDF(ID_COLUMN).withColumn("LABEL",f0())
      }
      df.dropDuplicates(ID_COLUMN).write.mode("overwrite").parquet("tmp/pid")
      spark.read.parquet("tmp/pid").coalesce(1).write.mode("overwrite").option("sep",",").option("header","true").csv(path)
    }
    spark.read.option("header","true").csv(path)
  }

  def checkModel(idPath:String,predict_month:String): Unit ={
    val job_month = next_month(predict_month,-1)
    val td_tabel = "td_work.dan_cdma_zr"
    val t_sql = s"""sel a.PRD_INST_ID from $td_tabel a
                  inner join (select prd_inst_id,ofr_id from PV_MART_Z.bas_prd_inst_month
                  where billing_cycle_id=$job_month and Std_Prd_Inst_Stat_Id/100<>12 and substr(trim(std_prd_id),1,4)=1015
                  and acct_id not in (SELECT ACCT_ID FROM pv_mart_z.bas_prd_inst_month a where a.billing_cycle_id=$job_month
                  and a.Std_Prd_Inst_Stat_Id/100<>12 and substr(trim(a.std_prd_id),1,4)=3020)
                  and ofr_id not in (select ofr_id from PV_DATA_Z.prd_ofr where ofr_name like '%十全十美%' or ofr_name like '%不限量%'))c
                  on a.prd_inst_id = c.prd_inst_id
                  inner join
                  (select prd_inst_id,ofr_id from PV_MART_Z.bas_prd_inst_cur
                  where ofr_id  in (select ofr_id from PV_DATA_Z.prd_ofr where ofr_name like '%十全十美%' or ofr_name like '%不限量%') )d
                  on a.prd_inst_id = d.prd_inst_id"""
    val df = spark.read.option("header","true").csv(idPath)
    df.select(df("PRD_INST_ID").cast(DecimalType(12,0))).write.mode("overwrite").jdbc(TD_URL,td_tabel,TD_PROPERTY)
    val r = spark.read.jdbc(TD_URL,s"($t_sql)x",TD_PROPERTY).count()
    println(r)
    println(s"accuray:${r.toDouble / df.count}")
  }

}