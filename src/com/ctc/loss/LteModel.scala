package com.ctc.loss

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import com.ctc.util.FeatureUtil._
import com.ctc.util.HDFSUtil._
import com.ctc.util.DateUtil.{get_four_month, get_three_month, next_month}
import com.ctc.util.INSTANTCE.{TD_PROPERTY, TD_URL}
import org.apache.spark.storage.StorageLevel

class LteModel (@transient spark:SparkSession)extends Serializable {
  val PIPLINE_PATH = "model/cdma_pipline"
  val LABEL_PATH = "label/loss_cdma/%s"
  val ID_COLUMN = "PRD_INST_ID"
  def getData(dfs:List[DataFrame],split_index:Int=27):DataFrame = {
    val now = new Date()
    def udf_parse_date = udf((date:Date)=>{
      if(date==null)
        -1
      else
        (now.getTime - date.getTime)/(1000*24*3600)
    })
    val suffix = List("_A","_B","_C")
    val names = dfs(2).columns.toList
    val common_name = List("DEVICE_TERM_TYPE_ID","COUNTRY_FLAG",
      "PAY_MODE_ID","BILLING_TYPE_ID","SUB_STRATEGY_SEGMENT_ID","CDE_CLUSTER_TYPE_ID",
      "AGE","GENDER_ID","EXCHANGE_ID","OFR_ID","ACCT_OPEN_DATE","INNET_DUR",
      "OLD_PRD_INST_TYPE_ID","LATN_ID","GROUP_REGION_ID","VIP_FLAG","CUST_TYPE_ID",
      "MARKET_MANAGER_ID","INDUS_TYPE_ID","CENTREX_FLAG","USER_TYPE_ID")
    val new_name = names.drop(split_index+1)
    val x = dfs(2).select(ID_COLUMN,common_name:_*)
    val common_df = x.withColumn("ACCT_OPEN_DAY",udf_parse_date(x("ACCT_OPEN_DATE"))).drop("ACCT_OPEN_DATE")
    (common_df+:dfs.map(_.select(ID_COLUMN,new_name:_*)).zip(suffix).map{
      case (df,s) => df.toDF(ID_COLUMN+:new_name.map(_+s):_*)
    }).reduce(_.join(_,Seq(ID_COLUMN),"left"))
  }

  def getTrainData(month:String):DataFrame={
    val months = get_four_month(month)
    val it = months.productIterator.toList.asInstanceOf[List[String]].map(CDMA_DATA_PATH.format(_))
    val dfs = for(i<-it) yield {
      val x = spark.read.parquet(i)
      x.toDF(x.columns.map(_.toUpperCase).toSeq:_*)
    }
    //获取LABEL字段
    var label_df = dfs(2).select(dfs(2)(ID_COLUMN),dfs(2)("STD_PRD_INST_STAT_ID").as("PREV"))
      .join(
        dfs(3).select(dfs(3)(ID_COLUMN),dfs(3)("STD_PRD_INST_STAT_ID").as("CURRENT")),
        Seq(ID_COLUMN),
        "left"
      )
    label_df = label_df.select(label_df(ID_COLUMN),drawLable(label_df("PREV"),label_df("CURRENT")).as("LABEL"))
    //获取训练数据
    val train = label_df.join(getData(dfs.dropRight(1)),Seq(ID_COLUMN),"left")
    train
  }


  def getPredictData(month: String):DataFrame = {
    val months = get_three_month(month)
    val it = months.productIterator.toList.asInstanceOf[List[String]].map(CDMA_DATA_PATH.format(_))
    val dfs = for(i<-it) yield {
      val x = spark.read.parquet(i)
      x.toDF(x.columns.map(_.toUpperCase).toSeq:_*)
    }
    //获取LABEL字段
    var label_df = dfs(1).select(dfs(1)(ID_COLUMN),dfs(1)("STD_PRD_INST_STAT_ID").as("PREV"))
      .join(
        dfs(2).select(dfs(2)(ID_COLUMN),dfs(2)("STD_PRD_INST_STAT_ID").as("CURRENT")),
        Seq(ID_COLUMN),
        "left"
      )
    label_df = label_df.select(label_df(ID_COLUMN),drawLable(label_df("PREV"),label_df("CURRENT")).as("LABEL"))
    //获取训练数据
    val train = label_df.join(getData(dfs.dropRight(1)),Seq(ID_COLUMN),"left")
    train
  }

  def getTrainAndPredictData(month:String):(DataFrame,DataFrame) = {
    val months = get_four_month(month)
    val it = months.productIterator.toList.asInstanceOf[List[String]].map(CDMA_DATA_PATH.format(_))
    val dfs = for(i<-it) yield {
      val x = spark.read.parquet(i)
      x.toDF(x.columns.map(_.toUpperCase).toSeq:_*)
    }
    //获取LABEL字段
    var label_df = dfs(2).select(dfs(2)(ID_COLUMN),dfs(2)("STD_PRD_INST_STAT_ID").as("PREV"))
      .join(
        dfs(3).select(dfs(3)(ID_COLUMN),dfs(3)("STD_PRD_INST_STAT_ID").as("CURRENT")),
        Seq(ID_COLUMN),
        "left"
      )
    label_df = label_df.select(label_df(ID_COLUMN),drawLable(label_df("PREV"),label_df("CURRENT")).as("LABEL"))
    //获取训练数据
    val train = label_df.join(getData(dfs.dropRight(1)),Seq(ID_COLUMN),"left")
    //获取预测数据
    val predict = label_df.join(getData(dfs.drop(1)),Seq(ID_COLUMN),"left")
    (train,predict)
  }



  def processByPipline(df:DataFrame,features:Array[String],is_new:Boolean): DataFrame = {
    val format = new SimpleDateFormat("yyyyMM")
    val now = new Date()
    var r = fillWithZero(fillWithMost(df))
    val piplie = if(is_new){
      val index_col = df.schema.filter(_.dataType==StringType).map(_.name)
      println(index_col)
      val stages = index_col.map(i=>{
        new StringIndexer().setInputCol(i).setOutputCol(i+"_idx")
      }).toArray[PipelineStage]
      val pipeline = new Pipeline().setStages(stages)
      val pipModel = pipeline.fit(r)
      pipModel.write.overwrite().save(PIPLINE_PATH)
      pipModel
    }else{
      PipelineModel.load(PIPLINE_PATH)
    }
    r = piplie.transform(r)
    for(i<-r.columns.filter(_.endsWith("_idx"))){
      r = r.drop(i.dropRight(4)).withColumnRenamed(i,i.dropRight(4))
    }
    r.select("LABEL",features:_*)
  }
  def saveData(month:String,df:DataFrame): String ={
    val path = CDMA_MODEL_DATA_PATH.format(month)
    df.coalesce(1).write.mode("overwrite").option("header","true").option("sep",",").csv(path)
    path
  }

  def process(month:String,update_pipline:Boolean=false):(String,String)={
    val (train,predict) = getTrainAndPredictData(month)
    try{
      train.persist(StorageLevel.MEMORY_AND_DISK_2)
      predict.persist(StorageLevel.MEMORY_AND_DISK_2)
      val train_month = next_month(month,-1)
      val features = train.drop("LABEL").columns
      println("处理TRAIN")
      val train_x = processByPipline(train,features,update_pipline)
      println("处理PREDICT")
      val predict_x = processByPipline(predict,features,false)
      val r = (saveData(train_month,train_x),saveData(month,predict_x))
      r
    }catch {
      case e:Exception => e.printStackTrace();("","")
    }finally {
      train.unpersist()
      predict.unpersist()
    }
  }

  def processTrain(month:String,update_pipline:Boolean=false):String = {
    val train = getTrainData( next_month(month,1))
    try{
      train.persist(StorageLevel.MEMORY_AND_DISK_2)
      val features = train.drop("LABEL").columns
      println("处理TRAIN")
      val train_x = processByPipline(train,features,update_pipline)
      saveData(month,train_x)
    }catch {
      case e:Exception => e.printStackTrace();""
    }finally {
      train.unpersist()
    }
  }


  def processPredict(month:String):String = {
    val train = getPredictData(month)
    try{
      train.persist(StorageLevel.MEMORY_AND_DISK_2)
      val features = train.drop("LABEL").columns
      println("处理TRAIN")
      val train_x = processByPipline(train,features,false)
      saveData(month,train_x)
    }catch {
      case e:Exception => e.printStackTrace();""
    }finally {
      train.unpersist()
    }
  }

  def getLabel(month:String):DataFrame={
    val path = LABEL_PATH.format(month)
    if(!exists(path)){
      val nMon=next_month(month,1)
      val struct = StructType(
        Array(
          StructField("Prd_Inst_Id",DecimalType(12,0)),
          StructField("Std_Prd_Inst_Stat_Id",IntegerType)
        )
      )
      val prev = spark.read.schema(struct).parquet(CDMA_DATA_PATH.format(month)).toDF(ID_COLUMN,"PREV")
      val next = spark.read.schema(struct).parquet(CDMA_DATA_PATH.format(nMon)).toDF(ID_COLUMN,"NEXT")
      val df = prev.join(next,Seq(ID_COLUMN),"inner")
      df.select(df(ID_COLUMN),drawLable(df("PREV"),df("NEXT")).as("LABEL")).coalesce(1).write.mode("overwrite").option("header","true").option("sep",",").csv(path)
    }
    spark.read.option("header","true").csv(path)
  }

  def checkModel(idPath:String,predict_month:String): Unit = {
    val job_month = next_month(predict_month,-1)
    val t_sql = s"""sel a.prd_inst_id,c.Outnet_Date from
                      (select prd_inst_id,ofr_id from PV_MART_Z.bas_prd_inst_month
                      where billing_cycle_id=${job_month} and Std_Prd_Inst_Stat_Id/100<>12 and substr(trim(std_prd_id),1,4)=1015) a
                      inner join  PV_MART_Z.BAS_PRD_INST_CUR c
                      on a.prd_inst_id = c.prd_inst_id
                      inner join (SELECT * FROM PV_DATA_Z.TMP_YUANLEI_ZC201803_XX WHERE latn_id not in (1004,1006) AND fz_flag = '1') d
                      on a.prd_inst_id = d.prd_inst_id
                  	inner join (sel * from td_work.lsyj_cdma_prd_id)e
                  	on a.prd_inst_id = e.prd_inst_id
                      where   CAST(c.Outnet_Date AS DATE FORMAT'yyyymmdd') between  '${job_month}31' and  '${predict_month}31'"""
    val df = spark.read.option("header","true").csv(idPath)
    df.select(df("PRD_INST_ID").cast(DecimalType(12,0))).write.mode("overwrite").jdbc(TD_URL,"td_work.lsyj_cdma_prd_id",TD_PROPERTY)
    val r = spark.read.jdbc(TD_URL,s"($t_sql)x",TD_PROPERTY).count()
    println(r)
    println(s"accuray:${r.toDouble / df.count}")
  }
}