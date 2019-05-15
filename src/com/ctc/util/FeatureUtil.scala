package com.ctc.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

import scala.collection.mutable

object FeatureUtil {
  def fillWithMost(df:DataFrame,col:Seq[String]=null):DataFrame={
    val cols = if(col==null){
      df.schema.filter(_.dataType==StringType).map(_.name)
    }else{
      col
    }
    val most_freq = df.stat.freqItems(cols,1).take(1)(0).toSeq.map{
      case a:mutable.WrappedArray[String] => a(0)
    }
    val map = cols.zip(most_freq).toMap
    println("fillna Map:")
    println(map)
    df.na.fill(map)
  }
  def fillWithZero(df:DataFrame,col:Seq[String]=null):DataFrame={
    val cols = if(col==null){
      df.schema.filter(r =>{
        val t = r.dataType
        t.isInstanceOf[DecimalType]||t==DoubleType||t==IntegerType||t==LongType||t==FloatType||t==ShortType
      }).map(_.name)
    }else{
      col
    }
    df.na.fill(0,cols)
  }
  val drawLable = udf((prev:Int,current:Int)=>{
    val error = Set(-1,9901)
    val using = Set(1001,1002,1101,1102,1103)
    val noUsing = Set(1201,1202)
    if(noUsing.contains(current)&&using.contains(prev)){
      1
    }else if(error.contains(prev)||error.contains(current)){
      -1
    }else{
      0
    }
  })
}
