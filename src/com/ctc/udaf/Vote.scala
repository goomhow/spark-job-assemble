package com.ctc.udaf
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class Vote extends UserDefinedAggregateFunction {
  override def inputSchema: StructType =  StructType(Array(StructField("input", StringType, true)))

  override def bufferSchema: StructType = StructType(Array(StructField("output", MapType(StringType,IntegerType), true)))

  override def dataType: DataType =  StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0)=Map[String,Int]()

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val key = input.getString(0)
    val old = buffer.getMap[String,Int](0)
    val value = old.getOrElse(key,0)+1
    buffer.update(0,old.updated(key,value))
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val map1 = buffer1.getMap[String,Int](0)
    val map2 = buffer2.getMap[String,Int](0)
    val map = (map1.toList:::map2.toList).groupBy(_._1).mapValues(_.map(_._2).sum).toMap[String,Int]
    buffer1.update(0,map)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getMap[String,Int](0).maxBy(_._2)._1
  }
}
