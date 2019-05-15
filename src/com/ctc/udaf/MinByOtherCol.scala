package com.ctc.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


class MinByOtherCol extends UserDefinedAggregateFunction {
  override def inputSchema: StructType =  StructType(Array(StructField("input_index", IntegerType, true),StructField("input_binder", StringType, true)))

  override def bufferSchema: StructType = StructType(Array(StructField("output_index", IntegerType, true),StructField("output_binder", StringType, true)))

  override def dataType: DataType =  StructType(Array(StructField("max_index", IntegerType, true),StructField("max_binder", StringType, true)))


  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit ={
    buffer(0)=60
    buffer(1)=""
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val new_index = input.getInt(0)
    val old_index = buffer.getInt(0)
    if(new_index<old_index){
      buffer.update(0,new_index)
      buffer.update(1,input.getString(1))
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val new_index = buffer2.getInt(0)
    val old_index = buffer1.getInt(0)
    if(new_index<old_index){
      buffer1.update(0,new_index)
      buffer1.update(1,buffer2.getString(1))
    }
  }

  override def evaluate(buffer: Row): Any = {
    buffer
  }

}
