package com.atguigu.day08

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StructType}

class MyAvgWeak extends UserDefinedAggregateFunction{

  /**
    * 指定输入数据的参数类型
    * @return
    */
  override def inputSchema: StructType = {
    //当前udaf函数是针对年龄求平均值,所以输入参数就是年龄
    new StructType()
      .add("input",IntegerType)
  }

  /**
    * 指定udaf函数计算过程中中间变量的类型【sum,count】
    * @return
    */
  override def bufferSchema: StructType = {
    new StructType()
      .add("x",IntegerType)
      .add("y",IntegerType)
  }

  /**
    * 指定udaf最终计算结果类型
    * @return
    */
  override def dataType: DataType = DoubleType

  /**
    * 指定一致性[同样的输入是否返回同样的输出]
    * @return
    */
  override def deterministic: Boolean = true

  /**
    * 指定中间变量的初始值【sum=0,count=0】
    * @param buffer
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // sum = 0
    buffer(0) = 0
    //count = 0
    buffer(1) = 0
  }

  /**
    * combiner计算过程【预聚合计算】
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    println(s"update计算: ${Thread.currentThread().getName} ------> 之前计算结果=${buffer}  当前待计算的元素=${input}")
    // sum = sum + age
    buffer(0) = buffer.getAs[Int](0) + input.getAs[Int](0)
    //count = count + 1
    buffer(1) = buffer.getAs[Int](1) + 1
  }

  /**
    * reducer计算过程[全局汇总计算]
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    println(s"merge计算: ${Thread.currentThread().getName} ------> 之前计算结果=${buffer1}  当前待计算的元素=${buffer2}")
    //sum  = 之前的sum + combiner结果中的sum
    buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)

    //count = 之前的count + combiner结果中的count
    buffer1(1) = buffer1.getAs[Int](1) + buffer2.getAs[Int](1)
  }

  /**
    * 计算最终结果
    * @param buffer
    * @return
    */
  override def evaluate(buffer: Row): Any = {
    // avg = sum/count
    buffer.getAs[Int](0).toDouble / buffer.getAs[Int](1)
  }
}
