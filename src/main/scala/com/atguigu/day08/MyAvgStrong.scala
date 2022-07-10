package com.atguigu.day08

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

/**
  * 强类型自定义UDAF函数
  *     1、创建一个class继承Aggregator[IN,BUFF,OUT]
  *           IN: 代表函数参数类型[age int类型]
  *           BUFF: 中间变量类型
  *           OUT: 最终结果类型
  *     2、重写抽象方法
  * 使用:
  *     1、创建自定义强类型UDAF对象
  *     2、导入udaf函数: import org.apache.spark.sql.functions._
  *     2、使用udaf函数转换自定义UDAF对象
  *     3、在spark.sql中使用
  */

case class AvgBuff(sum:Int,count:Int)

class MyAvgStrong extends Aggregator[Int,AvgBuff,Double]{
  /**
    * 对中间结果变量赋予初始值
    * @return
    */
  override def zero: AvgBuff = {
    AvgBuff(0,0)
  }

  /**
    * combiner计算
    * @param b
    * @param a
    * @return
    */
  override def reduce(agg: AvgBuff, age: Int): AvgBuff = {

    AvgBuff( agg.sum + age, agg.count + 1 )
  }

  /**
    *
    * @param b1
    * @param b2
    * @return
    */
  override def merge(agg: AvgBuff, curr: AvgBuff): AvgBuff = {

    AvgBuff( agg.sum + curr.sum , agg.count + curr.count )
  }

  /**
    * 计算最终结果
    * @param reduction
    * @return
    */
  override def finish(reduction: AvgBuff): Double = reduction.sum.toDouble / reduction.count

  /**
    * 指定中间变量类型编码格式
    * @return
    */
  override def bufferEncoder: Encoder[AvgBuff] = Encoders.product[AvgBuff]

  /**
    * 指定结果类型的编码格式
    * @return
    */
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
