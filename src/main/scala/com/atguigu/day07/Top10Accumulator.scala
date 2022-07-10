package com.atguigu.day07

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class Top10Accumulator extends AccumulatorV2[(String,(Int,Int,Int)),mutable.Map[String,(Int,Int,Int)]]{
  //创建一个存储中间容器
  val map = mutable.Map[String,(Int,Int,Int)]()

  //判断累加器是否为空
  override def isZero: Boolean = map.isEmpty

  //复制累加器
  override def copy(): AccumulatorV2[(String, (Int, Int, Int)), mutable.Map[String, (Int, Int, Int)]] = new Top10Accumulator

  //重置累加器
  override def reset(): Unit = map.clear()

  //在task中对每个元素累加
  override def add(v: (String, (Int, Int, Int))): Unit = {
    //取出当前task关于该品类id的统计次数
    val beforeNum = map.getOrElse(v._1, (0,0,0))
    //将之前的统计次数与当前的行为次数相加
    val totalNum = ( beforeNum._1 + v._2._1 , beforeNum._2 + v._2._2, beforeNum._3+v._2._3 )
    //更新中间结果中该品类的次数
    map.put(v._1, totalNum)
  }
  //Driver对每个task结果汇总
  override def merge(other: AccumulatorV2[(String, (Int, Int, Int)), mutable.Map[String, (Int, Int, Int)]]): Unit = {
    //取出task累加结果
    val taskMap = other.value

    taskMap.foreach{
      case ( id, (clickNum, orderNum, payNum) )  =>
        //取出当前task关于该品类id的统计次数
        val beforeNum = map.getOrElse(id, (0,0,0))
        //将之前的统计次数与当前的行为次数相加
        val totalNum = ( beforeNum._1 + clickNum , beforeNum._2 + orderNum, beforeNum._3+ payNum )
        //更新中间结果中该品类的次数
        map.put(id, totalNum)
    }
  }

  //取出结果
  override def value: mutable.Map[String, (Int, Int, Int)] = map
}
