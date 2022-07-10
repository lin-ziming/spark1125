package com.atguigu.day06

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * 自定义累加器
  *     1、创建class继承AccumulatorV2[IN,OUT]
  *           IN: 代表累加的元素类型
  *           OUT: 代表最终结果类型
  *     2、重写抽象方法
  *自定义累加器的使用
  *    1、创建累加器对象
  *    2、将累加器对象注册到sparkcontext中
  *    3、将累加器对象在spark算子中累加元素
  *    4、获取最终结果
  *
  */
class WordCountAccumulator extends AccumulatorV2[(String,Int),mutable.Map[String,Int]]{
  //状态中间结果的容器
  var map = mutable.Map[String,Int]()
  /**
    * 代表累加器是否为空
    * @return
    */
  override def isZero: Boolean = map.isEmpty

  /**
    * 复制累加器
    * @return
    */
  override def copy(): AccumulatorV2[(String, Int), mutable.Map[String, Int]] = new WordCountAccumulator

  /**
    * 重置累加器
    */
  override def reset(): Unit = map.clear()

  /**
    * 累加元素[在task中累加]
    * @param v
    */
  override def add(v: (String, Int)): Unit = {
    println(s"add计算: ${Thread.currentThread().getName} -- 当前计算数据=${v}  --之前中间结果状态: ${map}")
    //判断当前单词在容器中是否存在,如果不存在直接添加到中间容器中
    //if( !map.contains(v._1) ){
    //  map.put(v._1, v._2)
    //}else{
    ////如果存在需要将该单词之前的结果与当前的次数累加
    //val num = map.get(v._1).get
    //  map.put(v._1, num+v._2)
    //}

    val num = map.getOrElse(v._1,0) + v._2

    map.put(v._1,num)
  }

  /**
    * 合并结果[在Driver中合并task累加器结果]
    * @param other
    */
  override def merge(other: AccumulatorV2[(String, Int), mutable.Map[String, Int]]): Unit = {
    println(s"merge计算: ${Thread.currentThread().getName} -- task累加数据=${other.value}  --之前Driver中间结果状态: ${map}")
      //取出task累加的结果
    val taskMap = other.value

    taskMap.foreach{
      case (wc,num) =>
        val totalNum = map.getOrElse(wc,0) + num
        map.put(wc,totalNum)
       // add((wc,num))
    }
  }

  /**
    * 获取最终结果
    * @return
    */
  override def value: mutable.Map[String, Int] = map
}
