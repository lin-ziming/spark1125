package com.atguigu.day09

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable

/**
  *
  * @param totalNum  每个区域每个商品的总的点击次数
  * @param cityInfo  每个区域每个商品中每个城市的点击次数映射
  */
case class Top3Buff(totalNum:Int, cityInfo: mutable.Map[String,Int])

class Top3UDAF extends Aggregator[String,Top3Buff,String]{
  /**
    * 给中间变量赋予初始值
    * @return
    */
  override def zero: Top3Buff = {
    Top3Buff(0,mutable.Map[String,Int]())
  }

  /**
    * combiner计算逻辑
    * @param b
    * @param a
    * @return
    */
  override def reduce(agg: Top3Buff, city: String): Top3Buff = {

    //将每个区域每个商品每个城市之前统计的点击次数 + 1
    val cityNum = agg.cityInfo.getOrElse(city,0) + 1
    //更新城市点击次数
    agg.cityInfo.put(city,cityNum)

    Top3Buff( agg.totalNum + 1, agg.cityInfo )
  }

  /**
    * reducer计算逻辑
    * @param b1
    * @param b2
    * @return
    */
  override def merge(agg: Top3Buff, curr: Top3Buff): Top3Buff = {

    //总次数相加
    val totalNum = agg.totalNum + curr.totalNum

    //每个城市的次数相加
    curr.cityInfo.foreach{
      case (city,num) =>
        val cityNum = agg.cityInfo.getOrElse(city, 0) + num
        agg.cityInfo.put(city,cityNum)
    }

    Top3Buff(totalNum,agg.cityInfo )
  }

  /**
    * 计算最终结果
    * @param reduction
    * @return
    */
  override def finish(red: Top3Buff): String = {

    //取出总次数
    val totalNum = red.totalNum
    //totalNum = 57
    //取出每个城市的次数
    val cityInfo = red.cityInfo
    //Map(深圳->10,北京->30,上海->12,广州->5)
    //对城市排序取出Top2
    val cityTop2 = cityInfo.toList.sortBy(_._2).reverse.take(2)
    //List(北京->30,上海->12)
    //计算Top2城市在占比
    val top2CityList = cityTop2.map{
      case (city,num) => s"${city}:${ ( (num.toDouble / totalNum) * 100 ).formatted("%.3f")}%"
    }
    //List("北京: 52.631%","上海:21.052%")

    val top2Res = top2CityList.mkString(",") //北京: 52.631%,上海:21.052%

    //其他城市占比
    val top2Num = cityTop2.map(_._2).sum
    val otherCity = ( ( (totalNum - top2Num).toDouble / totalNum ) * 100 ).formatted("%.3f")

    s"${top2Res},其他:${otherCity}%"
  }

  /**
    * 指定中间变量类型的编码格式
    * @return
    */
  override def bufferEncoder: Encoder[Top3Buff] = Encoders.product[Top3Buff]

  /**
    * 指定最终结果类型编码格式
    * @return
    */
  override def outputEncoder: Encoder[String] = Encoders.STRING
}
