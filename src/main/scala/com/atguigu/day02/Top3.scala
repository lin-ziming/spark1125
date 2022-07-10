package com.atguigu.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Top3 {

  /**
    * 统计出农产品种类数最多的三个省份
    *
    */
  def main(args: Array[String]): Unit = {
    //1、创建sparkcontext对象
    val conf = new SparkConf().setMaster("local[4]").setAppName("TEST")
    //setMaster("local[4]")如果打包提交到集群執行不需要設置,后续提交的时候可以通过bin/spark-submit --master设置
    val sc = new SparkContext(conf)

    //2、读取数据
    val rdd1 = sc.textFile("datas/product.txt")
    //3、是否要过滤【要】
    val rdd2 = rdd1.filter(line=> line.split("\t").size==6)
    // 4、是否要列裁剪【省份、菜名】
    val rdd3 = rdd2.map(line=>{
      val arr = line.split("\t")
      val province = arr(4)
      val name = arr.head
      (province,name)
    })
    //
    //RDD( (广东省,西红柿),(广东省,大白菜),(广东省,西红柿)，(湖南省,西蓝花),(湖北省,青椒),(广东省,西红柿)，(广东省,上海青),.....  )
    // 5、是否要去重【要】
    val rdd4: RDD[(String, String)] = rdd3.distinct()
    //RDD( (广东省,西红柿),(广东省,大白菜),(湖南省,西蓝花),(湖北省,青椒),(广东省,上海青),.....  )

    //6、按照省份分组
    val rdd5 = rdd4.groupBy(x=> x._1)
    //RDD(
    //    广东省 -> Iterable( (广东省,西红柿),(广东省,大白菜),(广东省,上海青),...)
    //    湖南省 -> Iterable( (湖南省,西蓝花),.... )
    // )
    //7、统计每个省份菜的种类数
    val rdd6 = rdd5.map(x=>{
      //x =  广东省 -> Iterable( (广东省,西红柿),(广东省,大白菜),(广东省,上海青),...)
      (x._1,x._2.size)
    })
    //RDD(
    //    广东省->15,湖南省->23,湖北省->33,广西省->21,....
    // )
    //8、排序取前三
    val rdd7 = rdd6.sortBy(x=>x._2,false)

     val result =  rdd7.collect()

    val top3 = result.take(3)

    top3.foreach(println(_))

    Thread.sleep(1000000)

  }
}
