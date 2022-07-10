package com.atguigu.day05

object $06_Dependencys {

  /**
    *依赖: 是指父子RDD的关系
    *     依赖可以通过dependencies查看
    *  RDD的依赖分为两种: 宽依赖、窄依赖
    *     宽依赖: 有shuffle的称之为宽依赖
    *     窄依赖: 没有shuffle的称之为窄依赖
    *
    * Application: 应用,一个sparkcontext称之为一个应用
    *     Job: 任务[一般一个action算子产生一个job<除了first、take、takeOrdered例外>]
    *         Stage: 阶段[一个job中stage个数 = job中shuffle个数+1]
    *             Task: 子任务[一个stage中task个数 = stage中最后一个rdd的分区数]
    *
    * Job中多个stage之间是串行的,前面的stage先执行,后面的stage后执行
    * 一个stage中多个task之间是并行的
    *stage切分: 根据最后一个RDD的依赖从后向前依次查找,一直找到第一个rdd为止,在查询的过程中遇到宽依赖则切分stage.
    *
    *
    */
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("test"))
    println("++++++++++++++++++++++++++++++++++++++++++++++++++")
    val rdd1 = sc.textFile("datas/wc.txt")
    println(rdd1.dependencies)
    println("---------------------------------------------------")
    val rdd2 = rdd1.flatMap(_.split(" "))
    println(rdd2.dependencies)
    println("---------------------------------------------------")
    val rdd3 = rdd2.map(x=>(x,1))
    println(rdd3.dependencies)
    println("---------------------------------------------------")
    val rdd4 = rdd3.reduceByKey(_+_)
    println(rdd4.dependencies)
    println("---------------------------------------------------")
    println(rdd4.collect().toList)

    Thread.sleep(1000000)
  }
}
