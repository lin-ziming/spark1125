package com.atguigu.day05

object $05_Lineage {

  /**
    * 血统: 指从第一个RDD到当前RDD的链条
    *   toDebugString可以查看血统
    */
  def main(args: Array[String]): Unit = {

    import org.apache.spark.{SparkConf, SparkContext}
    val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("test"))
    println("++++++++++++++++++++++++++++++++++++++++++++++++++")
    val rdd1 = sc.textFile("datas/wc.txt")
    println(rdd1.toDebugString)
    println("---------------------------------------------------")
    val rdd2 = rdd1.flatMap(_.split(" "))
    println(rdd2.toDebugString)
    println("---------------------------------------------------")
    val rdd3 = rdd2.map(x=>(x,1))
    println(rdd3.toDebugString)
    println("---------------------------------------------------")
    val rdd4 = rdd3.reduceByKey(_+_)
    println(rdd4.toDebugString)
    println("---------------------------------------------------")
    println(rdd4.collect().toList)
  }
}
