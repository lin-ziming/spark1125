package com.atguigu.day06

import org.junit.Test

class $02_ReadWrite {

  import org.apache.spark.{SparkConf, SparkContext}
  val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("test"))
  /**
    * 数据的保存
    */
  @Test
  def write(): Unit ={

    val rdd = sc.parallelize(List(1,3,5,6,7,8,10))
    //保存数据为文本文件
    //rdd.saveAsTextFile("output/text")
    //保存为对象文件
    //rdd.saveAsObjectFile("output/obj")
    //保存为序列文件
    //rdd.map(x=>(x,1)).saveAsSequenceFile("output/seq")
    //rdd.map(x=>(x,1)).saveAsNewAPIHadoopFile()
  }

  /**
    * 读取文件
    */
  @Test
  def read(): Unit ={
    //读取文本
    println(sc.textFile("datas/wc.txt").collect().toList)
    //读取对象文件
    println(sc.objectFile[Int]("output/obj").collect().toList)
    //读取序列文件
    println(sc.sequenceFile[Int, Int]("output/seq").collect().toList)
    //sc.had
  }
}
