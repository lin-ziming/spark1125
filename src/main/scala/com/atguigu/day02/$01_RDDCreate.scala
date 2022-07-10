package com.atguigu.day02


import org.junit.Test

class $01_RDDCreate() {


  import org.apache.spark.{SparkConf, SparkContext}
  val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("test"))
  /**
    * RDD的创建方式
    *     1、通过本地集合创建
    *     2、通过读取文件创建
    *     3、通过其他RDD衍生
    */

  /**
    * 通过集合创建RDD 【工作中一般用于测试】
    *     sc.makeRDD(集合)
    *     sc.parallelize(集合)
    */
  @Test //@Test注解必须用在class中,标准的方法必须是无返回值的方法
  def createRddByLocalCollection(){

    new $01_RDDCreate

    val list = List(1,4,3,2,5)

    //val rdd = sc.makeRDD(list)
    val rdd = sc.parallelize(list)

    val arr = rdd.collect()

    println(arr.toList)
  }

  /**
    * 通过读取文件创建RDD
    */
  @Test
  def createRddByFile(): Unit ={
    //System.setProperty("HADOOP_USER_NAME","atguigu")
    val rdd = sc.textFile("datas/product.txt")
    //读取HDFS文件
    //val rdd = sc.textFile("hdfs://hadoop102:8020/datas/products.txt")
    println(rdd.collect().toList)
  }

  /**
    * 3、通过其他RDD衍生
    */
  @Test
  def createRddByRdd(): Unit ={
    val rdd = sc.textFile("datas/product.txt")

    val rdd2 = rdd.flatMap(_.split("\t"))

    println(rdd2.collect().toList)
  }
}
