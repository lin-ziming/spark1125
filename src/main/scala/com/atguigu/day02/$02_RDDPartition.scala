package com.atguigu.day02

import org.apache.spark.rdd.RDD
import org.junit.Test

class $02_RDDPartition {

  import org.apache.spark.{SparkConf, SparkContext}
  val sc = new SparkContext(new SparkConf()/*.set("spark.default.parallelism","6")*/.setMaster("local[4]").setAppName("test"))

  /**
    * 根据本地集合创建RDD的分区数
    *     如果有设置numSlices参数,此时RDD的分区数 = 设置的numSlice的值
    *     如果没有设置numSlices参数，此时RDD的分区数 = defaultParallelism
    *               如果在sparkconf中有设置spark.default.parallelism, defaultParallelism = spark.default.parallelism的值
    *               如果在sparkconf中没有设置spark.default.parallelism
    *                     master=local: defaultParallelism = 1
    *                     master=local[*]: defaultParallelism = cpu个数
    *                     master=local[N]: defaultParallelism = N
    *                     master=spark://... : defaultParallelism = math.max(所有executor总核数, 2)
    *
    *
    * 查看RDD的分区数: rdd.getNumPartitions/rdd.partitions.length
    */
  @Test
  def createRddByLocalCollectionPartition(): Unit ={

    //如果有设置numSlices参数,此时RDD的分区数 = 设置的numSlice的值
    val rdd: RDD[Int] = sc.parallelize(List(1,3,6,5,2,7,9,10),8)

    val arr = rdd.partitions
    println(rdd.getNumPartitions)
    println(arr.length)

    //如果在sparkconf中有设置spark.default.parallelism, defaultParallelism = spark.default.parallelism的值
    val rdd2: RDD[Int] = sc.parallelize(List(1,3,6,5,2,7,9,10))
    println(rdd2.getNumPartitions)

    val func = (index:Int,data:Iterator[Int]) => {
      println(s"分区号=${index} 分区数据=${data.toList}")
      data
    }
    //val rdd3 = rdd2.mapPartitionsWithIndex(func)
    val rdd3 = rdd2.mapPartitionsWithIndex((index,data) => {
      println(s"分区号=${index} 分区数据=${data.toList}")
      data
    })

    rdd3.collect
  }

  /**
    * 通过读取文件创建RDD的分区数
    *     如果有设置minPartitions的值,RDD的分区数>= minPartitions的值
    *     如果没有设置minPartitins的值,RDD的分区数>=math.min(defaultParallelism, 2)
    * 通过读取文件创建RDD的分区数最终由文件的切片数决定,一个切片一个分区
    */
  @Test
  def createRDDByFilePartitions(): Unit ={

    val rdd = sc.textFile("datas/wc.txt",4)

    println(rdd.getNumPartitions)
  }

  /**
    * 由其他RDD衍生出的新RDD的分区数 = 依赖的第一个父RDD的分区数
    */
  @Test
  def createRddByRddPartitions(): Unit ={
    val rdd = sc.textFile("datas/wc.txt",4)

    val rdd2 = rdd.flatMap(_.split(" "))

    println(rdd.getNumPartitions)
    println(rdd2.getNumPartitions)
  }
}
