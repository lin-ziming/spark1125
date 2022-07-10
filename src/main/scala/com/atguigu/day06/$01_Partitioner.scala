package com.atguigu.day06

import org.apache.spark.{HashPartitioner, RangePartitioner}
import org.junit.Test

class $01_Partitioner {

  import org.apache.spark.{SparkConf, SparkContext}
  val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("test"))

  /**
    * spark的分区器默认有两种:
    *     HashPartitioner
    *         分区规则: key.hashCode % 分区数 <0 ?  (key.hashCode % 分区数) + 分区数 : key.hashCode % 分区数
    *     RangePartitioner
    *         分区规则:
    *             1、对RDD所有数据的key抽样,通过采样的结果确定 分区数-1 个key
    *             2、通过这些采样的key确定rdd每个分区的边界
    *             3、后续拿到数据的key之后与分区边界对比,如果key处于分区边界范围内则将数据放入该分区中
    */
  @Test
  def hashPartitioner(): Unit ={

    val rdd1 = sc.parallelize(List( 1->"a",3 -> "b",5->"c",10->"p",7->"t",4->"g" ))

    val rdd2 = rdd1.partitionBy( new HashPartitioner(3) )

    rdd2.mapPartitionsWithIndex((index,it)=>{
      println(s"index:${index} it=${it.toList}")
      it
    }).collect()
  }

  @Test
  def rangePartitioner(): Unit ={

    val rdd1 = sc.parallelize(List( 1->"a",3 -> "b",5->"c",10->"p",7->"t",4->"g" ))

    val rdd2 = rdd1.partitionBy( new RangePartitioner(3,rdd1) )

    rdd2.mapPartitionsWithIndex((index,it)=>{
      println(s"index:${index} it=${it.toList}")
      it
    }).collect()
  }
}
