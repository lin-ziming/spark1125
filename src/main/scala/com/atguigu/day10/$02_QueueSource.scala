package com.atguigu.day10

import org.apache.spark.rdd.RDD

import scala.collection.mutable

object $02_QueueSource {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.SparkConf
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    val conf = new SparkConf().setMaster("local[4]").setAppName("test")
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.sparkContext.setLogLevel("warn")

    //从队列中读取数据
    val queue = mutable.Queue[RDD[String]]()
    // oneAtATime=true: 代表每次只从队列中获取一个RDD当做一个批次的RDD处理
    // oneAtATime=false: 代表每次批次之间取出批次时间内放入的所有的RDD,然后union之后当做一个批次的RDD处理
    val ds = ssc.queueStream(queue,false)
    //针对数据处理
    ds.flatMap(x=>x.split(" "))
      .map(x=>(x,1))
      .reduceByKey((agg,curr)=>agg+curr)
      .print()

    //启动
    ssc.start()

    for(i<- 1 to 100){
      val rdd = ssc.sparkContext.parallelize(List("hello java hello","hadoop flume flume","hello hello java"))
      queue.enqueue(rdd)
      Thread.sleep(2000)
    }
    //阻塞
    ssc.awaitTermination()
  }
}
