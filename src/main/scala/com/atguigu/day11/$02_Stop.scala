package com.atguigu.day11

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object $02_Stop {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.SparkConf
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    val conf = new SparkConf().setMaster("local[4]").setAppName("test")/*.set("spark.streaming.backpressure.enabled","true")*/
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.sparkContext.setLogLevel("warn")

    val ds = ssc.socketTextStream("hadoop102",9999)

    val ds2 = ds.flatMap(_.split(" "))
      .map(x=>(x,1))
      .reduceByKey(_+_)
      .map(x=>(s"-->${x._1}",x._2))

    ds2.print()


    ssc.start()

    //停止的条件应该通过监听外部的动作来触发
    //        1、可以监听HDFS指定目录是否删除来停止
    //        2、可以监听mysql某个表某个字段的值是否改变来停止

    val fs = FileSystem.get(new URI("hdfs://hadoop102:8020"),new Configuration())


    while(fs.exists(new Path("hdfs://hadoop102:8020/input"))){

      Thread.sleep(5000)
    }

    //stopGracefully=true 代表将接收到的数据处理完成之后才会停止
    //stopGracefully=false[默认] 代表立即停止,不关心数据是否处理完[有可能出现数据丢失的问题]
    ssc.stop(true,true)

    ssc.awaitTermination()

  }
}
