package com.atguigu.day10

object $05_Transform {

  def main(args: Array[String]): Unit = {
    //创建StreamingContext
    import org.apache.spark.SparkConf
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    val conf = new SparkConf().setMaster("local[4]").setAppName("test")/*.set("spark.streaming.backpressure.enabled","true")*/
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.sparkContext.setLogLevel("warn")
    //从数据源读取数据
    val ds = ssc.socketTextStream("hadoop102",9999)
    //切割+压平
    //transform(func: RDD[元素类型]=>RDD[T]) : 一对一转换[一个批次的RDD转换得到新的RDD]
    //  transform里面的函数针对是当前DStream中每个批次操作
    val ds2 = ds.transform(rdd=> {

      rdd.flatMap(_.split(" ")).map(x=>(x,1)).reduceByKey(_+_)
    })
    //结果打印
    ds2.print()
    //启动程序
    ssc.start()
    //阻塞
    ssc.awaitTermination()
  }
}
