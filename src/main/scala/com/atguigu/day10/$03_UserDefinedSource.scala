package com.atguigu.day10

object $03_UserDefinedSource {

  def main(args: Array[String]): Unit = {
    //创建StreamingContext
    import org.apache.spark.SparkConf
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    val conf = new SparkConf().setMaster("local[4]").setAppName("test")/*.set("spark.streaming.backpressure.enabled","true")*/
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.sparkContext.setLogLevel("warn")
    //从数据源读取数据
    val ds = ssc.receiverStream(new MySocketReceiver("hadoop102",9999))
    //切割+压平
    val ds2 = ds.flatMap(x=>{
      //Thread.sleep(6000)
      x.split(" ")
    })
    //转换数据
    val ds3 = ds2.map(x=>(x,1))
    //统计单词个数
    val ds4 = ds3.reduceByKey((agg,curr)=>agg+curr)
    //结果打印
    ds4.print()
    //启动程序
    ssc.start()
    //阻塞
    ssc.awaitTermination()
  }
}
