package com.atguigu.day10

object $07_Window {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.SparkConf
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    val conf = new SparkConf().setMaster("local[4]").setAppName("test")/*.set("spark.streaming.backpressure.enabled","true")*/
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.sparkContext.setLogLevel("warn")
    ssc.checkpoint("checkpoint")
    val ds = ssc.socketTextStream("hadoop102",9999)

    val ds2 = ds.flatMap(x=>x.split(" "))

    val ds3 = ds2.map(x=>{
      (x,1)
    })
    //打印当前批次数据:
    ds3.map(x=>(s"----->${x._1}",1)).print()

    //
    //window(窗口长度,滑动长度): 后续以窗口里面的数据计算
    //TODO 窗口长度与滑动长度必须是批次时间的整数倍
    //val ds4 = ds3.window( Seconds(15), Seconds(5))
//
    //val ds5 = ds4.reduceByKey((agg,curr)=>{
    //  println(s"窗口计算: agg=${agg} curr=${curr}")
    //  agg+curr
    //})
    //reduceByKeyAndWindow = window + reduceByKey
    //val ds5 = ds3.reduceByKeyAndWindow( (agg,curr)=>{
    //  println(s"窗口计算: agg=${agg} curr=${curr}")
    //  agg+curr
    //}, windowDuration = Seconds(15), Seconds(5) )
    //TODO 如果两个窗口中存在大量的重复批次,此时下一个窗口的结果 = 上一个窗口的结果-滑出批次结果+滑入的批次结果,这样可以避免两个窗口重复批次的重复计算,提高计算效率
    val ds5 = ds3.reduceByKeyAndWindow( (agg,curr)=>{
      println(s"滑入: agg=${agg} curr=${curr}")
      agg + curr
    },(agg,curr)=>{
      println(s"滑出: agg=${agg} curr=${curr}")
      agg - curr
    } ,Seconds(20), Seconds(5) )
    //打印窗口计算结果
    ds5.print

    ssc.start()
    ssc.awaitTermination()
  }
}
