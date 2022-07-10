package com.atguigu.day10

object $06_UpdateStateByKey {

  /**
    * updateStateByKey(func: (Seq[元素的value值类型],Option[U]) => Option[U]): 根据key统计全局结果[整个程序运行过程中该key的聚合结果]
    *     函数第一个参数代表对当前批次分组之后,该key所有的value值的集合
    *     函数第二个参数代表之前批次该key的统计结果
    *
    */
  def main(args: Array[String]): Unit = {

    import org.apache.spark.SparkConf
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    val conf = new SparkConf().setMaster("local[4]").setAppName("test")/*.set("spark.streaming.backpressure.enabled","true")*/
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.sparkContext.setLogLevel("warn")
    //设置状态的保存位置
    //状态不能丢失,丢失之后没办法回溯,所以必须保存到可靠的存储介质中
    ssc.checkpoint("checkpoint")

    val ds = ssc.socketTextStream("hadoop102",9999)

    val ds2 = ds.flatMap(_.split(" "))

    val ds3 = ds2.map(x=>{
      println(s"当前批次元素: ${x}")
      (x,1)
    })

    val func = (currentBatchValues: Seq[Int], state:Option[Int]) => {
      println(s"updateStateByKey计算过程: ${currentBatchValues}  ${state}")
      //得到当前批次中,key所有value的总和
      val currentBatchNum = currentBatchValues.sum
      //之前批次key的统计结果
      val beforeBatchNum = state.getOrElse(0)

      Some( currentBatchNum + beforeBatchNum  )
    }
    val ds4 = ds3.updateStateByKey(func)

    ds4.print

    ssc.start()
    ssc.awaitTermination()
  }
}
