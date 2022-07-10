package com.atguigu.day10

import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object $04_KafkaSource {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.SparkConf
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    val conf = new SparkConf().setMaster("local[4]").setAppName("test")/*.set("spark.streaming.backpressure.enabled","true")*/
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.sparkContext.setLogLevel("warn")

    //设置读取的topic的名称
    val topics = Array[String]("sparksql")
    //设置消费者相关配置
    val map = Map[String,Object](
      //设置key的反序列化器
      "key.deserializer"-> "org.apache.kafka.common.serialization.StringDeserializer",
      //设置value的反序列化器
      "value.deserializer"-> "org.apache.kafka.common.serialization.StringDeserializer",
      //设置kafka集群地址
      "bootstrap.servers"->"hadoop102:9092,hadoop103:9092",
      //设置消费者组的id
      "group.id" -> "g2",
      //设置消费者第一次消费该topic的时候从哪个位置开始拉取数据[earliest-从offset=0的位置拉取,latest-从最后一个offset+1的位置拉取]
      "auto.offset.reset" -> "earliest",
      //是否自动提交offset记录保存到__consumer_offsets中
    "enable.auto.commit" -> "true"
    )
    val ds = KafkaUtils.createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](topics,kafkaParams = map))
    //foreachRdd(func: RDD=>Unit):Unit
    //  foreachRdd里面的函数是针对每个批次的RDD操作。
    ds.foreachRDD(rdd=>{
      println(rdd.getNumPartitions)
      //sparkstreaming中批次RDD的分区数 = topic分区数,一个RDD的分区相当于消费者组中的一个消费者
      //当Topic分区数改变的时候,RDD的分区数会动态改变
      //模拟数据处理
      val rdd2 = rdd.map(x=> x.value() ).flatMap(_.split(" ")).map(x=>(x,1)).reduceByKey(_+_)

      rdd2.collect().foreach(println)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
