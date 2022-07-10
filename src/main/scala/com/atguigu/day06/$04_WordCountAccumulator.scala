package com.atguigu.day06

object $04_WordCountAccumulator {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.{SparkConf, SparkContext}
    val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("test"))
    //创建累加器对象
    val acc = new WordCountAccumulator

    //注册累加器
    sc.register(acc,"acc")
    val rdd1 = sc.textFile("datas/wc.txt")

    val rdd2 = rdd1.flatMap(_.split(" "))

    val rdd3 = rdd2.map(x=>(x,1))

    rdd3.mapPartitionsWithIndex((index,it)=>{
      println(s"index=${index} it=${it.toList}")
      it
    }).collect()

    //在spark算子中累加元素
    val rdd4 = rdd3.foreach(x=> acc.add(x))

    //获取最终结果
    println(acc.value)
    //println(rdd4.collect().toList)
  }
}
