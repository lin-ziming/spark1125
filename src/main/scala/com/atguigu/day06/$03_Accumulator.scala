package com.atguigu.day06

object $03_Accumulator {

  /**
    * 累加器的原理: 现在每个分区中对数据累加,然后将累加的结果发给Driver汇总
    * 累加器的好处: 能够一定程度上减少shuffle操作
    * 累加器的使用场景: 只能用于聚合场景并且是聚合之后数据量不会太大的场景
    *
    *
    */
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("test"))

    val acc = sc.longAccumulator("acc_sum")
    val rdd = sc.parallelize(List(10,50,30,20))

    var sum = 0

    //rdd.foreach(x=> sum = sum+x )
    rdd.foreach(x=> acc.add(x) )

    println(sum)
    //获取最终结果
    println(acc.value)
    Thread.sleep(100000)
  }
}
