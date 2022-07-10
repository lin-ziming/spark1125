package com.atguigu.day08

object $05_UDAF {

  /**
    * UDAF函数: 聚合函数[多行得到一个结果]
    */
  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.{Row, SparkSession}
    val spark = SparkSession.builder().master("local[4]").appName("test").getOrCreate()
    import spark.implicits._

    val list = List( ("张三",20,"北京"),("李四",25,"上海"),("赵流",50,"深圳") ,
      ("王大雷",40,"北京"),("王二雷",42,"北京"),("韩梅梅",38,"深圳"),
      ("王丽",18,"上海"),("陈建",20,"上海"),("黄世财",22,"深圳")
    )

    val rdd = spark.sparkContext.parallelize(list,2)

    rdd.mapPartitionsWithIndex((index,it)=>{
      println(s"index=${index} data=${it.toList}")
      it
    }).collect()

    val df = rdd.toDF("name","age","region")

    df.createOrReplaceTempView("person")
    //弱类型udaf函数注册
    //spark.udf.register("myavg",new MyAvgWeak)
    //强类型udaf函数注册
    import org.apache.spark.sql.functions._

    val func = udaf(new MyAvgStrong)

    spark.udf.register("myavg",func)
    //spark.sql("select region,avg(age) avg_age from person group by region").show
    spark.sql("select region,myavg(age) avg_age from person group by region").show
  }
}
