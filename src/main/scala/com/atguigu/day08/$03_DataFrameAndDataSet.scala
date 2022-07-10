package com.atguigu.day08

import com.atguigu.day07.Person

object $03_DataFrameAndDataSet {

  /**
    * DataFrame与DataSet的使用场景
    *     1、如果当前是RDD,RDD中的元素是样例类,此时随意转成DataFrame/DataSet
    *                      RDD中的元素是元组,此时推荐转成DataFrame，因为使用有参的toDF方法可以重定义列名
    *     2、如果当前需要使用map/flatMap等需要函数的算子,此时推荐使用DataSet,因为DataSet明确行的类型,操作更方面
    */
  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.{Row, SparkSession}
    val spark = SparkSession.builder().master("local[4]").appName("test").getOrCreate()
    import spark.implicits._
    val list = List( Person(1,"lisi",20),Person(2,"wangwu",30),Person(10,"wangwu",30),Person(10,"wangwu",30),Person(3,"zhaoliu",40),Person(5,"lilei",60) )

    val df = list.toDF

    val ds = df.map(row=> {
      val name = row.getAs[String]("name1")
      val age = row.getAs[Int]("age")
      (name,age)
    })

    val ds2 = list.toDS()

    ds2.map(p=>(p.name,p.age))
    ds.show
  }
}
