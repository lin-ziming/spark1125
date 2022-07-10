package com.atguigu.day08

import com.atguigu.day07.Person

object $02_RddDataFrameDatasetCover {

  /**
    * RDD转DataFrame: rdd.toDF()/rdd.toDF(列名,...)
    * RDD转DataSet: rdd.toDS()
    * DataFrame转RDD: df.rdd
    *     Row类型取值: row.getAs[列的类型](列名)
    * DataSet转RDD: ds.rdd
    * DataSet转DataFrame: ds.toDF()/ds.toDF(列名,...)
    * DataFrame转DataSet: df.as[行类型]
    *       行类型如果是元组,要求元组的元素个数必须和列的个数一致,元素类型必须与对应列的类型一致。
    *       行类型如果是样例类,样例类中的属性可以不一定要,列的个数保持不变,如果有定义属性,要求属性名必须与列名一致,属性类型必须与列的类型一致或者能够自动转换。
    *
    */
  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.{Row, SparkSession}
    val spark = SparkSession.builder().master("local[4]").appName("test").getOrCreate()
    import spark.implicits._

    val list = List( Person(1,"lisi",20),Person(2,"wangwu",30),Person(10,"wangwu",30),Person(10,"wangwu",30),Person(3,"zhaoliu",40),Person(5,"lilei",60) )

    val rdd = spark.sparkContext.parallelize(list)
    //rdd转dataframe
    val df = rdd.toDF()

    //RDD转DataSet
    val ds = rdd.toDS()

    //DataFrame转RDD
    val rdd2 = df.rdd
    val rdd3 = rdd2.map(row=> {
      //row类型取值
      //val name = row.getAs[String](1)
      val name = row.getAs[String]("name")
      //val age = row.getAs[Int](2)
      val age = row.getAs[Int]("age")
      (name,age)
    })
    println(rdd3.collect().toList)

    //DataSet转RDD
    val rdd4 = ds.rdd
    println(rdd4.collect().toList)

    //DataSet转DataFrame
    val df2 = ds.toDF("ID","NAME","AGE")
    df2.show

    //DataFrame转dataset
    val ds5= df.as[Student]
    val ds6= df.as[(Int,String,Int)]

    //ds5.map(s=>s.name)
    ds6.show
    ds5.show
  }
}

case class Student()
