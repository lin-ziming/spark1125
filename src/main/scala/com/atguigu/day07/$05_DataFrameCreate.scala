package com.atguigu.day07

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.junit.Test

case class Person( id:Int,name:String,age:Int )
class $05_DataFrameCreate {
  import org.apache.spark.sql.{Row, SparkSession}
  val spark = SparkSession.builder().master("local[4]").appName("test").getOrCreate()
  import spark.implicits._
  /**
    * DataFrame创建方式
    *     1、通过toDF方法创建
    *     2、通过读取文件创建
    *     3、通过其他的DataFrame
    *     4、通过createDataFrame方法
    */

  /**
    * 1、通过toDF方法创建
    *       集合.toDF()
    *       rdd.toDF()
    *    如果集合/rdd中元素类型是样例类,通过toDF方法转换成DataFrame的时候列名为样例类的属性名
    *    如果集合/rdd中元素类型是元组,通过toDF方法转换成DataFrame的时候列名为_N,此时可以通过有参的toDF方法重定义列名[注意: 重定义的列名的个数必须与列的个数一致]
    *
    */
  @Test
  def createDataFrameByToDF(): Unit ={

    val list = List( Person(1,"lisi",20),Person(2,"wangwu",30),Person(3,"zhaoliu",40),Person(5,"lilei",60) )
    val list2 = List( (1,"lisi",20),(2,"wangwu",30),(3,"zhaoliu",40),(5,"lilei",60) )

    // 如果集合/rdd中元素类型是样例类,通过toDF方法转换成DataFrame的时候列名为样例类的属性名
    val df = list.toDF
    //如果集合/rdd中元素类型是元组,通过toDF方法转换成DataFrame的时候列名为_N,此时可以通过有参的toDF方法重定义列名[注意: 重定义的列名的个数必须与列的个数一致]
    val df2 = list2.toDF("id2","name2","age2")

    df.show()
    df2.show

    val rdd = spark.sparkContext.parallelize(list)
    val df3 = rdd.toDF()
    df3.show
  }

  /**
    * 2、通过读取文件创建
    */
  @Test
  def createDataFrameByFile(): Unit ={

    spark.read.json("datas/j1.json").show
  }

  /**
    * 3、通过其他的DataFrame
    */
  @Test
  def createDataFrameByDataFrame(): Unit ={
    val df = spark.read.json("datas/j1.json")
    val df2 = df.select("name","age")
    df2.show
  }

  @Test
  def createDataFrameByMethod(): Unit ={

    //指定表结构
    //列的结构
    val fileds = Array(
      StructField("id",IntegerType),
      StructField("name",StringType),
      StructField("age",IntegerType)
    )
    val schema = StructType(fileds)
    //表的行数据
    val rdd = spark.sparkContext.parallelize(List(
      Row(1,"lisi1",201),
      Row(2,"lisi2",202),
      Row(3,"lisi3",203),
      Row(4,"lisi4",204)
    ))
    val df = spark.createDataFrame(rdd,schema)

    df.show
  }
}
