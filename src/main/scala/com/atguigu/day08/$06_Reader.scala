package com.atguigu.day08

import org.apache.spark.sql.SaveMode
import org.junit.Test

class $06_Reader {

  import org.apache.spark.sql.{Row, SparkSession}
  val spark = SparkSession.builder().master("local[4]").appName("test").getOrCreate()
  import spark.implicits._
  /**
    * sparksql读取数据有两种方式
    *       1、spark.read   <不用>
    *             .format("csv/text/json/jdbc/parquet/orc")  --指定数据读取格式
    *             [.option(k,v)....] --指定数据读取需要的参数
    *             .load([path]) --加载数据
    *      2、 spark.read.[.option(k,v)....].csv/json/jdbc/parquet/orc/text  <常用>
    */

  @Test
  def readFile(): Unit ={

    //读取文本
    //第一种方式
    val df = spark.read.format("text").load("datas/wc.txt")

    df//.show

    //第二种方式
    spark.read.textFile("datas/wc.txt")//.show

    //读取json文件
    spark.read.json("datas/j1.json").show

    //读取csv[字段之间有固定分隔符的文件就可以用csv来读]
    //    读取csv常用option:
    //        sep: 指定之间的分隔符
    //        header: 是否指定文件第一列作为列名
    //        inferSchema: 是否自动推断列的类型
    val df2 = spark.read.option("header","true").option("inferSchema","true").csv("datas/presidential_polls.csv")
    //打印列的信息
    df2.printSchema()
    //spark.read.option("sep","_").csv("datas/user_visit_action.txt").show

    //parquet文件是列式存储文件
    //df2.write.mode(SaveMode.Overwrite).parquet("output/parquet")
    //读取parquet文件
    spark.read.parquet("output/parquet").show
  }


}
