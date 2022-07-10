package com.atguigu.day08

import org.apache.spark.sql.SaveMode

object $07_Writer {

  /**
    * 写文件有两种方式
    *     1、df/ds.write <不用>
    *           .mode(SaveMode.XXX) --指定写入模式
    *           .format("csv/text/jdbc/paruqet/orc/json") --指定数据写入格式
    *           [.option(k,v)....] --指定数据写入参数
    *           .save([path]) --保存
    *     2、df/ds.write .mode(SaveMode.XXX)[.option(k,v)....].csv/paruqet/textFile/orc/json(...) <常用>
    *
    *常用的写入模式
    *     SaveMode.Append: 如果写入数据的目录/表已经存在则追加数据,一般将数据写入没有主键的mysql表中【如果表有主键,在使用append的时候可能出现主键冲突的问题,可以通过?解决】。
    *     SaveMode.Overwrite: 如果写入数据的目录/表已经存在则覆盖数据,一般用于将数据写入HDFS中
    *
    */
  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.{Row, SparkSession}
    val spark = SparkSession.builder().master("local[4]").appName("test").getOrCreate()
    import spark.implicits._

    val df = spark.read.json("datas/j1.json")

    //保存为json
    df.write.mode(SaveMode.Overwrite).json("output/json")

    //保存为csv
    //    csv常用option:
    //          sep: 指定数据保存的时候字段之间的分隔符
    //          header: 指定是否将列名作为文件第一行保存
    df.write.mode(SaveMode.Overwrite).option("header","true").option("sep","\t").csv("output/csv")

    //写文本[dataframe只有一个列的时候才能写成文本]
    df.toJSON.write.mode(SaveMode.Overwrite).text("output/text")
    //写parquet
    df.write.mode(SaveMode.Overwrite).parquet("output/parquet")

    //写orc
    df.write.mode(SaveMode.Overwrite).orc("output/orc")
  }
}
