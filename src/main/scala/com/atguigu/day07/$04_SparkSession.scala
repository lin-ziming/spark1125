package com.atguigu.day07

import org.apache.spark.sql.SparkSession

object $04_SparkSession {

  def main(args: Array[String]): Unit = {

    //sparksession第一种创建方式
    val spark = new SparkSession.Builder().master("local[*]").appName("test").getOrCreate()

    //sparksession第二种创建方式
    val spark2 = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
  }
}
