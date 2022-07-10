package com.atguigu.day09

object $02_SparkHive {


  /**
    * 在idea中sparksql操作hive
    *     1、加入mysql、spark-hive依赖
    *     2、将hive-site.xml文件加入resource目录
    *     3、创建sparksession的时候开启hive的支持: enableHiveSupport
    *
    */
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME","atguigu")
    import org.apache.spark.sql.{Row, SparkSession}
    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("test")
      .enableHiveSupport() //TODO 开启hive支持
      .getOrCreate()
    import spark.implicits._

    //读取hive表数据
    spark.sql("select * from gmall.ods_activity_info_full")//.show

    //写入数据到hive表中
    //spark.sql("select * from student")
    spark.sql("insert into student select * from student")
  }
}
