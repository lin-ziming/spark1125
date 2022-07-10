package com.atguigu.day08

object $04_UDF {

  /**
    * 自定义UDF函数
    *     1、定义一个函数
    *     2、注册udf函数
    *     3、使用
    */
  def main(args: Array[String]): Unit = {


    import org.apache.spark.sql.{Row, SparkSession}
    val spark = SparkSession.builder()
      //.config()
      .master("local[4]").appName("test").getOrCreate()
    import spark.implicits._

    val list = List( ("1001","zhagnsan"),("00203","lisi"),("100086","wangwu"),("00010086","zhaoliu") )

    val df = list.toDF("id","name")

    //需求: 员工id应该为8位,需要为不满8位id左侧用0补齐
    //df.selectExpr("lpad(id,8,'0') id","name").show

    //注册udf函数
    //spark.udf.register("m1",myLpad)
    spark.udf.register("m1",myLpad2 _)

    df.selectExpr("m1(id,8,'0') id","name").show

  }


  /**
    * 定义函数
    */
  val myLpad = ( id:String,length:Int,pad:String ) => {

    //补齐的0的个数
    val len = length - id.length

    (pad * len) + id

  }

  def myLpad2( id:String,length:Int,pad:String ):String = {
    //补齐的0的个数
    val len = length - id.length

    (pad * len) + id
  }
}



