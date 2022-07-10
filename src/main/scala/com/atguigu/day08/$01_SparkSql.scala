package com.atguigu.day08

import com.atguigu.day07.Person
import org.junit.Test

class $01_SparkSql {

  import org.apache.spark.sql.{Row, SparkSession}
  val spark = SparkSession.builder().master("local[4]").appName("test").getOrCreate()
  import spark.implicits._
  /**
    * sparksql的编程风格
    *     1、命令式: 使用一些算子操作数据
    *     2、声明式: 使用sql语句操作数据<工作常用>
    */


  /**
    * 命令式
    *     常用地命令式算子
    *         1、过滤: filter("sql过滤条件")/where("sql过滤条件")
    *         2、列裁剪:selectExpr("列名","函数(列名) 别名")
    *         3、去重
    *             distinct: 只有两行数据所有列都相同才会去重
    *             dropDuplicates: 当两行数据指定列相同就会去重
    *
    */
  @Test
  def command(): Unit ={
    val list = List( Person(1,"lisi",20),Person(2,"wangwu",30),Person(10,"wangwu",30),Person(10,"wangwu",30),Person(3,"zhaoliu",40),Person(5,"lilei",60) )
    val df = list.toDF()

    //过滤
    val df2 = df.filter("age>35")
    val df3 = df.where("age>35")
    //select * from person where age>35
    df2.show
    df3.show

    val ds = list.toDS()
    ds.filter(p=>p.age > 35)

    //列裁剪
    val df4 = df.select("name","age")

    df4.show
    //import org.apache.spark.sql.functions._
    //val df5 = df.select(sum("age"))
    //select sum(age)  sum_age from person
    df.selectExpr("id","name","age-10  age").show
    df.selectExpr("sum(age) sum_age").show
    //df5.show

    //去重
    val df6 = df.distinct()
    df6.show

    df.dropDuplicates("name").show
  }

  /**
    * 声明式
    *     1、将df/ds注册成表
    *         createOrReplaceTempView: 注册成临时表,如果表已经存在则覆盖,只能在当前sparksession中使用<常用,工作中一般只需要一个sparksession即可>
    *         createOrReplaceGlobalTempView: 注册成全局临时表,如果表已经存在则覆盖,可以在多个sparksession中使用,后续使用表的时候必须通过 global_temp.表名 的方式使用
    *     2、写sql
    */
  @Test
  def operator(): Unit ={
    val list = List( Person(1,"lisi",20),Person(2,"wangwu",30),Person(10,"wangwu",30),Person(10,"wangwu",30),Person(3,"zhaoliu",40),Person(5,"lilei",60) )
    val df = list.toDF()

    //select .. from 表名
    //注册成表
    //df.createOrReplaceTempView("person")
    df.createOrReplaceGlobalTempView("person")

    //通过sql操作表数据
    spark.sql(
      """
        |select
        |  name,age
        |from global_temp.person
        |where age>35
      """.stripMargin).show


    val spark2 = spark.newSession()

    spark2.sql(
      """
        |select
        |  name,age
        |from global_temp.person
        |where age>35
      """.stripMargin).show

  }
}
