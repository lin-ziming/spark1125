package com.atguigu.day07

import org.junit.Test

class $06_DataSetCreate {

  /**
    * DataSet的创建方式
    *     1、通过toDS方法
    *     2、通过读取文件创建
    *     3、其他DataSet衍生
    *     4、通过createDataSet方法创建
    */

  import org.apache.spark.sql.{Row, SparkSession}
  val spark = SparkSession.builder().master("local[4]").appName("test").getOrCreate()
  import spark.implicits._

  /**
    * 通过toDS方法创建
    *     集合.toDS
    *     rdd.toDS
    *    如果集合/rdd中元素类型是样例类,通过toDS方法转换成DataSet的时候列名为样例类的属性名
    *    如果集合/rdd中元素类型是元组,通过toDS方法转换成DataSet的时候列名为_N,此时可以通过有参的toDF方法重定义列名[注意: 重定义的列名的个数必须与列的个数一致]
    */
  @Test
  def createDataSetByToDS(): Unit ={
    val list = List( Person(1,"lisi",20),Person(2,"wangwu",30),Person(3,"zhaoliu",40),Person(5,"lilei",60) )
    val list2 = List( (1,"lisi",20),(2,"wangwu",30),(3,"zhaoliu",40),(5,"lilei",60) )

    //如果集合/rdd中元素类型是样例类,通过toDS方法转换成DataSet的时候列名为样例类的属性名
    val ds = list.toDS()

    ds.show()

    // 如果集合/rdd中元素类型是元组,通过toDS方法转换成DataSet的时候列名为_N,此时可以通过有参的toDF方法重定义列名[注意: 重定义的列名的个数必须与列的个数一致]
    list2.toDS().toDF("ID","NAME","AGE").show

    val rdd = spark.sparkContext.parallelize(list)

    val ds2 = rdd.toDS()

    ds2.show
  }

  /**
    * 通过读取文件创建[只有读取文本文件才能创建DataSet]
    */
  @Test
  def createDataSetByFile(): Unit ={

    val ds = spark.read.textFile("datas/wc.txt")

    ds.show
  }

  /**
    * 3、其他DataSet衍生
    */
  @Test
  def createDataSetByDataSet(): Unit ={

    val ds = spark.read.textFile("datas/wc.txt")

    val ds2 = ds.flatMap(line=> line.split(" "))

    val ds3 = ds2.map(x=>(x,1))

    ds3.show
  }

  /**
    * 4、通过createDataSet方法创建
    */
  @Test
  def createDataSetByMethod(): Unit ={
    val list = List( Person(1,"lisi",20),Person(2,"wangwu",30),Person(3,"zhaoliu",40),Person(5,"lilei",60) )

    val ds = spark.createDataset(list)

    ds.show
  }

}
