package com.atguigu.day05

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.junit.Test

class $02_Action {

  import org.apache.spark.{SparkConf, SparkContext}
  val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("test"))
  /**
    * collect: 用来收集RDD每个分区的数据并将所有分区的数据用数组封装之后返回给Driver 【重要,工作中一般会结合后面的广播变量使用】
    *     如果RDD所有分区数据量比较大,collect收集数据返回给Driver的时候数据是放在Driver内存中,Driver内存默认只有1G
    *     所以,工作中一般需要设置Driver内存为5-10G，可以通过bin/spark-submit --driver-memory 10G设置
    */
  @Test
  def collect(): Unit ={

    val rdd1 = sc.parallelize(List(1,3,2,4,5,6,7))
    val arr = rdd1.collect()
    println(arr.toList)
  }

  /**
    * first: 获取rdd第一个元素
    *     first = take(1)
    */
  @Test
  def first(): Unit ={
    val rdd1 = sc.parallelize(List(1,3,2,4,5,6,7))


    println(rdd1.first())
  }

  /**
    * take: 获取RDD前N个元素
    *     take是先启动一个job从0号分区尝试获取N个元素,如果0号分区没有N个元素,会在启动一个job从其他分区获取剩余的元素。
    */
  @Test
  def take(): Unit ={
    val rdd1 = sc.parallelize(List(1,3,2,4,5,6,7,10))

    val arr = rdd1.take(2)

    println(arr.toList)

    Thread.sleep(1000000)
  }

  /**
    * takeOrdered: 对RDD元素排序之后取前N个元素【默认升序】
    */
  @Test
  def takeOrdered(): Unit ={
    val rdd1 = sc.parallelize(List(1,3,2,4,5,6,7,10))

    println(rdd1.takeOrdered(3).toList)
  }

  /**
    * countByKey: 统计RDD元素中每个key的个数
    */
  @Test
  def countByKey(): Unit ={

    val rdd = sc.parallelize( List( "aa"->10,"cc"->30,"aa"->40,"dd"->50,"cc"->60,"cc"->70 ) )

    println(rdd.countByKey)
  }

  /**
    *saveXXX: 保存数据到文件中
    */
  @Test
  def save(): Unit ={
    val rdd = sc.parallelize( List( "aa"->10,"cc"->30,"aa"->40,"dd"->50,"cc"->60,"cc"->70 ) ,2)

    rdd.saveAsTextFile("output")
  }

  /**
    * foreach(func: RDD元素类型=>Unit):Unit : 对RDD每个元素遍历
    */
  @Test
  def foreach(): Unit ={
    val rdd = sc.parallelize( List( "aa"->10,"cc"->30,"aa"->40,"dd"->50,"cc"->60,"cc"->70 ) ,2)

    rdd.foreach(x=>println(s"${Thread.currentThread().getName}--${x}"))
  }

  /**
    * foreachPartition(func: Iterator[RDD元素类型]=>Unit):Unit : 对分区所有元素的迭代器处理
    *     foreachPartition里面的函数是针对每个分区操作,RDD有多少分区,函数就执行多少次
    *     foreachPartition一般用于将数据保存到mysql/hbase/redis等位置,可以减少资源链接创建与销毁的次数
    */
  @Test
  def foreachPartition(): Unit ={
    val rdd = sc.parallelize( List( "aa"->10,"cc"->30,"aa"->40,"dd"->50,"cc"->60,"cc"->70 ) ,2)

    val rdd2 = rdd.reduceByKey(_+_)

    //需要将结果保存到mysql
    rdd2.foreachPartition( it => {

      var connection:Connection = null
      var statement:PreparedStatement = null
      try{
        connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test","root","root123")
        statement = connection.prepareStatement("insert into wordcount values(?,?)")

        var i = 0
        it.foreach(x=>{
          statement.setString(1,x._1)
          statement.setInt(2,x._2)
          //statement.executeUpdate()
          //将当前sql加入一个批次中
          statement.addBatch()
          if(i%1000==0){
            //提交一个批次数据
            statement.executeBatch()
            //清空批次
            statement.clearBatch()
          }
          i = i+1

        })

        //提交最后一个不满1000条数据的批次
        statement.executeBatch()
      }catch {
        case e:Exception => e.printStackTrace()
      }finally {
        if(statement!=null)
            statement.close()
        if(connection!=null)
          connection.close()
      }

    })
  }
}
