package com.atguigu.day03

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.junit.Test

import scala.collection.mutable.ListBuffer

/**
  * spark算子分为两种: Transformtion算子[转换算子]、Action算子[行动算子]
  *     Transformtion算子: 不会触发任务的计算,只是封装了数据的处理逻辑,Transformation算子的结果还是RDD
  *     Action算子: 会触发任务计算,action算子的结果不是RDD是scala数据类型或者没有返回值
  */
class $01_Transformation {

  import org.apache.spark.{SparkConf, SparkContext}
  val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("test"))

  /**
    * map(func: RDD元素类型=>B): 一对一映射[原RDD一个元素计算得到新RDD一个元素]
    *     map里面的函数是针对集合每个元素操作,元素有多少个,函数就调用多少次
    *     map生成的新RDD元素个数 = 原RDD元素个数
    *     map的使用场景: 数据类型/值的转换[一对一转换]
    */
  @Test
  def map(): Unit ={

    val rdd = sc.parallelize(List(1,3,2,4,5,6,8,10))

    val rdd2 = rdd.map(x=> {
      println(s"${Thread.currentThread().getName} --${x}")
      x * 10
    })

    println(rdd2.collect().toList)
  }

  /**
    *  flatMap(func: RDD元素类型=> 集合 ) = map + flatten
    *     flatMap里面的函数是针对集合每个元素操作,元素有多少个,函数就调用多少次
    *     flatMap生成新RDD元素个数一般>=原RDD元素个数
    *     flatMap应用场景: 用于一对多
    */
  @Test
  def flatMap(): Unit ={

    val rdd = sc.parallelize(List("hello java spark","spark hadoop flume","flume kafka spark","spark hadoop"))

    val rdd2 = rdd.flatMap(x=> x.split(" "))

    println(rdd2.collect().toList)
  }

  /**
    * groupBy(func: RDD元素类型=> K ): 按照指定字段分组
    *     groupBy里面的函数是针对每个元素操作,元素有多少个,函数就调用多少次
    *     groupBy后续是按照函数的返回值对元素进行分组
    *     groupBy生成新RDD里面元素类型是KV键值对,K是函数的返回值,V是一个集合,集合中装载是原RDD中key对应的所有元素
    *groupBy会产生shuffle
    */
  @Test
  def groupBy(): Unit ={
    val rdd = sc.parallelize(List("hello java spark","spark hadoop flume","flume kafka spark","spark hadoop"))

    val rdd2 = rdd.flatMap(_.split(" "))

    val rdd3 = rdd2.groupBy(x=>x)

    println(rdd3.collect().toList)
  }

  /**
    * filter(func: RDD元素类型=>Boolean): 按照指定条件过滤
    *     filter里面的函数是针对每个元素操作,元素有多少个,函数就调用多少次
    *     filter是按照函数的返回值过滤,如果返回值为true则保留数据
    */
  @Test
  def filter(): Unit ={

    val rdd = sc.parallelize(List(1,4,3,6,8,10))

    val rdd2 = rdd.filter(x=> x%2==0)

    println(rdd2.collect().toList)


  }

  /**
    *  根据用户id获取用户详细信息[id,sex,create_time]
    */
  @Test
  def mapPartitions1(): Unit ={

    val rdd = sc.parallelize(List(1,4,3,2,5,6,7))

    //根据用户id获取用户详细信息[id,sex,create_time]
    //第一种方案: 使用map针对每个用户id查询mysql
    //    问题: map里面的函数是针对每个元素操作,每个元素id查询的时候都需要创建连接与销毁连接,所以如果rdd中元素很多,所以性能很慢
    val rdd2 = rdd.map(id=>{

      var connection:Connection = null
      var statement:PreparedStatement = null
      var sex:String = null
      var create_time:String = null
      try{
        connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test","root","root123")
        statement = connection.prepareStatement("select id,sex,create_time from person where id=?")
        //参数赋值
        statement.setInt(1,id)
        //查询
        val set = statement.executeQuery()
        //遍历结果行
        while(set.next()) {
          sex = set.getString("sex")
          create_time = set.getString("create_time")
        }

      }catch {
        case e:Exception => e.printStackTrace()
      }finally {
        if(statement!=null)
          statement.close()
        if(connection!=null)
          connection.close()
      }

      (id,sex,create_time)
    })

    println(rdd2.collect().toList)
  }

  /**
    * 根据用户id获取用户详细信息[id,sex,create_time]-方案2
    *     在map方法外面创建mysql连接,等到任务执行完成之后销毁连接
    */
  @Test
  def mapPartitions2(): Unit ={

    val rdd = sc.parallelize(List(1,4,3,2,5,6,7))

    //根据用户id获取用户详细信息[id,sex,create_time]
    //第二种方案: 在map方法外面创建mysql连接,等到任务执行完成之后销毁连接
    //    问题: spark算子函数体中的代码是在Task中执行,算子函数体外面的代码是在Driver执行的,如果spark算子函数体中使用了Driver的对象,Driver会将该对象序列化之后发送给Task使用,但是statement没有继承序列化接口不能序列化
    //
    var connection:Connection = null
    var statement:PreparedStatement = null
    connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test","root","root123")
    statement = connection.prepareStatement("select id,sex,create_time from person where id=?")

    val rdd2 = rdd.map(id=>{

      var sex:String = null
      var create_time:String = null
      try{

        //参数赋值
        statement.setInt(1,id)
        //查询
        val set = statement.executeQuery()
        //遍历结果行
        while(set.next()) {
          sex = set.getString("sex")
          create_time = set.getString("create_time")
        }

      }catch {
        case e:Exception => e.printStackTrace()
      }finally {

      }

      (id,sex,create_time)
    })

    println(rdd2.collect().toList)

    if(statement!=null)
      statement.close()
    if(connection!=null)
      connection.close()
  }

  /**
    * mapPartitions( func: Iterator[RDD元素类型] => Iterator[B] ) : 一对一映射[原RDD一个分区计算得到新RDD一个分区]
    *     mapPartitions里面的函数是针对每个分区操作,RDD有多少分区,函数就执行多少次
    *     mapPartitions的应用场景: 一般用于从mysql/redis/hbase等存储介质查询数据，可以减少资源链接创建与销毁此时提高效率
    *
    *map与mapPartitions的区别
    *     1、函数针对的对象不一样:
    *         map里面的函数是针对每个分区的每个元素操作
    *         mapPartitions里面的函数是针对每个分区操作
    *     2、函数的返回值不一样
    *         map里面的函数针对每个分区的每个元素操作,操作完成之后需要返回新的元素结果,新元素是新RDD中一个分区的元素,所以新RDD元素个数=原RDD元素个数
    *         mapPartitions里面的函数是针对每个分区所有数据的迭代器操作,要求操作完成之后返回一个新的迭代器,新迭代器是新RDD一个分区的所有数据，所有新RDD元素个数不一定等于原RDD元素个数
    *     3、对象内存回收的时机不一样
    *         map里面的函数是针对每个分区的每个元素操作,元素操作完成之后,元素就可以进行内存回收了
    *         mapPartitions里面的函数是针对每个分区所有数据的迭代器操作,所以必须等到该迭代器中所有数据都操作完成才能整体回收,所以如果RDD一个分区数据量特别大可能出现内存溢出,此时可以使用map代替【完成比完美要重要】
    *
    */
  @Test
  def mapPartitions3(): Unit ={

    val rdd = sc.parallelize(List(1,4,3,2,5,6,7))

    //根据用户id获取用户详细信息[id,sex,create_time]
    //第三种方案: 使用mapPartitions
    val rdd2 = rdd.mapPartitions( it=> {

      //it.filter()
      //it.flatMap()
      //it.map()
      var connection:Connection = null
      var statement:PreparedStatement = null
      var result = ListBuffer[(Int,String,String)]()
      try{

        connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test","root","root123")
        statement = connection.prepareStatement("select id,sex,create_time from person where id=?")
        println(s"${connection}")
        it.foreach(id=> {
          statement.setInt(1,id)
          val set = statement.executeQuery()
          var sex:String = null
          var create_time:String = null
          while(set.next()){
            sex = set.getString("sex")
            create_time = set.getString("create_time")
          }

          result.+=( (id,sex,create_time) )
        })

      }catch {
        case e:Exception => e.printStackTrace()
      }finally {
        if(statement!=null)
          statement.close()
        if(connection!=null)
          connection.close()
      }

      result.toIterator
    })

    println(rdd2.collect().toList)

  }


  /**
    * mapPartitionsWithIndex(func: (Int,Iterator[RDD元素类型])=>Iterator[B] ) : 一对一映射[原RDD一个分区计算得到新RDD一个分区]
    *     mapPartitionsWithIndex里面的函数是针对每个分区操作,RDD有多少分区,函数就执行多少次
    *
    * mapPartitions与mapParititonsWithIndex的区别:
    *     mapPartitionWithIndex里面的函数相比mapPartitions里面的函数多了个分区号的参数
    *
    */
  @Test
  def mapPartitionsWithIndex(): Unit ={

    val rdd = sc.parallelize(List(1,3,5,7,9,10,11,55))

    val rdd2 = rdd.mapPartitionsWithIndex( (index,it)=> {
      println(s"index=${index} it=${it.toList}")
      it
    } )

    val result = rdd2.collect()
    println(result.toList)
  }

  /**
    * 去重
    * distinct会产生shuffle操作
    */
  @Test
  def distinct(): Unit ={
    val rdd = sc.parallelize(List(1,3,5,1,5,7,9,11,1,3))

    val rdd2 = rdd.distinct()

    println(rdd2.collect().toList)

    val rdd3 = rdd.groupBy(x=> x)

    val rdd4 = rdd3.map(_._1)

    println(rdd4.collect().toList)
  }

  /**
    * coalesce:合并分区数
    *     coalesce默认只能减少分区数，此时没有shuffle操作
    *     如果想要增大分区数,此时可以设置shuffle参数为true，但是会产生shuffle操作
    *coalesce是否会产生shuffle由shuffle参数决定,shuffle=true会产生shuffle操作, shuffle=false不会产生shuffle操作
    *
    */
  @Test
  def coalesce(): Unit ={
    val rdd = sc.parallelize(List(1,3,5,1,5,7,9,11,1,3,33,44),6)

    println(rdd.getNumPartitions)
    //减少分区数,此时没有shuffle
    val rdd2 = rdd.coalesce(3)
    println(rdd2.getNumPartitions)

    rdd2.collect()

    //增大分区数，此时会产生shuffle
    val rdd3 = rdd.coalesce(12,true)

    rdd3.collect()

    //增大分区数，此时会产生shuffle
    val rdd4 = rdd.coalesce(4,true)
    rdd4.collect()
    Thread.sleep(1000000)


  }

  /**
    * repartition: 重分区[改变分区数]
    *     repartition既可以增大分区数也可以减少分区数,但是都会产生shuffle操作
    *     repartition底层就是coalesce(shuffle=true)
    *
    * coalesce与reparition的区别
    *     coalesce默认只能减少分区数,不会产生shuffle,要想增大分区数需要shuffle=true，会产生shuffle.【coalesce一般用于搭配filter使用,用于减少分区数】
    *     repartition既可以增大也可以减少分区,但是都会产生shuffle 【一般用于增大分区数,因为使用简单】
    */
  @Test
  def repartition(): Unit ={
    val rdd = sc.parallelize(List(1,3,5,1,5,7,9,11,1,3,33,44),6)

    //减少分区数
    val rdd2 = rdd.repartition(3)
    //增大分区数
    val rdd3 = rdd.repartition(10)

    println(rdd2.getNumPartitions)
    println(rdd3.getNumPartitions)

    rdd2.collect()

    rdd3.collect()

    Thread.sleep(10000000)
  }

  /**
    * sortBy(func: 集合元素类型=>K[,ascding=false]): 按照指定字段排序
    *   sortBy里面的函数是针对每个元素操作,元素有多少个,函数就执行多少次
    *   sortBy后续是按照函数的返回值对元素进行排序
    *sortBy会产生shuffle操作
    */
  @Test
  def sortBy(): Unit ={
    val rdd = sc.parallelize(List(1,3,5,1,5,7,9,11,1,3,33,44),6)

    //升序
    val rdd2 = rdd.sortBy( x=>x )
    //升序
    val rdd3 = rdd.sortBy( x=>x,false )

    println(rdd3.collect().toList)
  }


}
