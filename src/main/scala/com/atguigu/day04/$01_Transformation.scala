package com.atguigu.day04

import org.apache.spark.{HashPartitioner, Partitioner}
import org.junit.Test

class $01_Transformation /*extends Serializable*/ {

  import org.apache.spark.{SparkConf, SparkContext}

  //@transient
  val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("test"))

  /**
    * 交集: 两个RDD共同的元素
    *   会产生shuffle【两次shuffle】
    */
  @Test
  def intersection(): Unit ={

    val rdd1 = sc.parallelize(List(1,2,3,4,5))
    val rdd2 = sc.parallelize(List(4,5,6,7,8))

    val rdd3 = rdd1.intersection(rdd2)

    println(rdd3.collect().toList)

    Thread.sleep(10000000)
  }

  /**
    * 差集:
    *   会产生shuffle【两次shuffle】
    */
  @Test
  def subtract(): Unit ={
    val rdd1 = sc.parallelize(List(1,2,3,4,5))
    val rdd2 = sc.parallelize(List(4,5,6,7,8))

    val rdd3 = rdd1.subtract(rdd2)

    println(rdd3.collect().toList)
  }

  /**
    * 并集:
    *     union不会去重
    *     不会产生shuffle操作
    *     union生成新RDD的分区数 = 两个父RDD分区数之和
    */
  @Test
  def union(): Unit ={
    val rdd1 = sc.parallelize(List(1,2,3,4,5))
    val rdd2 = sc.parallelize(List(4,5,6,7,8))

    val rdd3 = rdd1.union(rdd2)

    println(rdd3.getNumPartitions)
    println(rdd3.collect().toList)
  }

  /**
    * 两个RDD要想拉链必须要求元素个数与分区数都要一样
    */
  @Test
  def zip(): Unit ={

    val rdd1 = sc.parallelize(List("aa","cc","dd","ee","ff","gg"),3)

    val rdd2 = sc.parallelize(List(1,2,3,4),2)

    val rdd3 = rdd1.zip(rdd2)

    println(rdd3.collect().toList)
  }

  /**
    * partitionBy(Partitioner分区器对象): 按照指定的分区器对数据重分区
    */
  @Test
  def partitionBy(): Unit ={

    val rdd1 = sc.parallelize(List("aa","dd","cc","gg","tt","pp","uu","zz"))

    val rdd2 = rdd1.map(x=>(x,1))

    val rdd3 = rdd2.partitionBy(new HashPartitioner(8))

    rdd3.mapPartitionsWithIndex((index,it)=>{
      println(s"index=${index} data=${it.toList}")
      it
    }).collect()

    val rdd4 = rdd2.partitionBy(new UserDefinedPartitioner(4))
    rdd4.mapPartitionsWithIndex((index,it)=>{
      println(s"index=${index} data=${it.toList}")
      it
    }).collect()

    println(rdd4.getNumPartitions)

  }

  /**
    * groupByKey: 按照key进行分组
    *     groupByKey的返回RDD元素类型是KV键值对,K是原RDD元素的key[此时每个key只有唯一一条数据], V是集合,结合中装的是K对应原RDD所有value值
    */
  @Test
  def groupByKey(): Unit ={

    val rdd1 = sc.parallelize(List( ("aa",5),("bb",6),("aa",10),("cc",11),("cc",22),("cc",33) ))

    val rdd2 = rdd1.groupBy(x=> x._1)

    println(rdd2.collect().toList)

    val rdd3 = rdd1.groupByKey()

    val rdd4 = rdd3.map(x=>(x._1,x._2.sum))

    println(rdd3.collect().toList)
    println(rdd4.collect().toList)
  }

  /**
    * reduceByKey(func: ( Value值类型,Value值类型 )=>Value值类型  ): 按照key分组并且对每个key所有value值聚合
    *
    * groupByKey与reduceByKey的区别
    *     groupByKey没有combiner预聚合过程
    *     reduceByKey存在combiner预聚合过程,性能要比groupByKey要高,工作中推荐使用reduceByKey这种高性能shuffle算子
    */
  @Test
  def reduceByKey(): Unit ={
    val rdd1 = sc.parallelize(List( ("aa",5),("bb",6),("aa",10),("cc",11),("cc",22),("cc",33),("aa",20),("bb",16),("aa",30),("cc",44),("cc",55),("cc",66) ),2)

    rdd1.mapPartitionsWithIndex((index,it)=>{
      println(s"index=${index} data=${it.toList}")
      it
    }).collect()
    val rdd2 = rdd1.reduceByKey((agg,curr)=>{
      println(s"agg=${agg} curr=${curr}")
      agg+curr
    })

    println(rdd2.collect().toList)
  }


  /**
    * combineByKey(createCombiner: Value值类型=> B ,mergeValue: (B,Value值类型)=>B ,mergeCombiners:(B,B)=>B): 按照key分组对每个key所有value值聚合
    *      createCombiner: 在combiner计算之前对每个组第一个value值进行转换
    *      mergeValue: combiner计算逻辑
    *      mergeCombiners: reducer计算逻辑
    */
  @Test
  def combineByKey(): Unit ={

    val rdd1 = sc.parallelize(List( "语文"->60,"英语"->70,"数学"->70,"语文"->90,"语文"->100,"数学"->100,"英语"->100,"数学"->80,"英语"->50,"语文"->70,"数学"->60,"英语"->90 ),2)

    rdd1.mapPartitionsWithIndex((index,it)=>{
      println(s"inde=${index} data=${it.toList}")
      it
    }).collect()
    //需求: 统计每门学科的平均分
    //第一种方案: groupByKey + map
    val rdd2 = rdd1.groupByKey()

    val rdd3 = rdd2.map( x=> {
      val name = x._1
      //当前学科总分
      val totalScore = x._2.sum

      //当前学科成绩个数
      val num = x._2.size

      (name, totalScore.toDouble/num)
    })

    println(rdd3.collect().toList)
    //List((英语,77.5), (数学,77.5), (语文,80.0))

    //第二种方案: map + reduceByKey + map
    //1、通过map转换: 学科名称->(成绩,1)
    val rdd4 = rdd1.map{
      case (name,score) => (name,(score,1))
    }
    //RDD( 语文->(60,1),数学->(70,1),英语->(70,1),语文->(90,1),...,语文->(100,1))
    //2、reduceByKey: 统计每个学科总成绩和总个数
    val rdd5 = rdd4.reduceByKey((agg,curr)=>( agg._1+curr._1 , agg._2+curr._2 ))
    //3、求得平均分
    val rdd6 = rdd5.map{
      case (name,(totalscore,num)) => (name,totalscore.toDouble/num)
    }
    println(rdd6.collect().toList)

    //第三种方案: combineByKey+ map
    //RDD( 语文->60,数学->70,英语->70,语文->90,...,语文->100)
    //RDD( 语文->(60,1),数学->70,英语->70,语文->90,...,语文->100)
    val rdd7 = rdd1.combineByKey(x=>{
      println(s"createCombiner:${x}")
      (x,1)
    }, (agg:(Int,Int),curr)=>{
      println(s"mergeValue:agg=${agg} curr=${curr}")
      ( agg._1+curr , agg._2+1 )
    }, (agg:(Int,Int),curr:(Int,Int)) => {
      println(s"mergeCombiners:agg=${agg} curr=${curr}")
      (agg._1+curr._1,agg._2+curr._2)
    })
    val rdd8 = rdd7 .map{
        case (name,(totalscore,num)) => (name,totalscore.toDouble/num)
      }
    println(rdd8.collect().toList)
  }

  /**
    * foldByKey(默认值)(func: (Value值类型,Value值类型)=> Value值类型): 按照key分组对每个key所有的value值聚合
    *     foldByKey在combiner计算阶段对每个组第一次计算的时候,函数第一个参数的初始值 = 默认值
    */
  @Test
  def foldByKey(): Unit ={
    val rdd1 = sc.parallelize(List( ("aa",5),("bb",6),("aa",10),("cc",11),("cc",22),("cc",33),("aa",20),("bb",16),("aa",30),("cc",44),("cc",55),("cc",66) ),2)

    val rdd2 = rdd1.foldByKey(0)((agg,curr)=> {
      println(s"agg=${agg} curr=${curr}")
      agg+curr
    } )

    println(rdd2.collect().toList)
  }

  /**
    * aggregateByKey(默认值)( seqop:(默认值类型,Value值类型)=>默认值类型   ,comop:( 默认值类型,默认值类型)=>默认值类型 ): 按照key分组对每个key所有的value值聚合
    *     aggregateByKey在combiner计算阶段对每个组第一次计算的时候,函数第一个参数的初始值 = 默认值
    *
    *
    * reduceByKey、foldByKey、aggregateByKey、combineByKey的区别
    *     reduceByKey: combiner与reducer计算逻辑一样, combiner阶段针对每个组第一次计算的时候函数第一个参数的初始值 = 该组第一个value值
    *     foldByKey: combiner与reducer计算逻辑一样, combiner阶段针对每个组第一次计算的时候函数第一个参数的初始值 = 默认值
    *     combineByKey: combiner与reducer计算逻辑可以不一样, combiner阶段针对每个组第一次计算的时候函数第一个参数的初始值 = 第一个函数的转换结果
    *     aggregateByKey: combiner与reducer计算逻辑可以不一样, combiner阶段针对每个组第一次计算的时候函数第一个参数的初始值 = 默认值
    */
  @Test
  def aggregateByKey(): Unit ={
    val rdd1 = sc.parallelize(List( "语文"->60,"英语"->70,"数学"->70,"语文"->90,"语文"->100,"数学"->100,"英语"->100,"数学"->80,"英语"->50,"语文"->70,"数学"->60,"英语"->90 ),2)

    val rdd2 = rdd1.aggregateByKey( (0,0) )( (agg: (Int,Int),curr) =>{
      println(s"combiner计算: agg=${agg} curr=${curr}")
      (agg._1+curr,agg._2+1)
    }, ( agg:(Int,Int) ,curr:(Int,Int) ) => {
      println(s"reducer计算: agg=${agg} curr=${curr}")
      (agg._1+curr._1, agg._2+curr._2)
    } )

    val rdd3 = rdd2.map{
        case (name,(totalscore,num)) => (name,totalscore.toDouble/num)
      }

    println(rdd3.collect().toList)
  }

  /**
    * sortByKey: 根据key排序
    */
  @Test
  def sortByKey(): Unit ={
    val rdd1 = sc.parallelize(List( ("aa",5),("bb",6),("aa",10),("cc",11),("cc",22),("cc",33),("aa",20),("bb",16),("aa",30),("cc",44),("cc",55),("cc",66) ),2)

    //降序
    val rdd2 = rdd1.sortByKey(false)
    //升序
    val rdd3 = rdd1.sortByKey()
    //sortBy实现sortByKey功能
    //val rdd4 = rdd1.sortBy(x=> x._1,false)

    println(rdd2.collect().toList)
  }

  /**
    * mapValues(func: Value值类型=>B ): 一对一映射[一个RDD元素Value值计算得到新RDD元素的value值,key没有改变]
    *     mapValues里面的函数是针对每个元素的value操作,key不会改变
    */
  @Test
  def mapValues(): Unit ={
    val rdd1 = sc.parallelize(List( ("aa",5),("bb",6),("aa",10),("cc",11),("cc",22),("cc",33),("aa",20),("bb",16),("aa",30),("cc",44),("cc",55),("cc",66) ),2)

    val rdd2 = rdd1.mapValues(x=>x*10)

    //val rdd3 = rdd1.map(x=>(x._1, x._2 * 10) )

    println(rdd2.collect().toList)
  }

  /**
    * join: 两个RDD必须都是KV键值对才能join, 两个RDD的元素只有key相同才能连接
    *     join生成的新RDD的元素类型是KV键值对,K是join的key, V是二元元组,二元元组第一个值是左RDD的value值,第二个值是右RDD的value值
    * leftOuterJoin: 两个RDD必须都是KV键值对才能join, 两个RDD的元素只有key相同才能连接
    *   leftOuterJoin生成的新RDD的元素类型是KV键值对,K是join的key, V是二元元组,二元元组第一个值是左RDD的value值,第二个值是Option(右RDD的value值)
    *   leftOuterJoin的结果 = 能够连接的数据 + 左RDD不能连接的数据
    * rightOuterJoin: 两个RDD必须都是KV键值对才能join, 两个RDD的元素只有key相同才能连接
    *   rightOuterJoin生成的新RDD的元素类型是KV键值对,K是join的key, V是二元元组,二元元组第一个值是Option(左RDD的value值),第二个值是右RDD的value值
    *   rightOuterJoin的结果 = 能够连接的数据 + 右RDD不能连接的数据
    * fullOuterJoin: 两个RDD必须都是KV键值对才能join, 两个RDD的元素只有key相同才能连接
    *   fullOuterJoin生成的新RDD的元素类型是KV键值对,K是join的key, V是二元元组,二元元组第一个值是Option(左RDD的value值),第二个值是Option(右RDD的value值)
    *   fullOuterJoin的结果 = 能够连接的数据 + 右RDD不能连接的数据 + 左RDD不能连接的数据
    */
  @Test
  def join(): Unit ={

    val rdd1 = sc.parallelize(List( "aa"->10,"aa"->20,"cc"->30,"dd"->40,"cc"->50 ))

    val rdd2 = sc.parallelize(List( "aa"->11,"cc"->22,"ff"->33))

    //类似sql inner join, join的条件就是key相同
    val rdd3 = rdd1.join(rdd2)

    //类似sql left join, join的条件就是key相同
    val rdd4 = rdd1.leftOuterJoin(rdd2)

    //类似sql right join, join的条件就是key相同
    val rdd5 = rdd1.rightOuterJoin(rdd2)

    //类似sql full join
    val rdd6 = rdd1.fullOuterJoin(rdd2)
    println(rdd3.collect().toList)
    println(rdd4.collect().toList)
    println(rdd5.collect().toList)
    println(rdd6.collect().toList)
  }

  /**
    * cogroup = groupByKey + fullOuterJoin
    *   cogroup生成的RDD元素是KV键值对,K是RDD元素的key,V是二元元组,第一个值是key对应左RDD所有value值的集合,第二个值是key对应右RDD所有value值的集合
    */
  @Test
  def cogroup(): Unit ={
    val rdd1 = sc.parallelize(List( "aa"->10,"aa"->20,"cc"->30,"dd"->40,"cc"->50 ))

    val rdd2 = sc.parallelize(List( "aa"->11,"cc"->22,"ff"->33))

    val rdd3 = rdd1.cogroup(rdd2)

    println(rdd3.collect().toList)
  }

}


/**
  * 自定义分区器
  * @param num
  */
class UserDefinedPartitioner(num:Int) extends Partitioner{
  //spark后续内部调用获取重分区的分区数
  override def numPartitions: Int = if(num<5) 5 else num
  //根据key获取分区号,后续shuffle的时候该key的数据放入分区号对应的分区中
  override def getPartition(key: Any): Int = key match {
    case "aa" =>0
    case "dd" =>1
    case "cc" =>2
    case "gg" =>3
    case "tt" =>4
    case x =>
      val r = x.hashCode() % num
      if(r <0)
        r+num
      else
        r

  }
}