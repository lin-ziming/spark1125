package com.atguigu.day05

import org.apache.spark.storage.StorageLevel

object $07_Cache {

  /**
    * 持久化场景
    *       1、rdd在多个job中重复使用
    *           问题: 此时默认情况下是该rdd之前的处理步骤是每个job都会执行,所以数据重复处理影响效率
    *           解决方案: 如果能够将该rdd数据保存下来供后续其他job执行则不用重复执行了
    *       2、当一个job依赖链条很长
    *           问题: 如果依赖链条太长,某个环节计算出错需要重新计算得到数据,浪费时间
    *           解决方案:将rdd的数据保存下来,后续计算出错拿到保存的数据计算得到结果,减少重新计算花费的时间。
    *如何持久化RDD：
    *     RDD持久化方案有两种
    *         缓存:
    *             数据保存位置: 是将数据保存到RDD分区所在主机的内存/本地磁盘中
    *             使用方式:  rdd.cache/ rdd.persist
    *                 cache与persist的区别
    *                     cache是将数据全部保存在分区所在主机的内存中
    *                     persist是根据设置的存储级别将数据保存在分区所在主机内存/本地磁盘中
    *             数据保存时机: 在第一个job执行到缓存rdd的时候将数据保存。
    *                 存储级别
    *                     NONE: 不保存
    *                     DISK_ONLY : 数据只保存到磁盘中
    *                     DISK_ONLY_2: 数据只保存到磁盘中,但是保存两份
    *                     MEMORY_ONLY: 数据只保存到内存中
    *                     MEMORY_ONLY_2: 数据只保存到内存中,但是保存两份
    *                     MEMORY_ONLY_SER : 数据只保存到内存中,以序列化形式保存
    *                     MEMORY_ONLY_SER_2  : 数据只保存到内存中,以序列化形式保存，但是保存两份
    *                     MEMORY_AND_DISK: 数据一部分保存到内存,一部分保存到磁盘
    *                     MEMORY_AND_DISK_2 : 数据一部分保存到内存,一部分保存到磁盘，但是保存两份
    *                     MEMORY_AND_DISK_SER: 数据一部分保存到内存,一部分保存到磁盘,以序列化形式保存
    *                     MEMORY_AND_DISK_SER_2 : 数据一部分保存到内存,一部分保存到磁盘,以序列化形式保存，但是保存两份
    *                     OFF_HEAP: 数据保存到堆外内存
    *                  常用的存储级别: MEMORY_ONLY <一般用于小数据量场景> ,MEMORY_AND_DISK<一般用于大数据量场景>
    *         checkpoint:
    *             原因: 缓存是将数据保存到服务器本地磁盘/内存中,如果服务器宕机数据丢失,后续job执行的时候需要根据RDD的依赖关系重新执行得到数据,
    *                   所以需要将数据保存到可靠的存储介质HDFS中,不会出现数据丢失的问题。
    *             数据保存位置: HDFS
    *             使用方式:
    *                 1、设置checkpoint数据保存路径: sc.setCheckpointDir(path)
    *                 2、保存rdd数据: rdd.checkpoint
    *             数据保存时机: 在checkpoint rdd所在第一个job执行完成之后,会单独触发一个job，计算得到checkpoint rdd的数据进行保存。【相当于checkpoint会触发一次job操作】
    *                 checkpoint操作会单独触发一个job执行得到数据再保存,此时该checkpoint rdd之前的数据处理会重复执行一次,所以为了避免重复执行一般 会先rdd.cache 然后再rdd.checkpoint
    *         shuffle算子相当于persist(StorageLevel.DISK_ONLY)
    *
    * 缓存与checkpoint的区别
    *     1、数据保存位置不一样:
    *         缓存是将数据保存到RDD分区所在主机的内存/本地磁盘中
    *         checkpoint是将数据保存到HDFS中
    *     2、保存数据的时机不一样
    *         缓存是RDD所在第一个job执行过程中就会保存数据
    *         checkpoint是rdd所在第一个job执行完成之后才会单独触发一个job运算得到数据保存。
    *     3、依赖关系是否保留不一样
    *         缓存是将到RDD分区所在主机的内存/本地磁盘中,如果服务器宕机数据丢失,丢失之后需要根据依赖关系重新计算得到数据,所以依赖关系不能删除的。
    *         checkpoint是将数据保存到HDFS中，数据不会丢失,所以RDD的依赖关系会删除
    *
    */
  def main(args: Array[String]): Unit = {

    import org.apache.spark.{SparkConf, SparkContext}
    val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("test"))
    //设置checkpoint数据保存路径
    sc.setCheckpointDir("checkpoint")
    val rdd1 = sc.textFile("datas/wc.txt")

    val rdd2 = rdd1.flatMap(x=>{
      println("-----------------------------------")
      x.split(" ")
    })
    //缓存
    //rdd2.cache()
    //rdd2.persist(StorageLevel.DISK_ONLY)

    //checkpoint
    //rdd2.cache()
    rdd2.checkpoint()
    val rdd3 = rdd2.map(x=>(x,1))

    println(rdd2.toDebugString)
    rdd3.collect()

    val rdd4 = rdd2.map(x=>s"--->${x}")
    println(rdd2.toDebugString)
    rdd4.collect()
    println(rdd2.toDebugString)
    //rdd4.collect()

    //val rdd4 = rdd3.reduceByKey(_+_)
//
    //val rdd5 = rdd4.map(_._1)
//
    //val rdd6 = rdd4.map(x=>(x._1,x._2 * 10))
//
//
    //rdd5.collect()
    //rdd6.collect()
    Thread.sleep(10000000)
  }
}
