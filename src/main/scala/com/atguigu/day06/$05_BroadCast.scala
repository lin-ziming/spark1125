package com.atguigu.day06

object $05_BroadCast {

  /**
    * 广播变量的使用场景
    *     1、spark算子中使用到Driver数据
    *         好处: 默认情况下Driver数据占用的空间大小 = task个数 * 数据大小,广播之后占用空间大小 = executor个数 * 数据大小,占用空间大大变小。
    *     2、大表join小表
    *         好处: 默认情况下会产生shuffle操作,此时将小表数据广播之后可以避免shuffle操作。
    *广播变量的用法
    *     1、广播Driver数据: val bc = sc.broadcast(数据)
    *     2、获取广播变量使用: bc.value
    *
    */

  /**
    * 场景1: spark算子中使用到Driver数据
    * @param args
    */
  def main2(args: Array[String]): Unit = {

    import org.apache.spark.{SparkConf, SparkContext}
    val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("test"))

    val rdd = sc.parallelize(List("jd","pdd","tm","atguigu","pt"))

    val map = Map[String,String]("jd"->"www.jd.com","pdd"->"www.pdd.com","tm"->"www.tm.com",
      "atguigu"->"www.atguigu.com")

    //广播数据
    val bc = sc.broadcast(map)

    val rdd2 = rdd.map(x=>{
      val map2 = bc.value
      map2.getOrElse(x,"")
      //map.getOrElse(x,"")
    })


    println(rdd2.collect().toList)

    Thread.sleep(10000000)
  }


  /**
    * 场景2: 大表 join 小表
    */
  def main(args: Array[String]): Unit = {

    import org.apache.spark.{SparkConf, SparkContext}
    val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("test"))

    val studetRdd = sc.parallelize(List(
      ("s01","zhangsan","A001"),
      ("s02","liming","A002"),
      ("s03","zhaotiezhu","A003"),
      ("s04","wagnwu","A002"),
      ("s05","zhaoliu","A001"),
      ("s07","hameimei","A005"),
      ("s08","lilei","A004"),
      ("s08","lilei","A009")
    ))

    val classRdd = sc.parallelize(List(
      ("A001","大数据班"),
      ("A002","java班"),
      ("A003","python班"),
      ("A004","法师班"),
      ("A005","辅助班")
    ))

    //需求: 获取每个学生信息以及所在班级名称
   //val stuRdd = studetRdd.map{
   //  case (stuid,stuname,classid) => (classid,(stuid,stuname))
   //}

   //val rdd1 = stuRdd.leftOuterJoin(classRdd)

   //val rdd2 = rdd1.map{
   //  case (classid,((id,name),className)) => (id,name,className.getOrElse(""))
   //}

   //rdd2.foreach(println(_))

    //1、收集小表数据到Driver端
    val classList = classRdd.collect().toMap

    //2、广播小表数据
    val bc = sc.broadcast(classList)

    //3、在spark算子中使用广播数据
    val rdd1 = studetRdd.map{
      case (id,name,classid) =>
        //取出小表广播数据
        val bcMap = bc.value
        (id,name, bcMap.getOrElse(classid,""))
    }

    println(rdd1.collect().toList)
    Thread.sleep(10000000)
  }
}
