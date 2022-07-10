package com.atguigu.day05

object $01_Test {

  """
    |select
    | a.省份id,
    | a.广告id,
    | a.num
    |from(
    |   select
    |    t.省份id,
    |    t.广告id,
    |    t.num,
    |    row_number() over(partition by 省份id order by num desc) rn
    |   from(
    |      select
    |         省份id,广告id,count(1) num
    |       from person
    |       group by 省份id,广告id
    |   ) t
    |) a where a.rn<=3
  """.stripMargin

  def main(args: Array[String]): Unit = {

    import org.apache.spark.{SparkConf, SparkContext}
    val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("test"))


    //1、读取文件
    val rdd1 = sc.textFile("datas/agent.log")
    //2、是否过滤[没有脏数据]、是否列裁剪[省份id、广告id]、是否去重[不能]
    val rdd2 = rdd1.map(line=> {
      val arr = line.split(" ")
      val province = arr(1)
      val adid = arr.last
      ( (province,adid) ,1 )
    })
    //RDD( ( (广东省,A),1 ) ,  ( (广东省,B),1 ) , ( (广东省,A),1 ) , ( (广东省,C),1 ) , ( (湖北省,A),1 ) ,....)
    //3、按照省份与广告id分组统计每个省份每个广告的点击次数
    val rdd3 = rdd2.reduceByKey((agg,curr)=>agg+curr)
    //RDD(
    //  (广东省,A) -> 10
    //  (广东省,B) -> 2
    //  (广东省,C) -> 30
    //   .....
    //  (湖北省,C) -> 23
    //   .....
    // )
    //4、按照省份分组
    val rdd4 = rdd3.groupBy{
      case ( (provinceid,_),_ ) => provinceid
    }
    //RDD(
    //    广东省 -> List( (广东省,A) -> 10 , (广东省,B) -> 2, (广东省,C) -> 30,..)
    //    湖北省 -> List( (湖北省,A) -> 4 , (湖北省,B) -> 23, (湖北省,C) -> 33,..)
    // )
    //5、按照对每个省份所有广告数据排序取前三
    val rdd5 = rdd4.map(x=>{
      //x = 广东省 -> List( (广东省,A) -> 10 , (广东省,B) -> 2, (广东省,C) -> 30,..)
      //转换数据 只要 (广告id,点击次数)
      val it = x._2.map{
        case ((_,adid),num) =>(adid,num)
      }

      //按照点击次数排序取前三
      val top3 = it.toList.sortBy(_._2).reverse.take(3)
      (x._1,top3 )
    })
    //RDD(
    //    广东省 -> List( E ->100, D->90, F->87)
    //    ....
    // )

    //6、结果展示
    println(rdd5.collect().toList)

  }
}
