package com.atguigu.day07

object $02_Test {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.{SparkConf, SparkContext}
    val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("test"))

    //1、读取数据
    val rdd = sc.textFile("datas/user_visit_action.txt")

    //2、过滤【过滤掉搜索行为】
    val rdd2 = rdd.filter(line=>{
      val arr = line.split("_")
      arr(5) == "null"
    })

    //3、列裁剪[只需要点击品类id,下单品类ids，支付品类ids]
    val rdd3 = rdd2.map(line=>{
      val arr = line.split("_")
      val clickid = arr(6)
      val orderids = arr(8)
      val payids = arr(10)

      (clickid,orderids,payids)
    })
    //4、炸开下单ids、支付ids、标注行的行为[ (品类id, (是否点击,是否下单,是否支付)) ]
    val rdd4 = rdd3.flatMap{
      case (clickid,orderids,payids) =>
        //点击行为
        if(clickid!="-1")
          List( (clickid,(1,0,0)) ) // (clickid,(1,0,0)) :: Nil
          //下单行为
        else if(orderids !="null"){
          val arr = orderids.split(",")
          val arr2 = arr.map(id=>(id,(0,1,0)))
          arr2
        }
        else
          payids.split(",").map(id=>(id,(0,0,1)))
    }
    //5、分组聚合
    val rdd5 = rdd4.reduceByKey((agg,curr) => ( agg._1 + curr._1, agg._2+curr._2 ,agg._3+curr._3 ))
    //6、排序取前十
    val top10 = rdd5.sortBy(_._2,false).take(10)

    //7、结果展示
    top10.foreach(println(_))

    Thread.sleep(1000000)
  }
}
