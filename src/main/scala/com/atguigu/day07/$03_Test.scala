package com.atguigu.day07

object $03_Test {

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

    //创建累加器对象
    val acc = new Top10Accumulator

    //注册累加器
    sc.register(acc,"acc")

    rdd4.foreach(x =>acc.add(x))
    //取出累加结果
    val result = acc.value

    val top10 = result.toList.sortBy(_._2).reverse.take(10)

    top10.foreach(println(_))

    Thread.sleep(100000)
  }
}
