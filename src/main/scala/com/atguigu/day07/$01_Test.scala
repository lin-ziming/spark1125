package com.atguigu.day07

object $01_Test {

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
    //4、统计每个品类的点击次数
    //4.1、过滤[只要点击数据]
    val clickFilterRdd = rdd3.filter(x=>x._1!="-1")
    //4.2、转换数据为 点击品类id->1
    val clickMapRdd = clickFilterRdd.map{
      case (clickid,orderids,payids) => (clickid,1)
    }
    //4.3、按照点击品类id分组统计点击次数
    val clickNumRdd = clickMapRdd.reduceByKey(_+_)

    //5、统计每个品类的下单次数
    //5.1、过滤[只需要下单数据]
    val orderFilterRdd = rdd3.filter(x=> x._2!="null")
    //5.2、切割下单品类ids+压平+转换【(下单品类id,1)】
    val orderMapRdd = orderFilterRdd.flatMap{
      case (clickid,orderids,payids) =>
        val arr = orderids.split(",")
        arr.map(id=>(id,1))
    }
    //5.3、统计每个品类的下单次数
    val orderNumRdd = orderMapRdd.reduceByKey(_+_)

    //6、统计每个品类的支付次数
    //6.1、过滤[只要支付数据]
    val payFilterRdd = rdd3.filter(x=>x._3!="null")

    //6.2、切割支付品类ids+压平+转换【(支付品类id,1)】
    val payMapRdd = payFilterRdd.flatMap{
      case (clickid,orderids,payids)=>
        val arr = payids.split(",")
        arr.map(id=>(id,1))
    }
    //6.3、统计每个品类的支付次数
    val payNumRdd = payMapRdd.reduceByKey(_+_)

    //7、三者join
    val clickOrderRdd = clickNumRdd.leftOuterJoin(orderNumRdd)

    val clickOrderPayRdd = clickOrderRdd.leftOuterJoin(payNumRdd)

    val numRdd = clickOrderPayRdd.map{
      case (id,( (clickNum,orderNum),payNum )) => ( id,( clickNum, orderNum.getOrElse(0) , payNum.getOrElse(0) ))
    }
    //8、对每个品类的点击次数、下单次数、支付排序,取前十
    val top10 = numRdd.sortBy(_._2,false).take(10)
    //9、结果展示
    top10.foreach(println(_))


    Thread.sleep(10000000)
  }
}
