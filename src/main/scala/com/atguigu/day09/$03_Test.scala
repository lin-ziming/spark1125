package com.atguigu.day09

import org.apache.spark.sql.SaveMode

object $03_Test {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.{Row, SparkSession}
    val spark = SparkSession.builder().master("local[4]").appName("test").getOrCreate()
    import spark.implicits._

    //读取城市数据
    spark.read
      .option("sep","\t")
      .csv("datas/sql/city_info.txt")
      .toDF("city_id","city_name","region_name")
      .createOrReplaceTempView("city_info")
    //读取商品信息数据
    spark.read
      .option("sep","\t")
      .csv("datas/sql/product_info.txt")
      .toDF("product_id","product_name","product_type")
      .createOrReplaceTempView("product_info")
    //读取用户行为数据[只需要点击数据]
    spark.read
      .option("sep","\t")
      .csv("datas/sql/user_visit_action.txt")
      .toDF("action_date","user_id","session_id","page_id","action_time","query_key",
        "click_category_id","click_product_id","order_category_ids","order_product_ids","pay_category_ids","pay_product_ids","city_id")
      //只要点击数据[热门商品只看点击维度]
      .filter("click_category_id!='-1'")
      .createOrReplaceTempView("user_visit_action")


    //导入udaf函数
    import org.apache.spark.sql.functions._
    //注册
    spark.udf.register("cityMarket",udaf(new Top3UDAF))
    //join得到商品名称、区域名称,按照区域+商品分组,得到每个区域每个商品点击次数
    spark.sql(
      """
        |select
        |   b.region_name,
        |   c.product_name,
        |   count(1) click_num,
        |   cityMarket(b.city_name) city_market
        |from user_visit_action a join city_info b
        |     on a.city_id = b.city_id
        |join product_info c
        |     on a.click_product_id = c.product_id
        |group by b.region_name,c.product_name
      """.stripMargin).createOrReplaceTempView("t1")

    //取出每个区域点击数前三的商品
    spark.sql(
      """
        |select
        | a.region_name,
        | a.product_name,
        | a.click_num,
        | a.city_market
        |from(
        |   select
        |     t1.*,
        |     row_number() over(partition by t1.region_name order by t1.click_num desc) rn
        |   from t1
        |) a
        |where a.rn<=3
      """.stripMargin).write.mode(SaveMode.Overwrite).json("output/json")
  }
}
