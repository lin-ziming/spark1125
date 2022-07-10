package com.atguigu

object Test2 {

  def main(args: Array[String]): Unit = {


    import org.apache.spark.sql.{Row, SparkSession}
    val spark = SparkSession.builder().master("local[4]").appName("test").getOrCreate()
    import spark.implicits._

    spark.read.option("sep","\t").csv("datas/session.txt")
      .toDF("user_id","action_time","page")
      .createOrReplaceTempView("user_action")

    spark.sql(
      """
        |select
        |   user_id,
        |   action_time,
        |   page,
        |   lag(action_time) over(partition by user_id order by action_time asc ) befor_action_time
        |from user_action
      """.stripMargin).createOrReplaceTempView("t1")

    /*spark.sql(
      """
        |select
        |   user_id,
        |   action_time,
        |   page,
        |   case
        |     when unix_timestamp(befor_action_time) is null then 1
        |     when unix_timestamp(action_time) - unix_timestamp(befor_action_time) <= 30 * 60 then 0
        |     else 1
        |   end rn
        |from t1
      """.stripMargin).createOrReplaceTempView("t2")*/


    /* spark.sql(
       """
         |
         |select
         | sum(rn) over(ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT  ROW ) session_id,
         | user_id,
         | action_time,
         | page
         |from t2
       """.stripMargin).createOrReplaceTempView("t3")

     spark.sql(
       """
         |
         |select
         | session_id,
         | user_id,
         | action_time,
         | page,
         | row_number() over(partition by session_id order by action_time ) rn
         |from t3
       """.stripMargin).show*/

    spark.sql(
      """
        |select
        |  user_id,
        |  action_time,
        |  page,
        |  case
        |    when unix_timestamp(befor_action_time) is null then concat(user_id,'-',unix_timestamp(action_time))
        |    when unix_timestamp(action_time) - unix_timestamp(befor_action_time) <= 30 * 60 then null
        |    else concat(user_id,'-',unix_timestamp(action_time))
        |  end session_start_point
        |from t1
      """.stripMargin).createOrReplaceTempView("t3")

    spark.sql(
      """
        |
        |select
        |   user_id,
        |   action_time,
        |   page,
        |   last_value(session_start_point,true) over(partition by user_id order by action_time) session
        |from t3
      """.stripMargin).createOrReplaceTempView("t4")

    spark.sql(
      """
        |
        |select
        | session,
        | user_id,
        | action_time,
        | page,
        | row_number() over(partition by session order by action_time ) rn
        |from t4
      """.stripMargin).show
  }
}
