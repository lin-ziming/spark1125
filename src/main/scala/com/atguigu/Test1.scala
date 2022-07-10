package com.atguigu

object Test1 {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder().master("local[4]").appName("test").getOrCreate()

    spark.read.csv("datas/score.txt")
      .toDF("name","score")
      .selectExpr("name","cast(score as bigint) score")
      .createOrReplaceTempView("stu_score")

    spark.sql(
      """
        |select
        |  name,
        |  score,
        |  lag(name) over( order by score asc) before_name
        |from stu_score
      """.stripMargin).createOrReplaceTempView("t1")

    /**
      * +----+-----+-----------+
      * |name|score|before_name|
      * +----+-----+-----------+
      * |语文|    0|       null|
      * |语文|   10|       语文|
      * |语文|   20|       语文|
      * |数学|   40|       语文|
      * |数学|   50|       数学|
      * |语文|   50|       数学|
      * |英语|   51|       语文|
      * |英语|   52|       英语|
      * |英语|   59|       英语|
      * |语文|   60|       英语|
      * |英语|   61|       语文|
      * |数学|   70|       英语|
      * |数学|   80|       数学|
      * |数学|   90|       数学|
      * |英语|   97|       数学|
      * |英语|   98|       英语|
      * |英语|  100|       英语|
      * +----+-----+-----------+
      */

    spark.sql(
      """
        |select
        |   name,
        |   score,
        |   case when name=before_name then 0 else 1 end rn
        |from t1
        |
      """.stripMargin).createOrReplaceTempView("t2")

    spark.sql(
      """
        |select
        | name,
        | score,
        | sum(rn) over(rows BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) rn2,
        | rank(rn) over(order by score) rn3
        |from t2
      """.stripMargin).show

    /**
      * +----+-----+---+
      * |name|score| rn|
      * +----+-----+---+
      * |语文|    0|  1|
      * |语文|   10|  0|
      * |语文|   20|  0|
      * |数学|   40|  1|
      * |数学|   50|  0|
      * |语文|   50|  1|
      * |英语|   51|  1|
      * |英语|   52|  0|
      * |英语|   59|  0|
      * |语文|   60|  1|
      * |英语|   61|  1|
      * |数学|   70|  1|
      * |数学|   80|  0|
      * |数学|   90|  0|
      * |英语|   97|  1|
      * |英语|   98|  0|
      * |英语|  100|  0|
      * +----+-----+---+
      */
  }
}
