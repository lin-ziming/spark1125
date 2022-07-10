package com.atguigu.day09

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import org.apache.spark.sql.SaveMode
import org.junit.Test

class $01_MysqlReaderAndWriter {

  import org.apache.spark.sql.{Row, SparkSession}
  val spark = SparkSession.builder().master("local[4]").appName("test").getOrCreate()
  import spark.implicits._

  @Test
  def mysqlReader(): Unit ={

    val url = "jdbc:mysql://hadoop102:3306/gmall"
    //读取表中指定字段指定条件的数据
    //val tableName = "(select * from user_info where gender='M') user"
    //读取mysql整表
    val tableName = "user_info"

    //指定mysql账号密码
    val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","root123")

    //TODO 第一种读取方式[此种方式只会生成一个分区,此种方式只适合用于小数据量场景] <常用>
    val df1 = spark.read.jdbc(url,tableName,props)

    println(df1.rdd.getNumPartitions)


    //第二种方式[此种方式生成的dataFrame的分区数 = conditions数组元素个数, conditions数组中元素代表每个分区拉取数据的条件] <不用>
    //每个分区拉取数据的where条件
    val conditions = Array[String]("id<=50","id>50 and id<100","id>=100 and id<150","id>150")
    val df2 = spark.read.jdbc(url,tableName,conditions,props)
    df2.write.mode(SaveMode.Overwrite).csv("out1put/csv")

    //TODO 第三种方式[此种方式的dataframe的分区数 = (upperBound-lowerBound)<numPartitions ? upperBound-lowerBound : numPartitions ] <常用>

    //需要获取id字段最小值和最大值
    val df4 = spark.read.jdbc(url, "(select min(id) min_id,max(id) max_id from user_info) user"  ,props)

    val rows = df4.collect()
    //最小值与最大值值结果只有一行数据
    val row = rows.head

    val minid = row.getAs[Long]("min_id")
    val maxid = row.getAs[Long]("max_id")

    //列的类型必须是数字、日期、时间戳类型,用于后续分区的 【TODO 推荐使用主键字段,如果主键字段不是数字、日期、时间戳类型不能用】
    val columnName = "id"
    //是指columName字段的下限,用于后续分区的【TODO 推荐使用columnName的最小值】
    val lowerBound = minid
    //是指columName字段的上限,用于后续分区的【TODO 推荐使用columnName的最大值】
    val upperBound = maxid
    //分区数
    val numPartitions = 2000
    println(minid,maxid)
    val df3 = spark.read.jdbc(url,tableName,columnName,lowerBound,upperBound,numPartitions,props)

    println(df3.rdd.getNumPartitions)
    //df3.write.mode(SaveMode.Overwrite).csv("output/csv2")
  }


  @Test
  def mysqlwriter(): Unit ={

    val df = spark.read.json("datas/j1.json")

    //
    val url = "jdbc:mysql://hadoop102:3306/test"

    val tableName = "user"
    val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","root123")
    //TODO SaveMode.Append是采用insert into 的sql语句插入数据,所以如果mysql表有主键可能出现主键冲突
    // TODO 解决方案: foreachPartition + INSERT INTO `user`(`name`,age,address) VALUES('lisi10',31,'shenzhen') ON DUPLICATE KEY UPDATE age=31 ,address='shenzhen' 解决
    //df.write.mode(SaveMode.Append).jdbc(url,tableName,props)

    df.printSchema()
    val ds = df.as[(String,Long,String)]

    ds.rdd.foreachPartition(it=>{

      var connection:Connection = null
      var statement:PreparedStatement = null
      try{
        connection = DriverManager.getConnection(url,"root","root123")

        statement = connection.prepareStatement("INSERT INTO `user`(`name`,age,address) VALUES(?,?,?) ON DUPLICATE KEY UPDATE age=? ,address=?")

        var i = 1
        it.foreach{
          case (address,age,name) =>
            statement.setString(1,name)
            statement.setInt(2,age.toInt)
            statement.setString(3,address)
            statement.setInt(4,age.toInt)
            statement.setString(5,address)

            statement.addBatch()

            if(i % 1000 ==0 ){
              statement.executeBatch()
              statement.clearBatch()
            }
            i = i + 1

        }
        //提交最后一个批次
        statement.executeBatch()

      }catch {
        case e:Exception => e.printStackTrace()
      }finally {
        if(statement!=null)
          statement.close()
        if(connection!=null)
          connection.close()
      }

    })
  }
}
