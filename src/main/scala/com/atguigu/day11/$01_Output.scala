package com.atguigu.day11

import java.sql.{Connection, DriverManager, PreparedStatement}

object $01_Output {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.SparkConf
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    val conf = new SparkConf().setMaster("local[4]").setAppName("test")/*.set("spark.streaming.backpressure.enabled","true")*/
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.sparkContext.setLogLevel("warn")

    val ds = ssc.socketTextStream("hadoop102",9999)

    val ds2 = ds.flatMap(_.split(" "))
      .map(x=>(x,1))
      .reduceByKey(_+_)
    //工作中一般不会将数据保存到磁盘中,因为会写入大量的小文件、写磁盘速度比较慢影响处理数据的时效性
    //工作中流水数据的处理结果一般保存到mysql\hbase\redis等位置
    //ds.saveAsTextFiles("output")

    //foreachRDD(func: RDD[元素类型]=>Unit):Unit 对每个批次RDD遍历处理 <工作常用>
    //    foreachRDD里面的函数是针对一个批次RDD处理
    ds2.foreachRDD( rdd=> {

      //TODO 此处处于RDD算子外部,是在Driver执行的

      rdd.foreachPartition( it => {
        //TODO 此处是RDD算子内部,是在Task执行
        var connection:Connection = null
        var statement:PreparedStatement = null

        try{

          connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test","root","root123")

          statement = connection.prepareStatement("insert into wc values(?,?)")

          var i = 1
          it.foreach{
            case (wc,num) =>
              statement.setString(1,wc)
              statement.setInt(2,num)

              statement.addBatch()

              if( i%1000==0 ){
                statement.executeBatch()
                statement.clearBatch()
              }
              i =  i + 1
          }

          statement.executeBatch()

        }catch {
          case e:Exception =>e.printStackTrace()
        }finally {
          if(statement!=null)
            statement.close()
          if(connection!=null)
            connection.close
        }
      } )

    } )

    ssc.start()
    ssc.awaitTermination()
  }
}
