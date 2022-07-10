package com.atguigu.day05

import org.apache.spark.rdd.RDD
//com.atguigu.day05.$04_Ser
object $04_Ser {

  /**
    *
    * 序列化的原因: Spark算子里面的代码是在Executor中的task执行的, spark算子外面的代码是在Driver执行的,如果spark算子中使用了Driver定义的对象
    *               就必须要求Driver将该对象序列化之后传递给task才能使用
    *spark序列化方式有两种:
    *       Java序列化: 序列化的时候会将类的继承信息、类的全类名、类的属性名、属性类型、属性值等信息都会序列化进去
    *       Kryo序列化: 序列化的时候只会将类的全类名、属性名、属性类型、属性值序列化
    *Spark默认使用的是java序列化方式, 工作中一般使用Kryo序列化方式,Kryo序列化性能比java序列化性能高10倍左右
    *如何配置spark序列化:
    *     1、在创建sparkconf的时候设置spark默认的序列化方式: new SparkConf().set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    *     2、将需要使用Kryo序列化的类注册[可选]:new SparkConf().registerKryoClasses(Array(classOf[类名1],classOf[类名2],....))
    *           类如果注册了,后续序列化的时候不会将全类名序列化进去
    *           类如果没有注册了,后续序列化的时候会将全类名序列化进去
    */
  def main(args: Array[String]): Unit = {

    import org.apache.spark.{SparkConf, SparkContext}
    val sc = new SparkContext(new SparkConf().registerKryoClasses(Array(classOf[Person],classOf[Array[Int]])).set("spark.serializer","org.apache.spark.serializer.KryoSerializer").setMaster("local[4]").setAppName("test"))


    val rdd = sc.parallelize(List(1,5,4,3,8,10))

    //val a = 7
    //val b:Integer = a
    //报错: 需要Driver的Person对象,需要序列化person对象传递给task，但是person不能序列化
    //val person = new Person
    //val rdd2 = rdd.map(x => x * person.y)
    //不报错:此时没有错误,因为person是在task内部创建的 不需要序列化传递
    //val rdd2 = rdd.map(x => x * new Person().y)

    //报错: 因为map中使用了this.y， this指代person对象,person对象定义在Driver中,需要序列化person对象传递给task,但是person不能序列化
    val person = new Person()
    //val rdd2 = person.m1(rdd)
    //不报错: 因为z是局部变量,z的赋值操作就是在Driver中进行的
    val rdd2 = person.m2(rdd)
    println(rdd2.collect().toList)
  }


}

class Person{
  val y = 10

  def m1(rdd:RDD[Int]):RDD[Int] = {
    rdd.map(x=>x* y)
  }

  def m2(rdd:RDD[Int]):RDD[Int] = {
    val z = this.y
    rdd.map(x=>x * z)
  }
}
