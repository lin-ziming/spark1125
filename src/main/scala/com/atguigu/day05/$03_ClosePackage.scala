package com.atguigu.day05

object $03_ClosePackage {

  /**
    * 闭包: 函数体中使用了外部变量的函数称之为闭包
    */
  def main(args: Array[String]): Unit = {

    println(func(30))


  }

  val y = 10
  //闭包函数
  val func = (x:Int) => x+y
}
