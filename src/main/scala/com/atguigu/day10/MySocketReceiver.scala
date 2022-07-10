package com.atguigu.day10

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
  * 自定义Receiver
  *     1、创建class继承Receiver
  *     2、重写抽象方法
  */
class MySocketReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK){
  /**
    * recever启动时调用【读取数据】
    */
  override def onStart(): Unit = {

    new Thread(){
      override def run(): Unit = {
        receive
      }
    }.start()
  }

  def receive(): Unit ={
    val socket = new Socket(host,port)

    val br = new BufferedReader(new InputStreamReader(socket.getInputStream))

    var line:String = br.readLine()

    while( line != null ){

      //保存
      store(line)

      line = br.readLine()
    }

    br.close()
    socket.close()
  }
  /**
    * receiver停止的时候调用
    */
  override def onStop(): Unit = {

  }
}
