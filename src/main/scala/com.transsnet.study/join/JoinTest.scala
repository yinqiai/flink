package com.transsnet.study.join


import org.apache.flink.streaming.api.TimeCharacteristic

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

/**
  * @author yinqi
  * @date 2021/8/12
  */
object JoinTest {
  def main(args: Array[String]): Unit = {
    //    if(args.length != 3){
    //      System.err.println("缺少必要参数")
    //      return
    //    }
    //    val hostname = args(0)
    //    val port1 = args(1).toInt
    //    val port2 = args(2).toInt
    val hostname ="hadoop000"
    val port1 = 9000
    val port2 = 9001

    //指定运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1);
    // 这里 一定要设置 并行度为1
    // 接入两个网络输入流
    val a = env.socketTextStream(hostname, port1)
    val b = env.socketTextStream(hostname, port2)

    //将字符串转换成二元组
    val as = a.map(x=>{
      //println("a=>"+x+",时间=>"+Utils.getStringDate3(x.split(",")(1).toLong))
      ("key",x.split(",")(0),x.split(",")(1).toLong)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, String, Long)](Time.seconds(1)) {
      override def extractTimestamp(t: (String, String, Long)): Long = t._3
    })

    val bs = b.map(x=>{
     // println("b=>"+x+",时间=>"+Utils.getStringDate3(x.split(",")(1).toLong))
      ("key",x.split(",")(0),x.split(",")(1).toLong)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, String, Long)](Time.seconds(1)) {
      override def extractTimestamp(t: (String, String, Long)): Long = t._3
    })

    as
      .join(bs)
      .where(_._1)
      .equalTo(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .apply{
        (t1 : (String,String, Long), t2 : (String,String, Long), out : Collector[(String,String, Long,Long)]) =>
//          println("t1=>"+t1+",时间=>"+Utils.getStringDate3(t1._3))
//          println("t2=>"+t2+",时间=>"+Utils.getStringDate3(t2._3))
          // 直接拼接到一起
          out.collect((t1._2,t2._2,t1._3,t2._3))
      }
      .map(x=>{
        println("结果输出=>"+x)
      })

    // 执行程序
    env.execute("joinedStreams")

  }


}
