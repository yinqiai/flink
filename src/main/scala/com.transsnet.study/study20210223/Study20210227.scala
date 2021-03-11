package com.transsnet.study.study20210223

import java.util.stream.Collector

import org.apache.flink.streaming.api.functions.co.{CoFlatMapFunction, CoMapFunction}
import org.apache.flink.streaming.api.operators.TimestampedCollector
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.util

object Study20210227 {
  def main(args: Array[String]): Unit = {
   //1.connect coFlatMap coMap
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val  dateStream1 = env.fromElements(("a",1),("b",1),("c",1),("d",1),("b",2),("a",5))
    val  dateStream2 = env.fromElements(1,2,3,4,5,6)
    //1.1普通的连接
/*    val connectStream = dateStream1.connect(dateStream2)
    val resStream = connectStream.map(new CoMapFunction[(String,Int),Int,(Int,String)] {
      override def map1(in1: (String, Int)): (Int, String) = {(in1._2,in1._1)}

      override def map2(in2: Int): (Int, String) = {(in2,"default")}
    })*/

/*    val resStream = connectStream.flatMap(new CoFlatMapFunction[(String,Int),Int,(String,Int,Int)]{
      var number =0;
      override def flatMap1(in1: (String, Int), out: util.Collector[(String,Int ,Int)]): Unit = {
             out.collect((in1._1,in1._2,number))
      }

      override def flatMap2(in2: Int, out: util.Collector[(String,Int ,Int)]): Unit = {
           number=in2
      }
    })*/

    //1.2通过keyby函数根据指定的key连接两个数据集
     val  ketConn = dateStream1.connect(dateStream2).keyBy(0,1)
    //1.3 通过 broadcast关联两个数据集
    val broadcastConn = dateStream1.connect(dateStream2.broadcast())

    val resStream = ketConn.map(new CoMapFunction[(String,Int),Int,(Int,String)] {
      override def map1(in1: (String, Int)): (Int, String) = {(in1._2,in1._1)}

      override def map2(in2: Int): (Int, String) = {(in2,"default")}
    })

    // 2.
    resStream.print()

    env.execute()



  }

}
