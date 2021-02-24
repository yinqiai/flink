package com.transsnet.study.study20210223

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

/**
  * @author yinqi
  *
  */
object Study20210223 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //便于测试，并行度设置为1
    //env.setParallelism(1)
     TypeInformation
     val txt = env.readTextFile("file:///C:\\Users\\11597\\Downloads\\test\\test.txt")

    val counts =txt.flatMap(_.toLowerCase.split(" ").filter(_.nonEmpty).map((_,1))).keyBy(0).sum(1)

    counts.print()

    env.execute()
  }
}
