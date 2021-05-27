package com.transsnet.study.userSource

import java.util.Random

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
/**
  * @author yinqi
  * @date 2021/5/27
  *      自定义flink source
  */
object UserSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream4 = env.addSource( new MySensorSource() )

   stream4.print()

    env.execute("Table Api test")
  }

  class MySensorSource extends SourceFunction[SensorReading] {
    // flag: 表示数据源是否还在正常运行
    var running: Boolean = true

    override def cancel(): Unit = {
      running = false
    }

    override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
      // 初始化一个随机数发生器
      val rand = new Random()
      var curTemp = 1.to(10).map(i => ("sensor_" + i, 65 + rand.nextGaussian() * 20))
      while (running) {
        // 更新温度值
        curTemp = curTemp.map(t => (t._1, t._2 + rand.nextGaussian()))
        // 获取当前时间戳
        val curTime = System.currentTimeMillis()
        curTemp.foreach(t => ctx.collect(SensorReading(t._1, curTime, t._2)))
        Thread.sleep(100)
      }
    }
  }
  // 定义样例类，传感器 id，时间戳，温度
  case class SensorReading(id: String, timestamp: Long, temperature: Double)
}
