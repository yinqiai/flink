package com.transsnet.study.windowsFunction

import com.transsnet.study.userSource.UserSource.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._

/**
  * @author yinqi
  * @date 2021/5/27
  */
object TimeWindows {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //便于测试，并行度设置为1
    env.setParallelism(1)
    val txt = env.readTextFile("C:\\Users\\11597\\Desktop\\palmpay_new\\flink\\src\\main\\resources\\table_test.txt")

    val reduceDataStream =txt.map( data => { val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble) })
      .keyBy("id") .reduce( (x, y) => SensorReading(x.id, x.timestamp + 1, y.temperature) )
    //1.滚动窗口
    val minTempPerWindow = reduceDataStream.map(r => (r.id, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(15))
      .reduce((r1, r2) => (r1._1, r1._2.min(r2._2)))

    minTempPerWindow.print()

    //2.

    env.execute()
  }

}
