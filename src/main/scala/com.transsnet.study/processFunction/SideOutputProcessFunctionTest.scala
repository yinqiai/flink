package com.transsnet.study.processFunction

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * @author yinqi
  *         侧输出流
  */
object SideOutputProcessFunctionTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //便于测试，并行度设置为1
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //设置source 本地socket
    val text: DataStream[String] = env.socketTextStream("hadoop001", 9000)

    val value = text
        //.flatMap(_.split(","))
      //先转化为对象
      .map(data => {val arr = data.split(",")
      TemperatureObj(arr(0),arr(1).toLong,arr(2).toDouble)
    })

    val monitoredReadings: DataStream[TemperatureObj] = value.process(new FreezingMonitor)

    //输出侧输出里面数据 这里面freezing-alarms一定要和（lazy val freezingAlarmOutput: OutputTag[String] = new OutputTag[String]("freezing-alarms")一样
    monitoredReadings.getSideOutput(new OutputTag[String]("freezing-alarms")) .print()

    //输出所有数据
    value.print()

    env.execute("ProcessFunctionTest warning test")

  }

}


class FreezingMonitor extends ProcessFunction[TemperatureObj, TemperatureObj] {
  // 定义一个侧输出标签
  lazy val freezingAlarmOutput: OutputTag[String] = new OutputTag[String]("freezing-alarms")

  override def processElement(r: TemperatureObj, ctx: ProcessFunction[TemperatureObj, TemperatureObj]#Context, out: Collector[TemperatureObj]): Unit = { // 温度在 32F 以下时，输出警告信息
    if (r.temperature < 32.0) {
      ctx.output(freezingAlarmOutput, s"Freezing Alarm for ${r.id}")
    }
    // 所有数据直接常规输出到主流
    out.collect(r)
  }
}




