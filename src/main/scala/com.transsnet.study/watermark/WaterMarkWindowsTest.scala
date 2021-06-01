package com.transsnet.study.watermark

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @author yinqi
  */
object WaterMarkWindowsTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //便于测试，并行度设置为1
    env.setParallelism(1)
    //设置为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
   //设置提取watermark间隔时间
    env.getConfig.setAutoWatermarkInterval(50)
    //设置source 本地socket
    val text: DataStream[String] = env.socketTextStream("hadoop000", 9000)
    val lateText = new OutputTag[(String,Long,Double)]("late-data")
    val value = text
        //.flatMap(_.split(","))
      //先转化为对象
      .map(data => {val arr = data.split(",")
      TemperatureObj(arr(0),arr(1).toLong,arr(2).toDouble)
    })
      //再转化为3元祖
      .map(data=>(data.name,data.timestamp,data.temperature))
      //抽取TemperatureObj 的timestamp 这里会取最新的数据的的timestamp 为窗口时间 ，并设置3秒为延迟数据到达时间（计算水位线用的）
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String,Long,Double)](Time.seconds(3)) {
       //sensor_1,1547718225,12 把1547718225秒*1000转化为毫秒
        override def extractTimestamp(element: (String,Long,Double)): Long = element._2*1000
      })
      .keyBy(_._1)
      //设置窗口为滚动窗口，窗口大小为15s
      .window(TumblingEventTimeWindows.of(Time.seconds(15)))
      //允许最大延迟时间是1min（经过测试这个时间是相对watermark的最近一个窗口的结束时间（非常重要）， 而不是最新的数据时间）,注意，如果没有设置这个，则窗口只会在水位线过了之后触发计算一次（因为窗口已经关闭）
      .allowedLateness(Time.minutes(1))
      //迟到的数据放侧输出流里面
      .sideOutputLateData(lateText)
      //窗口计算逻辑 计算逻辑为取name分组后的最早时间和最小温度值
        .reduce((lastTemp,newTemp)=> (lastTemp._1,lastTemp._2.min(newTemp._2),lastTemp._3.min(newTemp._3)))
        //.apply(new WindowFunction[] {})
    //.window(TumblingEventTimeWindows.of(Time.seconds(3)))
    //.apply(new MyWindow)
    //输出延迟数据
    value.getSideOutput(lateText).map(x => {
      "延迟数据|name:" + x._1 + "延迟数据|datetime:" + x._2+ "延迟数据|temp:" + x._3
    }).print("late")
    //输出窗口计算结果
    value.print("result")
    env.execute("watermark test")

  }
}

case class TemperatureObj(name:String,timestamp:Long, temperature:Double)



