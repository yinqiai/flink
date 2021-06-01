package com.transsnet.study.watermark


import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import scala.collection.mutable
//这个包是错误的
//import org.apache.flink.streaming.api.datastream.{DataStream, KeyedStream}
//这个是正确的
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment, _}
/**
  * @author yinqi
  * @date 2021/6/1
  *
  */
object EventTimeWindowsWM {

    def main(args: Array[String]): Unit = {
      // 环境
      val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
      env.setParallelism(1)
      val dstream: DataStream[String] = env.socketTextStream("hadoop000", 9000)

      val textWithTsDstream = dstream.map (text => {val arr = text.split(" ")
        (arr(0), arr(1).toLong, 1)} )

      val textWithEventTimeDstream: DataStream[(String, Long, Int)] = textWithTsDstream
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Int)](Time.milliseconds(1000)) {
          override def extractTimestamp(element: (String, Long, Int)): Long = {
            return element._2*1000
          }
        })
      val textKeyStream: KeyedStream[(String, Long, Int), Tuple] = textWithEventTimeDstream.keyBy(0)

      //textKeyStream.print("textkey:")
      val windowStream: WindowedStream[(String, Long, Int), Tuple, TimeWindow] = textKeyStream
        //滚动窗口
        //.window(TumblingEventTimeWindows.of(Time.seconds(2)))
      //滑动窗口
        .window(SlidingEventTimeWindows.of(Time.seconds(2),Time.milliseconds(1000)))
     /* sensor_1 1622450400
      sensor_1 1622450402
      sensor_1 1622450403
      sensor_1 1622450400
      sensor_1 1622450402
      sensor_1 1622450402
      sensor_2 1622450402
      sensor_3 1622450402
      sensor_1 1622450406
      sensor_1 1622450403
      sensor_2 1622450408
      sensor_2 1622450409*/
      val groupDstream: DataStream[mutable.ListBuffer[Long]] = windowStream
        .fold(new mutable.ListBuffer[Long]()) { case (set, (key, ts, count)) => set += ts }

      groupDstream.print("window::::").setParallelism(1)

      env.execute()
    }

}
