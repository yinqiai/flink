package com.transsnet.study

import java.text.SimpleDateFormat
import java.util.Objects

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector



/**
  * @author yinqi
  */
object WaterMarkWindows {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //便于测试，并行度设置为1
    env.setParallelism(1)

    //env.getConfig.setAutoWatermarkInterval(9000)

    //设置为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //设置source 本地socket
    val text: DataStream[String] = env.socketTextStream("hadoop000", 9000)


    val lateText = new OutputTag[(String, Long, Long, Long)]("late-data")

    val value = text
      .filter(new MyFilterNullOrWhitespace)
      .flatMap(new MyFlatMap)
      .assignTimestampsAndWatermarks(new MyWaterMark)
      .map(x => (x.name, x.datetime, x.timestamp, 1L))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .sideOutputLateData(lateText)
      //.sum(2)
      .apply(new MyWindow)
    //.window(TumblingEventTimeWindows.of(Time.seconds(3)))
    //.apply(new MyWindow)
    value.getSideOutput(lateText).map(x => {
      "延迟数据|name:" + x._1 + "|datetime:" + x._2
    }).print()

    value.print()

    env.execute("watermark test")

  }
}

class MyFilterNullOrWhitespace extends FilterFunction[String]{
  override def filter(value: String): Boolean = if (Objects.isNull(value)||value=="") false else true
}


class MyFlatMap extends  FlatMapFunction[String,EventObj]{
  override def flatMap(value: String, out: Collector[EventObj]): Unit = {
   // val data = JSON.parseObject(value, SimpleEventObj.class)
    val jsonObject = JSON.parseObject(value)
    val name = jsonObject.getString("name")
    val datetime = jsonObject.getString("datetime")

    val datetime1 :Long= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(datetime).getTime
    //val timestamp :Long=new Date().getTime

    val eventObj =new EventObj(datetime1,name,datetime1)

    out.collect(eventObj)

  }
}

class MyWindow extends  WindowFunction[(String, Long, Long, Long), (String, Long, String, String,String,String), String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long, Long, Long)], out: Collector[(String, Long, String, String,String,String)]): Unit =
    {
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      //key,窗口内元素个数，窗口内最早元素的时间，窗口内最晚元素的时间，窗口自身开始时间，窗口自身结束时间
      out.collect((key, input.toList.size,sdf.format(input.head._2), sdf.format(input.last._2),sdf.format( window.getStart),sdf.format( window.getEnd)))}
}

class MyWaterMark extends AssignerWithPeriodicWatermarks[EventObj] {

  val maxOutOfOrderness = 10000L // 3.0 seconds
  var currentMaxTimestamp = 0L

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  /**
    * 用于生成新的水位线，新的水位线只有大于当前水位线才是有效的
    *
    * 通过生成水印的间隔（每n毫秒）定义 ExecutionConfig.setAutoWatermarkInterval(...)。
    * getCurrentWatermark()每次调用分配器的方法，如果返回的水印非空并且大于先前的水印，则将发出新的水印。
    *
    * @return
    */
  override def getCurrentWatermark: Watermark = {
    new Watermark(this.currentMaxTimestamp - this.maxOutOfOrderness)
  }

  /**
    * 用于从消息中提取事件时间
    *
    * @param element                  EventObj
    * @param previousElementTimestamp Long
    * @return
    */
  override def extractTimestamp(element: EventObj, previousElementTimestamp: Long): Long = {

    currentMaxTimestamp = Math.max(element.timestamp, currentMaxTimestamp)

    val id = Thread.currentThread().getId
    println("currentThreadId:" + id + ",key:" + element.name + ",eventTime:[" + sdf.format(element.datetime) + "],currentMaxTimestamp:[" + sdf.format(currentMaxTimestamp) + "],watermark:[" + sdf.format(getCurrentWatermark().getTimestamp) + "]")

    element.timestamp
  }
}

case class EventObj(timestamp:Long,name:String,datetime:Long)



