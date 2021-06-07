package com.transsnet.study.tableApi

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Tumble
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
  * @author yinqi
  */
object FlinkTableWindowsTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //便于测试，并行度设置为1
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //val text: DataStream[String] = env.readTextFile("C:\\Users\\11597\\Desktop\\palmpay_new\\flink\\src\\main\\resources\\table_test.txt")
    val text: DataStream[String] =  env.socketTextStream("hadoop000", 9000)
    val value = text
        //.flatMap(_.split(","))
      //先转化为对象
      .map(data => {val arr = data.split(",")
      TemperatureObj(arr(0),arr(1).toLong,arr(2).toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[TemperatureObj](Time.milliseconds(1000)) {
        override def extractTimestamp(element: TemperatureObj): Long = {
          return element.timestamp*1000
        }
      })
    //创建table执行环境
     val tableEnv = StreamTableEnvironment.create(env)
    val tempTable = tableEnv.fromDataStream(value,'id,'temperature,'timestamp.rowtime as 'ts)

    //实现方式一 ，通过table api实现  开窗 GroupWindow OverWindow scala 里面一个参数的调用函数可以不用. 用空格
    val resultTable=tempTable
      .window(Tumble over 10.seconds on 'ts as 'tw  )//10S 滚动一次
        .groupBy('id,'tw)
      //可以拿到窗口里面的count avg,还可以拿到窗口的一些信息
        .select('id,'id.count,'temperature.avg,'tw.end)
    //实现方式二 ，通过sql实现
    tableEnv.createTemporaryView("temperature",tempTable)
    val resultsqlTable = tableEnv.sqlQuery(
      """|select id,
         |count(id),
         |avg(temperature),
         |tumble_end(ts, interval '10' second)
         |from temperature
      |group by
    |id,
    |tumble(ts, interval '10' second)
       """.stripMargin)

    resultTable.toAppendStream[Row].print("result")
    resultsqlTable.toRetractStream[Row].print("sql")
    env.execute("Table Api and sql test")

  }

}
case class TemperatureObj(id:String,timestamp:Long, temperature:Double)



