package com.transsnet.study.tableApi


import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment, _}
import org.apache.flink.table.api.scala.StreamTableEnvironment

import  org.apache.flink.table.api.scala._

/**
  * @author yinqi
  */
object FlinkTableTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //便于测试，并行度设置为1
    env.setParallelism(1)

    //
    val text: DataStream[String] = env.readTextFile("C:\\Users\\11597\\Desktop\\palmpay_new\\flink\\src\\main\\resources\\table_test.txt")

    val value = text
        //.flatMap(_.split(","))
      //先转化为对象
      .map(data => {val arr = data.split(",")
      TemperatureObj(arr(0),arr(1).toLong,arr(2).toDouble)
    })
    //创建table执行环境
     val tableEnv = StreamTableEnvironment.create(env)
    //调用dataApi转换  dataStream ->Table
    val dataTable = tableEnv.fromDataStream(value)
    //处理数据方式一 调用相关select filter等api
    //过滤数据
    val resultTable  =dataTable.select("id,temperature")//也可以写成('id,'temperature)
        .filter(" id == 'sensor_1'")
    //可以转化为datastream输出  Table->dataStream
    resultTable.toAppendStream[(String,Double)].print("result")

    //处理数据方式二 注册临时表 直接写sql
    //用table env把Table数据类型注册临时表
    tableEnv.createTemporaryView("temp_table",dataTable)

    val sql ="select id ,temperature from temp_table where id = 'sensor_1' "
     //执行sql得到 Table结果数据
    val resultSqlTable = tableEnv.sqlQuery(sql)

    //跟上面一样转化为datastream输出  Table->dataStream
    resultSqlTable.toAppendStream[(String,Double)].print("result-sql")

    env.execute("Table Api test")

  }

}
/*
case class  TempIncreWarning(val interval:Long) extends  KeyedProcessFunction[String,TemperatureObj,String] {
  //保存上一次温度状态
   lazy val  lastTempState = getRuntimeContext.getState[Double](new ValueStateDescriptor[Double]("temp-state",classOf[Double]))
  //保存上一次定时器状态
    lazy val lastTimerState = getRuntimeContext.getState[Long](new ValueStateDescriptor[Long]("timer-state",classOf[Long]))

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, TemperatureObj, String]#OnTimerContext, out: Collector[String]): Unit = {

    //当定时器触发时候调用这个方法 这个方法的逻辑只要输出报警数据
    out.collect("10s连续温度上升报警")

  }
  override def processElement(value: TemperatureObj, ctx: KeyedProcessFunction[String, TemperatureObj, String]#Context, out: Collector[String]): Unit = {
    //获取上一次温度
    val lastTemp = lastTempState.value()
    //更新温度状态
    lastTempState.update(value.temperature)
    //如果温度在上升 定时器状态值为0 首次要定时器状态是0 中间温度有下降因为定时器删除了，定时器状态也清除了 所以定时器状态还是0
    if(value.temperature>lastTemp && lastTimerState.value()==0){
     val timer = ctx.timerService().currentProcessingTime()+interval
      //注册定时器
      ctx.timerService().registerProcessingTimeTimer(timer)
      //更新定时器时间状态
      lastTimerState.update(timer)
    }
    //如果温度有下降 要删除原来定时器 重新建立定时器
    else if (value.temperature  < lastTemp){
      //获取上个定时器的时间
      val lastTimer = lastTimerState.value()
      ctx.timerService().deleteProcessingTimeTimer(lastTimer)
      //清除定时器时间状态
      lastTimerState.clear()
    }

  }
}
*/

case class TemperatureObj(id:String,timestamp:Long, temperature:Double)



