package com.transsnet.study.processFunction

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

/**
  * @author yinqi
  */
object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //便于测试，并行度设置为1
    env.setParallelism(1)

    //设置source 本地socket
    val text: DataStream[String] = env.socketTextStream("hadoop000", 9000)

    val value = text
        //.flatMap(_.split(","))
      //先转化为对象
      .map(data => {val arr = data.split(",")
      TemperatureObj(arr(0),arr(1).toLong,arr(2).toDouble)
    })
    val keyData:DataStream[String] =  value
      .keyBy(_.id).process(TempIncreWarning(10*1000L))

    keyData.print("tempincrewarning")

    env.execute("ProcessFunctionTest warning test")

  }

}
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

case class TemperatureObj(id:String,timestamp:Long, temperature:Double)



