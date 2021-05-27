package com.transsnet.study.state

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

/**
  * @author yinqi
  */
object StateTest {
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

    val value = text
        //.flatMap(_.split(","))
      //先转化为对象
      .map(data => {val arr = data.split(",")
      TemperatureObj(arr(0),arr(1).toLong,arr(2).toDouble)
    })

    val keyData:KeyedStream[TemperatureObj,String] =  value.keyBy(_.id)
    // 需求 当相邻数据两个温差大于1.7阈值 输出数据
     val result:DataStream[(String,Double,Double)] = keyData
       //方式1 自己实现状态管理
      // .flatMap(new TemperatureAlertFunction(1.7))
       //方式二 flink 自带的 还可以解决第一次数据进来时候bug
         .flatMapWithState[(String,Double,Double),Double]({
       //数据第一次进入时候 状态为空，这时候输出的集合为空，但是要更新温度为最新的状态
       case(temp,None)=>(List.empty,Some(temp.temperature))
       case(temp,lastTemp:Some[Double])=>{
         //求温差
         val  tempdiff = (temp.temperature-lastTemp.get).abs
         //只要温差大于阈值 就放入集合输出
         if(tempdiff>=1.7) {
           (List((temp.id,temp.temperature,tempdiff)),Some(temp.temperature))
         }//温差没有超过1.7 一样要更新状态，但是这时候返回的集合要给空
         else{
           (List.empty,Some(temp.temperature))
         }
       }
     })

    result.print("result-alert")

    env.execute("state test")

  }

  class TemperatureAlertFunction(val threshold:Double) extends RichFlatMapFunction[TemperatureObj,(String,Double,Double)]{
    //lazy 用到时候才初始化变量 保存上一下温度的值
    lazy val lastTempState:ValueState[Double] = getRuntimeContext.getState[Double](new ValueStateDescriptor[Double]("lasttemp",classOf[Double]))

    override def flatMap(value: TemperatureObj, out: Collector[(String, Double, Double)]): Unit = {
      //获取 这里有一个bug 就是数据第一次进来时候获取的lastTemp 为0，所以第一次数据会输出
      val lastTemp = lastTempState.value()
      //求温差
      val  tempdiff = (value.temperature-lastTemp).abs
      //只要温差大于阈值 就放入集合输出
      if(tempdiff>=threshold) {out.collect((value.id,value.temperature,tempdiff))

      }
      //每次更新状态为最新的温度
      lastTempState.update(value.temperature)
      //
    }
  }

}

case class TemperatureObj(id:String,timestamp:Long, temperature:Double)



