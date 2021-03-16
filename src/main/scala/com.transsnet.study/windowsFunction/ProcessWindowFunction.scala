package com.transsnet.study.windowsFunction


import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
//如果使用flink已经通过TypeInformation定义数据类型，TypeInformation类不会自动创建，需要隐试参数方式引入（如下），代码不会直接抛出异常，运行时候会报错
// could not find implicit value for evidence parameter of type org.apache.flink.api.common.typeinfo.TypeInformation[(String, Int)]
import org.apache.flink.api.scala._

/**
  * @author yinqi
  *
  */
object ProcessWindowFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //便于测试，并行度设置为1
    env.setParallelism(1)

    val inputStream = env.fromElements(("a",1L,1),("b",1L,2),("c",1L,3),("d",1L,4),("b",2L,5),("a",5L,6))
    val resultStream =   inputStream.keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .reduce(
        //定义reduceFunction,完成求最小值的逻辑
        (r1:(String,Long,Int),r2:(String,Long,Int))=>{if (r1._2>r2._2) r2 else r1}
        //定义ProcessWindowsFunction 完成对窗口元数据的采集
        ,(key : String,
          window : TimeWindow,
          minReadings:Iterable[(String,Long,Int)],
          out:Collector[(Long,(String,Long,Int))])
        =>{
          val min =minReadings.iterator.next()
           out.collect((window.getEnd,min))
        }
      )

    resultStream.print()
     env.execute()
  }
}
