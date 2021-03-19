package com.transsnet.study.windowsFunction

/**
  * @author yinqi
  *
  */
//如果使用flink已经通过TypeInformation定义数据类型，TypeInformation类不会自动创建，需要隐试参数方式引入（如下），代码不会直接抛出异常，运行时候会报错
// could not find implicit value for evidence parameter of type org.apache.flink.api.common.typeinfo.TypeInformation[(String, Int)]
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, SlidingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
object AggregateFunctionCus {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //便于测试，并行度设置为1
    env.setParallelism(1)
    val inputStream = env.fromElements(("a",1L),("b",1L),("c",1L),("d",1L),("b",2L),("a",5L))

    val result = inputStream.keyBy(_._1)
      //指定窗口类型
        .window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(10)))
      //指定聚合函数逻辑，将根据id分组分组给第二个字段求平均
        .aggregate(new MyAverageAggregate)

    result.print()
    env.execute()
  }

  class  MyAverageAggregate extends AggregateFunction[(String,Long),(Long,Long),Double]{
    //初始化创建累加器
    override def createAccumulator(): (Long, Long) = (0L,0L)
    //把进入的数据叠加到累加器上
    override def add(in: (String, Long), acc: (Long, Long)): (Long, Long) = {
      (in._2+acc._1,acc._2+1L)
    }
   // 在累加器上求平均值
    override def getResult(acc: (Long, Long)): Double = {acc._1/acc._2}

    //把累加器合并
    override def merge(a: (Long, Long), b: (Long, Long)): (Long, Long) = {(a._1+b._1,a._2+b._2)}
  }
}
