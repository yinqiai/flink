package com.transsnet.study.project

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple0}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable.ListBuffer


/**
  * @author yinqi
  * @date 2021/6/23
  */
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)
object HotItems {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream = env.readTextFile("C:\\Users\\11597\\Desktop\\palmpay_new\\flink\\src\\main\\resources\\UserBehavior.csv")
      .map(line => {
        val linearray = line.split(",")
        UserBehavior(linearray(0).toLong, linearray(1).toLong, linearray(2).toInt, linearray(3), linearray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior == "pv")
      .keyBy("itemId")
      .timeWindow(Time.minutes(60), Time.minutes(5))
      .aggregate(new CountAgg(), new WindowResultFunction())
      .keyBy(1)
      .process(new TopNHotItems(3))
      .print()
    env.execute("Hot Items Job")
  }

  // COUNT 统计的聚合函数实现，每出现一条记录加一
  class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(userBehavior: UserBehavior, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
  }

  // 用于输出窗口的结果
  class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, aggregateResult: Iterable[Long], collector: Collector[ItemViewCount]): Unit = {
      val itemId: Long = key.getField(0)
      val count = aggregateResult.iterator.next
      collector.collect(ItemViewCount(itemId, window.getEnd, count))
    }
  }

  // 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串
  class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
    private var itemState: ListState[ItemViewCount] = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      // 命名状态变量的名字和状态变量的类型
      val itemsStateDesc = new ListStateDescriptor[ItemViewCount]("itemState-state", classOf[ItemViewCount])
      // 从运行时上下文中获取状态并赋值
      itemState = getRuntimeContext.getListState(itemsStateDesc)
    }

    override def processElement(input: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
      // 每条数据都保存到状态中 itemState.add(input)
      // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于 windowEnd 窗口的所 有商品数据
      // 也就是当程序看到 windowend + 1 的水位线 watermark 时，触发 onTimer 回调函数
      context.timerService.registerEventTimeTimer(input.windowEnd + 1) }
      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit={
        // 获取收到的所有商品点击量
        val allItems: ListBuffer[ItemViewCount] = ListBuffer()
        import scala.collection.JavaConversions._
        for (item <- itemState.get) {
          allItems += item
        }
        // 提前清除状态中的数据，释放空间 itemState.clear()
        // 按照点击量从大到小排序
        val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
        val result: StringBuilder = new StringBuilder
        result.append("====================================\n")
        result.append("时间: ")
          .append(new Timestamp(timestamp - 1)).append("\n")
        for (i <- sortedItems.indices) {
          val currentItem: ItemViewCount = sortedItems(i)
          // e.g. No1： 商品 ID=12224 浏览量=2413
          result.append("No").append(i + 1).append(":").append(" 商品 ID=").append(currentItem.itemId).append(" 浏览量=").append(currentItem.count).append("\n")
        }
        result.append("====================================\n\n")
        // 控制输出频率，模拟实时滚动结果
        Thread.sleep(1000)
        out.collect(result.toString)
      }
    }


}