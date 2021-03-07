package com.transsnet.study.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import scala.util.Properties

object KafkaConnector {
  def main(args: Array[String]): Unit = {
   val env = StreamExecutionEnvironment.getExecutionEnvironment

    // kafka 配置
    val ZOOKEEPER_HOST = "hadoop001:2181"
    val KAFKA_BROKERS = "hadoop001:9092"
    val TRANSACTION_GROUP = "flink-helper-label-count"
    val TOPIC_NAME = "pojo_topic"
    val kafkaProps = new Properties()
    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKERS)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)

    // 消费kafka数据
   //1.处理String
  /*  val streamData = env.addSource(
      new FlinkKafkaConsumer[String](TOPIC_NAME, new SimpleStringSchema(), kafkaProps)
    )

   val result=streamData.flatMap(_.toLowerCase().split(" ").filter(_.nonEmpty).map((_,1))).keyBy(0).sum(1)

   result.print()*/
   //2。处理自定义Java 类

   val streamData = env.addSource(
    new FlinkKafkaConsumer[SourceEvent](TOPIC_NAME, new SourceEventSchema(), kafkaProps)
   )

   val result=streamData.flatMap(_.getName.map((_,1))).keyBy(0).sum(1)
   result.print()

    env.execute()
  }



}
