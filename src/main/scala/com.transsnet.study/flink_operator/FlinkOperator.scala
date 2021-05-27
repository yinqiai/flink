package com.transsnet.study.flink_operator

import com.transsnet.study.userSource.UserSource.SensorReading
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.util.Collector
//如果使用flink已经通过TypeInformation定义数据类型，TypeInformation类不会自动创建，需要隐试参数方式引入（如下），代码不会直接抛出异常，运行时候会报错
// could not find implicit value for evidence parameter of type org.apache.flink.api.common.typeinfo.TypeInformation[(String, Int)]
import org.apache.flink.api.scala._

/**
  * @author yinqi
  * @date 2021/5/27
  *
  */
object FlinkOperator {
  def main(args: Array[String]): Unit = {
   val env= StreamExecutionEnvironment.getExecutionEnvironment

    //便于测试，并行度设置为1
     env.setParallelism(1)
    //1。测试读取本地文件
    /*  val txt = env.readTextFile("/Users/yinqi/test/test.txt")
      val xx =txt.flatMap(_.toLowerCase.split(" ").filter(_.nonEmpty).map((_,1)))
      val counts =txt.flatMap(_.toLowerCase.split(" ").filter(_.nonEmpty).map((_,1))).keyBy(0).sum(1)*/
    //counts.print()
    //==============flink支持数据类型：1。原生数据类型（Java基本类型（装箱），String类型 对应BasicTypeInfo）2。 java tuple类型 （new tuple2("a",1) 对应 TupleTYpeInfo）
    //==============3 scala Case class 对应 CaseClassTYpeInfo 包括scala tuple
    //==============4。pojos数据集对应PojoTYpeInfo,支持java和scala类，如下例子是scala类
    /*val personStream = env.fromElements(new Person("yinqi",18),new Person("yinruihang",6))
    val dataStream =personStream.keyBy("name")
    dataStream.print()*/
    //================5 Flink value类型  对应Value 内建的有IntValue DoubleValue String等
    //================6 比较特殊的数据类型，类如：scala map list类型数据集 少用
    //    val mapStream = env.fromElements(Map("name"->"yinqi","age"->30),Map("name"->"chenlimin","age"->30))
    //    val listStream = env.fromElements(List("name","yinqi","age",30,"name","chenlimin","age",30))
    //========================================================读取数据================================================================
    //1.csv格式
    /*    val csvStream=env.readFile(new CsvInputFormat[String] (new Path("/Users/yinqi/Desktop/chris/flink-doc/data_example.csv")){
          override def fillRecord(out: String, objects: Array[AnyRef]): String = ???
        },"/Users/yinqi/Desktop/chris/flink-doc/data_example.csv")*/
    //val counts =csvStream.flatMap(_.toLowerCase.split(",").filter(_.nonEmpty).map((_,1))).keyBy(0).sum(1)
    //counts.print()
    //=========================================================转换算子=====================================================================
    //1。keyBy  和 reduce(滚动进行数据聚合处理)
   /* val date=env.fromElements(("a",5),("a",2),("b",4),("c",3),("c",4),("e",5))
    val keyByDateStream =date.keyBy(0)
    val reduceDataStream = keyByDateStream.reduce{(t1,t2)=>(t1._1,t1._2+t2._2)}*/
    //1.2keyBy  和 reduce
    val txt = env.readTextFile("C:\\Users\\11597\\Desktop\\palmpay_new\\flink\\src\\main\\resources\\table_test.txt")
    val reduceDataStream =txt.map( data => { val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble) })
      .keyBy("id") .reduce( (x, y) => SensorReading(x.id, x.timestamp + 1, y.temperature) )

    //1.3 Split select
    //split 一个流分成多个流
    val splitStream=  reduceDataStream.split(data=>{if (data.temperature>30) Seq("high") else Seq("low") })
    val high =splitStream.select("high")
    val low =splitStream.select("low")
    val all =splitStream.select("high","low")

    //1.4 connect 和coMap coFlatMap(这两个算子其实就是在connect算子上 map 和flatMap)

    val warningStream = high.map(data=>(data.id,data.temperature))
    val connectStream = warningStream.connect(low)
    //看源码是fun1: IN1 => R, fun2: IN2 => R ，但是实际是两个流合并后里面可以是他们各自类型的数 据流，一个是三元组，一个是二元组
    val coMapStream = connectStream.map(
      warning=>(warning._1,warning._2,"warning"),
      low=>(low.id,"low")
    )

   /* Connect 与 Union 区别：
    1． Union 之前两个流的类型必须是一样，Connect 可以不一样，在之后的 coMap 中再去调整成为一样的。
    2. Connect 只能操作两个流，Union 可以操作多个。

    //1.5 union
     val unionStream =low.union(high)*/


   /* //1.6 富函数
    /*“富函数”是 DataStream API 提供的一个函数类的接口，所有 Flink 函数类都 有其 Rich 版本。它与常规函数的不同在于，可以获取运行环境的上下文，并拥有一 些生命周期方法，所以可以实现更复杂的功能。 
    RichMapFunction 
    RichFlatMapFunction 
    RichFilterFunction 
    … Rich Function 有一个生命周期的概念。典型的生命周期方法有： 
    open()方法是 rich function 的初始化方法，当一个算子例如 map 或者 filter 被调用之前 open()会被调用。 
    close()方法是生命周期中的最后一个调用的方法，做一些清理工作。 
    getRuntimeContext()方法提供了函数的 RuntimeContext 的一些信息，例如函 数执行的并行度，任务的名字，以及 state 状态*/

    val dataStream = env.fromElements((1),(2),(3),(4),(5))
    val dataS = dataStream.flatMap(new MyFlatMap)*/

    //1.7 sink kafka
    val union = high.union(low).map(_.temperature.toString)

    //发送到kafka的flink-sink-test这个topic
    union.addSink(new FlinkKafkaProducer011[String]("hadoop000:9092","hello_topic",new SimpleStringSchema()))

    //dataS.print()

    env.execute()
  }

  class MyFlatMap extends RichFlatMapFunction[Int, (Int, Int)] {
    var subTaskIndex = 0
    override def open(configuration: Configuration): Unit = {
      subTaskIndex = getRuntimeContext.getIndexOfThisSubtask
      println(subTaskIndex)
      // 以下可以做一些初始化工作，例如建立一个和 HDFS 的连接
      }
      override def flatMap(in: Int, out: Collector[(Int, Int)]): Unit = {
        if (in % 2 == subTaskIndex)
        { out.collect((subTaskIndex, in)) } }
      override def close(): Unit = {
        // 以下做一些清理工作，例如断开和 HDFS 的连接。
        }
      }
}
