package com.transsnet.study.study20210223

//import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.CsvInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.hadoop.mapred.FileInputFormat
//如果使用flink已经通过TypeInformation定义数据类型，TypeInformation类不会自动创建，需要隐试参数方式引入（如下），代码不会直接抛出异常，运行时候会报错
// could not find implicit value for evidence parameter of type org.apache.flink.api.common.typeinfo.TypeInformation[(String, Int)]
import org.apache.flink.api.scala._

/**
  * @author yinqi
  *
  */
object Study20210223 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //便于测试，并行度设置为1
    //env.setParallelism(1)
    //1。测试读取本地文件
    /*val txt = env.readTextFile("/Users/yinqi/test/test.txt")
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
    val csvStream=env.readFile(new CsvInputFormat[String] (new Path("/Users/yinqi/Desktop/chris/flink-doc/data_example.csv")){
      override def fillRecord(out: String, objects: Array[AnyRef]): String = ???
    },"/Users/yinqi/Desktop/chris/flink-doc/data_example.csv")
    val counts =csvStream.flatMap(_.toLowerCase.split(",").filter(_.nonEmpty).map((_,1))).keyBy(0).sum(1)
    counts.print()

    env.execute()
  }

  class Person(var name: String, var age: Int) {
    //默认空构造器
    def this() {
      this(null, -1)
    }

//    toString(){
//       age;
//    }

  }
}
