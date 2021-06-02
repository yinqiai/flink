package com.transsnet.study.study20210223

/**
  * @author yinqi
  *
  */
object Test {
  def main(args: Array[String]): Unit = {
    //match和Option使用
   /* Option(None) match { case Some(c) => println(c)
    case None => println("None") }*/

    val a="yinqi".map((_,1))

    println(a)
    println(Long.MaxValue)
  }
}
