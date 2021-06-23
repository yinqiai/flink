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

    /*val a="yinqi".map((_,1))

    println(a)
    println(Long.MaxValue)*/

    /*val a =Iterable[ChannelInfo]
    a.apply()*/

    val aggregateResult=List(8,new yinqi,5)
    val iterator=aggregateResult.iterator
    println(aggregateResult.head)
  while(iterator.hasNext==true)
{println(iterator)
    println(iterator.next())}
  }

  case class yinqi()
}
