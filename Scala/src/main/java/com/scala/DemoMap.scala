package com.shujia

import  scala.collection.mutable

object DemoMap {
  def main(args: Array[String]): Unit = {
    //Map 以键值对(key -> value)的形式保存数据
    //键(key)是唯一的，可以通过key获取value
    val m1 = Map(
      "1001" -> "xiaoming",
      "1002" -> "zhangsan",
      "1003" -> "lisi"
    );
    val m2:Map[String,String] = Map("1002" -> "zhangsan");

    //通过key获取值
    println(m1("1001"));
    println(m1.get("1001"));
    println(m1.get("1001").get);
    println(m1.getOrElse("1001",None));
    println("----------------------")

    //如果key不存在
    //println(m1("1004"));
    println(m1.get("1004"));
    //println(m1.get("1004").get);
    println(m1.getOrElse("1004", None));
    println("--------------------------")

    //mutable可变Map，可以对数据进行增删操作
    val muMap = mutable.Map("1001" -> "xiaoming");
    muMap += ("1002" -> "zhangsan");
    println(muMap.getOrElse("1002", None));
    println("--------------------------")


    //对值求和
    val m3 = Map(
      "zhangsan" -> "99",
      "lisi" -> "59",
      "xiaoming" -> "40",
      "wanger" -> "75"
    )
    var sum = 0;
    m3.foreach(line => {
      val score = line._2.toInt;
      sum += score;
    });
    println(sum);
    println("--------------------------")

    //将60分以下的分数变为60
    m3.map(line => {
      val key = line._1;
      var value = line._2.toInt;
      if(value < 60){
        value = 60;
      }
      (key,value.toString)
    }).foreach(line => {
      println(line._1+":"+line._2);
    })
    println("--------------------------")


    //打印keys或values
    m3.keys.foreach(println);
    m3.values.foreach(println);

  }
}
