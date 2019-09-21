package com.shujia

object DemoSet {
  def main(args: Array[String]): Unit = {
    //set的定义，注意：set的元素是唯一的即不重复的
    //set的元素是无序的
    // 可以用来进行去重
    val s1:Set[Int] = Set(1,2,2,3,4);
    val s2 = Set("a","b","c","a");

    s1.foreach(println);
    s2.foreach(println);
    println("----------------------------")


    //可变集合
    import scala.collection.mutable.Set
    val muset = Set(1,2,3);
    muset.add(5);
    muset.add(3);
    muset.foreach(println);



  }

}
