package com.shujia

import scala.collection.mutable.ListBuffer


object DemoList {
  def main(args: Array[String]): Unit = {
    //定义列表的三种方式
    val list1 = List(1,2,3,4,5);
    val list2:List[String] = List("a","b","c");
    val list3 = "e"::("f"::("g"::Nil));
    println(list2+"\n"+list3);
    println("--------------------------------")
    //列表操作
    //取头，取第一个元素
    println(list2.head);
    //取尾，取第一个元素外的其他元素
    println(list2.tail);
    //判断列表是否为空，空为true
    println(list2.isEmpty);
    println("-------------------")

    //连接列表
    val cclist = List.concat(list2,list3);
    println(cclist);
    println("---------------------")

    //翻转列表
    val relist = list1.reverse;
    println(relist);
    println("---------------------")

    //map操作，返回值为新列表
    println(list1.map(x => x+1));
    println("---------------------")

    //foreach操作，没有返回值
    list1.foreach(println);
    println("---------------------")

    //filter操作，过滤符合条件的元素到新列表中
    list1.filter(x => x>2).foreach(println);
    println("---------------------")

    //take操作，取前X个元素至新列表
    list1.take(3).foreach(println);
    println("---------------------")

    //sortby操作，排序，返回一个新列表
    list1.sortBy(x => -x).take(3).foreach(println);
    println("==============================")

    /***
      *   ListBuffer
      *     可变列表
      */

    val listbuf = new ListBuffer[Int];
    listbuf.append(1);
    listbuf.append(2);
    println(listbuf);
    println("---------------------")
    //初始化操作
    val listbuf1 = ListBuffer("a","b","c");
    println(listbuf1);

  }

}
