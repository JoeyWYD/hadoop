package com.shujia

object DemoTuple {
  def main(args: Array[String]): Unit = {
    //元组的定义,可以存放不同类型的元素
    val t1 = (1,"a",2.01,3);

    //元组值的获取
    println(t1._2);
    println(t1._3);

    //toString操作,将列表、元组、数组等等的元素转为字符串
    //可以通过.getClass.getName 查看一个对象的类型
    println(t1.toString());

    //通过productIterator方法生成迭代器遍历元组；
    t1.productIterator.foreach(println);

  }

}
