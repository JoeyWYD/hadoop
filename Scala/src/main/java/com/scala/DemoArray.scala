package com.shujia

object DemoArray {
  def main(args: Array[String]): Unit = {

    //定义一个数组
    val arr1 = Array("a,b,c","e,f","g,h");
    val arr2:Array[Int] = Array(1,2,3,4,5);

    val arr3:Array[String] = new Array[String](3);
    arr3(0) = "x";
    arr3(1) = "y";
    arr3(2) = "z";
    arr3.foreach(println);
    println("---------------------------")

    //求和
    println(arr2.sum);
    println("---------------------------")



    //flatmap操作，进去一行数据返回多行数据，返回值是一个数组
    val flatlist = arr1.flatMap(line => {
      val arrs = line.split(",");
      arrs
    }).toList;
    println(flatlist)


  }

}
