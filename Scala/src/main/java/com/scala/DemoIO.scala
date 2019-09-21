package com.shujia

import java.io.PrintWriter
import java.util.Scanner

import scala.io.Source

object DemoIO {
  def main(args: Array[String]): Unit = {

    //从控制台录入一行数据
//    val line = Console.readLine("Please enter: ");
//    println(line);

//    //文件的写入
//    val file = new PrintWriter("scala/src/data/1.txt");
//    file.write("abc")
//    file.write("\n")
//    file.write("efg")
//    file.close()
//
//    //文件的读取
//    val lines = Source
//      .fromFile("scala/src/data/students.txt")
//      .getLines()
//
//    val file1 = new PrintWriter("scala/src/data/students_copy.txt")
//    lines.foreach(line => {
//      file1.write(line+"\n")
//    })
//    file1.close()

    //读取students.txt文件，并将数据保存至Map中，以id为key
    val studentMap = Source
      .fromFile("scala/src/data/students.txt")
      .getLines()
      .map(line => {
        val list = line.split(",").toList
        val key = list(0);
        val value = s"${list(1)},${list(2)},${list(3)},${list(4)}"

        (key,value)
      }).toMap


    //提取文科学生信息
    studentMap.filter(line => {
      val clazz = line._2.split(",")(3)

      clazz.startsWith("文科")
    }).foreach(line => {
      println(line._1+","+line._2)
    })


  }

}
