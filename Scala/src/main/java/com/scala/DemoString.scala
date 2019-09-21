package com.shujia

object DemoString {
  def main(args: Array[String]): Unit = {
    val str = "abc,efg";

    val str2 = s"$str,hij";

    println(str2)


  }
}
