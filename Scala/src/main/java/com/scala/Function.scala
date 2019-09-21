package com.shujia

object Function {

  def main(args: Array[String]): Unit = {
    //函数的定义：
    // def [functionName](参数列表(参数名:数据类型)): 返回值类型={函数体}
    def fun1(n1:Int, n2:Int): Int={
      var sum = n1 + n2;
      sum
    }
    println(fun1(4, 6))

    println("---------------------------")

    /*
        高阶函数：
          以函数为参数的函数
          定义：(函数名: 作为参数的函数的参数的数据类型 => 作为参数的函数的返回值类型)


     */
    def fun2(f:String => Int, str:String): Unit ={
      println(str)
      val num = f("a,c,d");
      println(num);
    }

    def fun3(str:String): Int={
      val strs = str.split(",");
      return strs.length;
    }

    fun2(fun3,"1,2,3,4");





  }
}
