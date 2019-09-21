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

    println("---------------------------------")
    /**
      *   匿名函数
      */

    val ano = (x:Int, y:Int) => {
      val sum = x+y;
      sum/2;
    };
    println(ano(7,3));

    def fun4(f:Int => Int): Unit ={
      val num = f(4);
      println(num)
    }

    def fun5(x:Int): Int={
      val sum = x + 5;
      sum/3;
    }
    fun4(fun5);

    fun4(x => {
      val sum = x + 5;
      sum / 3;
    })

    println("------------------------")

    val list = List(1,2,3,4);


    list
      .map(num => num+1)
      .map(num => num-2)
      .foreach(println);


  }
}
