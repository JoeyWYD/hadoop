package com.shujia

object DemoObject {
  def main(args: Array[String]): Unit = {
    val cat1 = new Cat("xiaobai", "white");
    cat1.eat();
    val cat2 = new Cat1("xiaohei","black");
    cat2.eat();
    println("-------------------");


    val dog = new Dog("dahuang","yellow","60cm");
    dog.eat();
    dog.sleep();
    dog.bark();

    println("-------------------");

    /**
      * 样例类：
      * 1.不需要重写父类的方法
      * 2.创建时不要用new创建对象
      */
    val stu = Student("xiaoming","male");
    println(stu.name);



  }

}
//第一种创建类的方法
class Cat(var name:String, var color:String){

  def eat(): Unit ={
    println(s"${this.name} is eating");
  }
}

//第二种创建类的方法
class Cat1(name:String, color:String){
  val n = name;
  val c = color;

  def eat(): Unit ={
    println(s"${this.n} is eating");
  }
}

class Animal(var name:String, var color:String){

  def eat(): Unit ={
    println(s"${this.name} is eating");
  }

  def sleep(): Unit ={
    println(s"${this.name} is sleeping");
  }
}

class Dog(name: String, color:String, var hight:String) extends Animal(name, color){
  override def sleep(): Unit = {
    println(s"${this.name} is sleeping well!");
  }

  def bark(): Unit ={
    println(s"${this.name} is barking");
  }
}

case class Student(name: String, gender:String);