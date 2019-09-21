package com.spark

import org.apache.spark.{SparkConf, SparkContext}

object DemoAvg{
  def main(args: Array[String]): Unit = {
    //创建spark配置文件
    val conf = new SparkConf().setAppName("StuAvg").setMaster("local[2]")
    //创建spark上下文
    val sc = new SparkContext(conf)
    //读取文件
    val stuRDD = sc.textFile("Spark/src/data/students.txt")
    val scoreRDD = sc.textFile("Spark/src/data/score.txt")

    //将两个待join的RDD转为kv格式的RDD
    val stukvRDD = stuRDD.map(line => {
      val strs = line.split(",")
      val key = strs(0)
      val value = s"${strs(1)},${strs(4)}"

      (key, value)
    })

    val scorekvRDD = scoreRDD.map(line => {
      val strs = line.split(",")
      val key = strs(0)
      val value = strs(2)

      (key, value)
    })
    //进行join
    val joindata = stukvRDD.join(scorekvRDD)

    joindata.map(line => {
      val key = line._1 + "," + line._2._1
      val value = line._2._2
      (key, value)
    })
      .groupByKey() //分组
      .map(line => {
      var sum_score = 0;
      line._2.toList.foreach(x => {
        sum_score += x.toInt
      })
      val avg_score = sum_score / line._2.toList.length
      (line._1, avg_score)
    })
      .sortBy(-_._2)
      .foreach(println)

  }
}