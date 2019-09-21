package com.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(arr:Array[String]):Unit ={
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("WordCount")

    val sc=new SparkContext(conf)

    val linesRDD=sc.textFile(path="Spark/src/data/word.txt")

    val wordRDD=linesRDD.map(line => line.split(" ")(0))


    val kvRDD = wordRDD.map(word=>(word , 1))

    val countRDD=kvRDD
      .groupByKey()
      .map(kv=>{
        val word = kv._1
        val count= kv._2.sum

        word + "\t" + count
      })

    countRDD.foreach(println)
  }
}
