package com.info.spark.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //编写spark的Wordcount的案例

    //0.实例化sparkcontext时需要传一个config对象，配置就在config中
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("WordCount")
    //1.拿到spark的sparkcontext对象
    val sc: SparkContext = new SparkContext(conf)

    //2.读取数据源,它的格式是一行一行的
    val lines: RDD[String] = sc.textFile("D:\\projects\\sparkone\\src\\input")

    //3.对读取的数据进行扁平化操作,相当于将一行的数据进行切割
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //4.改变数据结构，也就是形成一个touple2的元组
    val word: RDD[(String, Int)] = words.map((_,1))
    //5.对touple2进行聚合
    val arr: RDD[(String, Int)] = word.reduceByKey(_+_)

    //6.遍历这个arr将结构打印在控制台上
    arr.foreach(println)

  }
}
