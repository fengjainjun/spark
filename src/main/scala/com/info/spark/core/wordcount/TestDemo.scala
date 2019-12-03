package com.info.spark.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestDemo {
  def main(args: Array[String]): Unit = {
    //编写spark的Wordcount的案例

    //0.实例化sparkcontext时需要传一个config对象，配置就在config中
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("WordCount")
    //1.拿到spark的sparkcontext对象
    val sc: SparkContext = new SparkContext(conf)

    //2.读取数据源,它的格式是一行一行的
    val lines: RDD[String] = sc.textFile("in/hello.log")

    //3.对读取的数据进行扁平化操作,相当于将一行的数据进行切割
    var str1 : String = ""
    var str2 : String = ""
    val mapRDD: RDD[(String, Int)] = lines.map {
      line =>
        val filed: Array[String] = line.split(",")
        val times: Array[String] = filed(1).split("T")
        val month: Array[String] = times(1).split(":")
        val str: String = month(0)
        str1 = filed(1)
        str2 = times(1)
        (month(0), 1)
    }
    println(str1)
    println(str2)
    val reduceByKeyRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
    val finaly: RDD[(String, String, String, Int)] = reduceByKeyRDD.map {
      line =>(str1, str2, line._1, line._2)
    }
    finaly.collect().foreach(print)

    //192.168.43.1 2019-11-26T18:36:52+08:00

//    //4.改变数据结构，也就是形成一个touple2的元组
//    val word: RDD[(String, Int)] = words.map{
//
//    }
//    //5.对touple2进行聚合
//    val arr: RDD[(String, Int)] = word.reduceByKey(_+_)
//
//    //6.遍历这个arr将结构打印在控制台上
//    arr.foreach(println)


    /*val mapRDD: RDD[((String, String), (String, Int))] = lines.map {
      line =>
        val filed: Array[String] = line.split(",")
        val times: Array[String] = filed(1).split("T")
        val month: Array[String] = times(1).split(":")
        val str: String = month(0)
        (((filed(0), times(0)), (str, 1)))

    }
    mapRDD.mapValues()
    mapRDD*/
  }
}
