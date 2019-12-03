package com.info.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Example01 {

  def main(args: Array[String]): Unit = {
    /*
    1.需求：统计出每一个省份广告被点击次数的TOP3
    2.数据结构：时间戳，省份，城市，用户，广告，中间字段使用空格分割。
      1516609143867 6 7 64 16
      1516609143869 9 4 75 18
      1516609143869 1 7 87 12
    * */
    /*
    分析：1.使用textFile读取文件，形成RDD
          2.对数据进行切割
          3.取出第二位和最后一次的数据作为key，加上数字，形成一个touple：（（城市，广告），数量）
          4.利用reduceByKey的算子进行聚合操作
          5.转换结构，（城市，（广告，数量））
          6.根据城市进行分组
          7.取出top3
    * */
    //1.创建sc
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Example01")
    val sc = new SparkContext(conf)
    //2.读取文件
    val linesRDD: RDD[String] = sc.textFile("in")
    //3.对读取的每一行数据用空格切分
    val cityAndTVRDD: RDD[((String, String), Int)] = linesRDD.map {
      x =>
        val words: Array[String] = x.split(" ")
        ((words(1), words(4)), 1)
    }
    //4.用reduceByKey的算子进行聚合操作
    val reduceByKeyRDD: RDD[((String, String), Int)] = cityAndTVRDD.reduceByKey(_+_)

    //5.换数据结构。以城市为key，广告和点击数量做value，形成touple
    val mapRDD: RDD[(String, (String, Int))] = reduceByKeyRDD.map {
      x =>
        (x._1._1, (x._1._2, x._2))
    }
    //6.根key进行分组
    val groupByKeyRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()

    //7.取出前三条数据
    val top3: RDD[(String, List[(String, Int)])] = groupByKeyRDD.mapValues {
      x =>
        x.toList.sortWith(
          (x, y) => x._2 > y._2
        ).take(3)
    }
    //打印
    top3.collect().foreach(println)

    //关闭与spark的连接
    sc.stop()

  }
}
