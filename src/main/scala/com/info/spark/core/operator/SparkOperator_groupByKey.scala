package com.info.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkOperator_groupByKey {
  def main(args: Array[String]): Unit = {
    //获取sparkkconf
    val conf: SparkConf = new SparkConf().setAppName("SparkOperator").setMaster("local[*]")
    //获取sparkcontext
    val sc = new SparkContext(conf)
    //创建一个rdd
   val arrayRDD: RDD[String] = sc.makeRDD(Array("one", "two", "two", "three", "three", "three"))
    //转化数据结构，形成K-V键值对
    val mapRDD: RDD[(String, Int)] = arrayRDD.map((_,1))
    //根据K进行分组
    val groupByKeyRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
    //对V进行聚合
    val sumRDD: RDD[(String, Int)] = groupByKeyRDD.map {
      x =>
        (x._1, (x._2.sum))
    }
    sumRDD.collect().foreach(println)

    groupByKeyRDD.collect().foreach(println)
  }
}
