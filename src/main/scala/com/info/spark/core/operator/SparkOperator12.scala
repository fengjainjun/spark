package com.info.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkOperator12 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkOperator10")
    val sc = new SparkContext(conf)

    //需求：创建一个pairRDD，根据key计算每种key的均值。
    val listRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2)
    val combineByKeyRDD: RDD[(String, (Int, Int))] = listRDD.combineByKey(
      (_, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )
    val value: RDD[(String, Double)] = combineByKeyRDD.map {
      case (key, value) => (key, value._1 / value._2.toDouble)
    }
    value.foreach(println)

  }
}