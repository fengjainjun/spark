package com.info.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkOperator11 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkOperator10")
    val sc = new SparkContext(conf)

    //需求：2.创建一个pairRDD，计算相同key对应值的相加结果
    val listRdd: RDD[(String, Int)] = sc.makeRDD(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)
    val foldByKeyRDD: RDD[(String, Int)] = listRdd.foldByKey(0)(_+_)
    foldByKeyRDD.foreach(println)

  }
}