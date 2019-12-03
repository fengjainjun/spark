package com.info.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkOperator10 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkOperator10")
    val sc = new SparkContext(conf)

    //需求：创建一个pairRDD，取出每个分区相同key对应值的最大值，然后相加
    val listRdd: RDD[(String, Int)] = sc.makeRDD(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)
    listRdd.glom().collect().foreach(println)
    val aggregateByKeyRDD: RDD[(String, Int)] = listRdd.aggregateByKey(0)(math.max(_,_),_+_)
    aggregateByKeyRDD.foreach(println)

  }
}