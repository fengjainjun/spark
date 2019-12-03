package com.info.spark.core.operator
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object SparkOperator {
  def main(args: Array[String]): Unit = {
    //获取sparkkconf
    val conf: SparkConf = new SparkConf().setAppName("SparkOperator").setMaster("local[*]")
    //获取sparkcontext
    val sc = new SparkContext(conf)
    //创建一个rdd
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)
    //转换数据结构。形成新的RDD
    val mapRDD: RDD[Int] = listRDD.map(_ * 2)
    //打印
    mapRDD.collect().foreach(println)
  }
}
