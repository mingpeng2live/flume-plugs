package demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by johnliu on 16/11/15.
  */
object WordCount {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile("/user/dmp/gaga")
    val resultRdd = rdd.filter( !_.contains("woca")).flatMap(_.split(" ",-1)).
      map(key => (key, 1)).reduceByKey(_ + _).map(entry => (entry._2, entry._1)).
      sortByKey().saveAsTextFile("/user/dmp/gaga-result")
  }
}
