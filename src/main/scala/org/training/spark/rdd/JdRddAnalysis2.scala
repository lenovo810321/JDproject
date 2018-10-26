package org.training.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by 张宝玉 on 2018/7/6.
  * 从不买打折产品且不借款的用户ID
  */
object JdRddAnalysis2 {
  def main(args: Array[String]): Unit = {
    val PATH = "hdfs://master:9000/user/hive/warehouse/jd2.db/"

    //创建接口
    val conf = new SparkConf().setMaster("local[*]").setAppName("JdAnalysis")
    val sc = new SparkContext(conf)

    //读取文件
    val orderRdd = sc.textFile(PATH + "t_order")
    val loanRdd = sc.textFile(PATH + "t_loan")

    //分析
    val orderUserID = orderRdd.map(s => s.split(",")).filter(s => s(0) != "uid")
      .map(s => (s(0), s(5).toDouble))
      .filter(_._2 == 0).map(_._1).distinct()
    val loanUserID = loanRdd.map(s => s.split(",")).filter(s => s(0) != "uid").map(_(0)).distinct()
    val result = orderUserID.subtract(loanUserID)
    println("count:" + result.count())

    sc.stop()
  }
}
