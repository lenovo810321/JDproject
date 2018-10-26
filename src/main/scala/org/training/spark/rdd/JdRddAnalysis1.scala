package org.training.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by 张宝玉 on 2018/7/6.
  * 借款金额超过0.2且购买商品总价值超过借款总金额的用户ID
  */
class JdRddAnalysis1 {
  def main(args: Array[String]): Unit = {
    val PATH = "hdfs://master:9000/user/hive/warehouse/jd2.db/"

    //创建接口
    val conf = new SparkConf().setMaster("local[*]").setAppName("JdAnalysis")
    val sc = new SparkContext(conf)

    //读取文件
    val orderRdd = sc.textFile(PATH + "t_order")
    val loanRdd = sc.textFile(PATH + "t_loan")

    //分析
    // 在loanRdd中得到loan_amount的和并且大于0.2 [(uid, loan_amount_sum)]
    val loan = loanRdd.map(s => s.split(",")).filter(s => s(0) != "uid").map(s => (s(0), s(2).toDouble)).reduceByKey(_+_).filter(_._2 > 0.2)

    // 在orderRdd中得到 [(uid, price * qty - discount)]
    val order = orderRdd.map(s => s.split(",")).filter(s => s(0) != "uid").map(s => {
      val price = if (s(2).isEmpty) "0" else s(2)
      (s(0), price.toDouble * s(3).toInt - s(5).toDouble)
    }).reduceByKey(_+_)

    //两表join 过滤购买商品总价值超过借款总金额
    var result1 = loan.join(order).filter(s => s._2._2 > s._2._1).map(_._1)
    println("count:" + result1.count())

    sc.stop()
  }

}
