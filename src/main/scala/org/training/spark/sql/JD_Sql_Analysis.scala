package org.training.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by 张宝玉 on 2018/7/6.
  */
object JD_Sql_Analysis {
  def main(args: Array[String]): Unit = {
    var dataPath = args(0)
    var masterUrl = args(1)
    val spark = SparkSession.builder().appName("JD_Sql_Analysis").master(masterUrl).getOrCreate()

    val loanDF = spark.read.option("header","true").option("sep",",").csv(dataPath + "t_loan_sum.csv")
    val orderDF = spark.read.option("header","true").option("sep",",").csv(dataPath + "t_order.csv")

    loanDF.createOrReplaceTempView("loan")
    orderDF.createOrReplaceTempView("order")

    // 借款金额超过2且购买商品总价值超过借款总金额的用户ID
    println("借款金额超过2000且购买商品总价值超过借款总金额的用户ID")
    val sql_Analysis_1 = spark.sql("select o.uid " +
      "from " +
      "(select uid, sum(price * qty - discount) os from order group by uid ) as o, " +
      "(select uid, sum(loan_sum) ls from loan group by uid having ls > 2) as l " +
      "where o.uid = l.uid and o.os > l.ls")
    sql_Analysis_1.collect().foreach(println(_))
    println("结果条数：" + sql_Analysis_1.count())
    // 从不买打折产品且不借款的用户ID
    println("从不买打折产品且不借款的用户ID")
    val sql_Analysis_2 = spark.sql("select uid from " +
      "(select uid, sum(discount) os from order group by uid having os = 0) as o " +
      "where (select count(1) as num from loan where loan.uid = o.uid) = 0")
    sql_Analysis_2.collect().foreach(println(_))
    println("结果条数：" + sql_Analysis_2.count())

    spark.stop()
  }
}
