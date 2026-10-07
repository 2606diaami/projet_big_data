package com.ecommerce.analytics

import org.apache.spark.sql.SparkSession

object TestQ21 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Test Q2.1 - Ingestion")
      .getOrCreate()

    try {
      val transactions = DataIngestion.lireTransactions(spark)
      transactions.show(5)
      transactions.printSchema()

      val users = DataIngestion.lireUsers(spark)
      users.show(5, truncate = false)

      val products = DataIngestion.lireProducts(spark)
      products.show(5)

      val merchants = DataIngestion.lireMerchants(spark)
      merchants.show(5)

    } finally {
      spark.stop()
    }
  }
}