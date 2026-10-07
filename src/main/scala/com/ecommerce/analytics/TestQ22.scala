package com.ecommerce.analytics

import org.apache.spark.sql.SparkSession

object TestQ22 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Test Q2.2 - Validation")
      .getOrCreate()

    try {
      val transactions = DataIngestion.lireTransactions(spark)
      val (transValides, transRejetees) = DataValidation.validerTransactions(transactions)
      transRejetees.show(10, truncate = false)

      val users = DataIngestion.lireUsers(spark)
      val (usersValides, usersRejetees) = DataValidation.validerUsers(users)
      usersRejetees.show(10, truncate = false)

      val products = DataIngestion.lireProducts(spark)
      val (productsValides, productsRejetees) = DataValidation.validerProducts(products)
      productsRejetees.show(10, truncate = false)

      val merchants = DataIngestion.lireMerchants(spark)
      val (merchantsValides, merchantsRejetees) = DataValidation.validerMerchants(merchants)
      merchantsRejetees.show(10, truncate = false)

    } finally {
      spark.stop()
    }
  }
}