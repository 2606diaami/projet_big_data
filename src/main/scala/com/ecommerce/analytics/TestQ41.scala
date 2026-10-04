package com.ecommerce.analytics

import org.apache.spark.sql.SparkSession

object TestQ41 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Test Q4.1 - KPI Marchands")
      .getOrCreate()

    val transactionsEnrichies = TestDataGenerator.genererTransactionsEnrichies(spark)

    val kpi = Analytics.kpiMarchands(transactionsEnrichies)

    println("=== KPI par marchand ===")
    kpi.show(false)

    println("=== Schéma du résultat ===")
    kpi.printSchema()

    spark.stop()
  }
}