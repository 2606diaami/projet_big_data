package com.ecommerce.analytics

import org.apache.spark.sql.SparkSession

object TestQ42 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Test Q4.2 - Cohortes")
      .getOrCreate()

    val transactionsEnrichies = TestDataGenerator.genererTransactionsEnrichies(spark)

    val cohortes = Analytics.analyseCohortes(transactionsEnrichies)
    println("=== Matrice de rétention par cohorte ===")
    cohortes.show(false)

    val meilleure = Analytics.meilleureCohorte3m(cohortes)
    println("=== Meilleure cohorte à 3 mois ===")
    meilleure.show(false)

    spark.stop()
  }
}