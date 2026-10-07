package com.ecommerce.analytics

import org.apache.spark.sql.SparkSession

object TestQ44 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Test Q4.4 - Analyse produits et categories (donnees reelles)")
      .getOrCreate()

    try {
      val transactions = DataIngestion.lireTransactions(spark)
      val (transValides, _) = DataValidation.validerTransactions(transactions)

      val products = DataIngestion.lireProducts(spark)
      val (productsValides, _) = DataValidation.validerProducts(products)

      val merchants = DataIngestion.lireMerchants(spark)
      val (merchantsValides, _) = DataValidation.validerMerchants(merchants)

      val top10 = ProductAnalytics.top10Produits(transValides, productsValides)
      println("=== TOP 10 PRODUITS (donnees reelles) ===")
      top10.show(false)

      val caCatRegion = ProductAnalytics.caCategorieRegion(transValides, merchantsValides)
      println("=== CA PAR CATEGORIE ET REGION ===")
      caCatRegion.show(false)

      val caPaiementPeriode = ProductAnalytics.caPaiementPeriode(transValides)
      println("=== CA PAR MODE DE PAIEMENT ET PERIODE ===")
      caPaiementPeriode.show(false)

    } finally {
      spark.stop()
    }
  }
}