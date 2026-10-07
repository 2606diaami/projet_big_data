package com.ecommerce.analytics

import org.apache.spark.sql.SparkSession
import com.ecommerce.utils.DataFrameWriterUtils

// test pour verifier que le rapport de qualite (Q2.4) fonctionne bien
// avec les 4 vrais fichiers de donnees
object TestQ24 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Test Q2.4 - Rapport de qualite")
      .getOrCreate()

    try {
      // transactions
      val transactions = DataIngestion.lireTransactions(spark)
      val (transValides, transRejetees) = DataValidation.validerTransactions(transactions)
      val ligneTrans = DataQualityReport.construireLigneQualite(
        "transactions", transactions.toDF(), transValides, transRejetees)

      // users
      val users = DataIngestion.lireUsers(spark)
      val (usersValides, usersRejetees) = DataValidation.validerUsers(users)
      val ligneUsers = DataQualityReport.construireLigneQualite(
        "users", users.toDF(), usersValides, usersRejetees)

      // products
      val products = DataIngestion.lireProducts(spark)
      val (productsValides, productsRejetees) = DataValidation.validerProducts(products)
      val ligneProducts = DataQualityReport.construireLigneQualite(
        "products", products.toDF(), productsValides, productsRejetees)

      // merchants
      val merchants = DataIngestion.lireMerchants(spark)
      val (merchantsValides, merchantsRejetees) = DataValidation.validerMerchants(merchants)
      val ligneMerchants = DataQualityReport.construireLigneQualite(
        "merchants", merchants.toDF(), merchantsValides, merchantsRejetees)

      // on assemble tout dans un seul DataFrame
      val rapport = DataQualityReport.genererRapport(
        spark, Seq(ligneTrans, ligneUsers, ligneProducts, ligneMerchants))

      println("=== RAPPORT DE QUALITE DES DONNEES ===")
      rapport.show(false)

      DataFrameWriterUtils.ecrireRapportQualiteUnique(rapport)

    } finally {
      spark.stop()
    }
  }
}