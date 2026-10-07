package com.ecommerce.analytics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{to_timestamp, col}

object TestQ43 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Test Q4.3 - Segmentation RFM (donnees reelles)")
      .getOrCreate()

    try {
      // on utilise les vraies donnees (deja validees avec la Partie A), pas les donnees simulees
      val transactions = DataIngestion.lireTransactions(spark)
      val (transValides, _) = DataValidation.validerTransactions(transactions)

      val users = DataIngestion.lireUsers(spark)
      val (usersValides, _) = DataValidation.validerUsers(users)

      // pour le RFM on a juste besoin de : user_id, customer_segment, transaction_date, amount
      // pas besoin d'attendre toute la Partie 3, une simple jointure suffit ici
      val transactionsPourRFM = transValides
        .join(usersValides.select("user_id", "customer_segment"), Seq("user_id"))
        .withColumn("transaction_date", to_timestamp(col("timestamp"), "yyyyMMddHHmmss"))
        .select("user_id", "customer_segment", "transaction_date", "amount")

      val rfm = RFMSegmentation.genererRFM(transactionsPourRFM)
      println("=== RFM PAR CLIENT (donnees reelles) ===")
      rfm.show(20, false)

      val tableauCroise = RFMSegmentation.croiserAvecCustomerSegment(rfm)
      println("=== TABLEAU CROISE segment_rfm x customer_segment ===")
      tableauCroise.show(false)

    } finally {
      spark.stop()
    }
  }
}