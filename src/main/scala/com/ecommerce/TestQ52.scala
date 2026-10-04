package com.ecommerce

import com.ecommerce.analytics.SparkOptimizations
import org.apache.spark.sql.SparkSession

object TestQ52 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Test Q5.2 - Broadcast et Shuffle Partitions")
      .getOrCreate()

    import spark.implicits._

    // Q5.2 : on réduit le nombre de partitions de shuffle (au lieu de 200 par défaut)
    SparkOptimizations.configurerShufflePartitions(spark, 8)

    // Simule transactions (grande table)
    val transactions = Seq(
      ("TX0001", "M001", 221.80),
      ("TX0002", "M002", 1331.20),
      ("TX0003", "M001", 907.60),
      ("TX0004", "M003", 761.61)
    ).toDF("transaction_id", "merchant_id", "amount")

    // Simule merchants (petite table -> candidate au broadcast)
    val merchants = Seq(
      ("M001", "GardenShop 1", "Ile-de-France"),
      ("M002", "ElecStore 2", "Grand Est"),
      ("M003", "BeautyPlus 4", "Auvergne-Rhône-Alpes")
    ).toDF("merchant_id", "merchant_name", "region")

    // Q5.2 : jointure avec broadcast
    val resultat = SparkOptimizations.joinAvecBroadcast(transactions, merchants, "merchant_id")

    println("=== Résultat de la jointure avec broadcast ===")
    resultat.show()

    println("=== Plan d'exécution (vérifie qu'on voit 'BroadcastHashJoin') ===")
    resultat.explain()

    spark.stop()
  }
}