package com.ecommerce

import com.ecommerce.analytics.SparkOptimizations
import org.apache.spark.sql.SparkSession

/**
 * Programme de test autonome pour valider Q5.1 (cache/persist/unpersist)
 * avec un petit jeu de données simulé.
 */
object TestQ51 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Test Q5.1 - Cache et Persist")
      .getOrCreate()

    import spark.implicits._

    // Jeu de données simulé : représente ce que sera transactionsEnrichies
    val transactionsSimulees = Seq(
      ("TX0001", "U001", 221.80, "Paris", "Electronics"),
      ("TX0002", "U002", 1331.20, "Paris", "Electronics"),
      ("TX0003", "U003", 907.60, "Strasbourg", "Home & Garden"),
      ("TX0004", "U004", 761.61, "Paris", "Home & Garden")
    ).toDF("transaction_id", "user_id", "amount", "location", "category")

    println("=== Avant mise en cache ===")
    transactionsSimulees.explain()

    // Q5.1 : mise en cache réelle (persist + action count())
    val transactionsEnCache = SparkOptimizations.cacheAndMaterialize(
      transactionsSimulees, "transactionsSimulees"
    )

    println("=== Utilisation 1 : nombre de transactions par ville ===")
    transactionsEnCache.groupBy("location").count().show()

    println("=== Utilisation 2 : montant moyen par catégorie ===")
    transactionsEnCache.groupBy("category").avg("amount").show()

    // Q5.1 : libération du cache
    SparkOptimizations.releaseCache(transactionsEnCache, "transactionsSimulees")

    spark.stop()
  }
}
