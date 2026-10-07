package com.ecommerce.analytics

import org.apache.spark.sql.SparkSession

object TestQ53 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Test Q5.3 - Mesure du gain des optimisations")
      .getOrCreate()

    try {
      val transactions = DataIngestion.lireTransactions(spark)
      val (transValides, _) = DataValidation.validerTransactions(transactions)

      // on refait les memes operations 3 fois, SANS cache
      val debutSansCache = System.nanoTime()
      transValides.groupBy("category").count().collect()
      transValides.groupBy("payment_method").count().collect()
      transValides.groupBy("location").count().collect()
      val finSansCache = System.nanoTime()
      val dureeSansCache = (finSansCache - debutSansCache) / 1e9d
      println(s"[SANS CACHE] duree totale : $dureeSansCache secondes")

      // on cache le dataframe, puis on refait les memes operations
      SparkOptimizations.cacheAndMaterialize(transValides, "transValides")
      val debutAvecCache = System.nanoTime()
      transValides.groupBy("category").count().collect()
      transValides.groupBy("payment_method").count().collect()
      transValides.groupBy("location").count().collect()
      val finAvecCache = System.nanoTime()
      val dureeAvecCache = (finAvecCache - debutAvecCache) / 1e9d
      println(s"[AVEC CACHE] duree totale : $dureeAvecCache secondes")

      val gain = ((dureeSansCache - dureeAvecCache) / dureeSansCache) * 100
      println(f"[GAIN] le cache a permis un gain de $gain%.2f %% de temps")

      SparkOptimizations.releaseCache(transValides, "transValides")

    } finally {
      spark.stop()
    }
  }
}