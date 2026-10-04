package com.ecommerce.analytics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.storage.StorageLevel

/**
 * Q5.1 et Q5.2 - Optimisations Spark pour le module Analytics (Membre C).
 */
object SparkOptimizations {

  /**
   * Q5.1 - Met un DataFrame en cache et déclenche immédiatement
   * une action pour que la mise en cache soit réellement effectuée.
   */
  def cacheAndMaterialize(df: DataFrame, label: String): DataFrame = {
    df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val nbLignes = df.count()
    println(s"[CACHE] '$label' mis en cache : $nbLignes lignes")
    df
  }

  /**
   * Q5.1 - Libère la mémoire/disque occupée par un DataFrame en cache.
   */
  def releaseCache(df: DataFrame, label: String): Unit = {
    df.unpersist()
    println(s"[CACHE] '$label' libéré")
  }

  /**
   * Q5.2 - Force Spark à diffuser (broadcast) une petite table plutôt
   * que de la redistribuer par shuffle lors d'une jointure.
   * A utiliser pour joindre transactions (gros) avec merchants/products (petits).
   */
  def joinAvecBroadcast(grandDf: DataFrame, petitDf: DataFrame,
                        colonneJointure: String): DataFrame = {
    grandDf.join(broadcast(petitDf), colonneJointure)
  }

  /**
   * Q5.2 - Ajuste le nombre de partitions utilisées lors d'un shuffle
   * (groupBy, join, orderBy...). Par défaut Spark en met 200, ce qui
   * est trop pour notre volume de données (138k lignes).
   */
  def configurerShufflePartitions(spark: SparkSession, nombre: Int): Unit = {
    spark.conf.set("spark.sql.shuffle.partitions", nombre)
    println(s"[CONFIG] spark.sql.shuffle.partitions = $nombre")
  }
}