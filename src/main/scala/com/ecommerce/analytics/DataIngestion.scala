package com.ecommerce.analytics

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types._
import com.ecommerce.models.{Transaction, User, Product, Merchant}
import com.ecommerce.utils.ConfigLoader

/**
 * Q2.1 - Centralise la lecture des 4 fichiers de données.
 * Chaque méthode renvoie un Dataset[T] typé (pas juste un DataFrame),
 * ce qui permet d'avoir l'autocomplétion et la vérification des types
 * dès la compilation (ex: df("merchant_id") existe vraiment).
 */
object DataIngestion {

  def lireTransactions(spark: SparkSession): Dataset[Transaction] = {
    import spark.implicits._
    try {
      // Schéma explicite imposé par le sujet pour transactions.csv
      val schema = StructType(Seq(
        StructField("transaction_id", StringType, nullable = true),
        StructField("user_id", StringType, nullable = true),
        StructField("product_id", StringType, nullable = true),
        StructField("merchant_id", StringType, nullable = true),
        StructField("amount", DoubleType, nullable = true),
        StructField("timestamp", StringType, nullable = true),
        StructField("location", StringType, nullable = true),
        StructField("payment_method", StringType, nullable = true),
        StructField("category", StringType, nullable = true)
      ))

      val ds = spark.read
        .option("header", "true")
        .schema(schema)
        .csv(ConfigLoader.transactionsPath)
        .as[Transaction]

      println(s"[INGESTION] transactions.csv : ${ds.count()} lignes lues")
      ds
    } catch {
      case e: Exception =>
        println(s"[ERREUR INGESTION] Impossible de lire transactions.csv : ${e.getMessage}")
        throw e
    }
  }

  def lireUsers(spark: SparkSession): Dataset[User] = {
    import spark.implicits._
    try {
      // users.json : Spark gère tout seul le champ imbriqué preferred_categories (tableau)
      val ds = spark.read.json(ConfigLoader.usersPath).as[User]
      println(s"[INGESTION] users.json : ${ds.count()} lignes lues")
      ds
    } catch {
      case e: Exception =>
        println(s"[ERREUR INGESTION] Impossible de lire users.json : ${e.getMessage}")
        throw e
    }
  }

  def lireProducts(spark: SparkSession): Dataset[Product] = {
    import spark.implicits._
    try {
      // products.parquet : le schéma est déjà intégré au fichier, pas besoin de le redéfinir
      val ds = spark.read.parquet(ConfigLoader.productsPath).as[Product]
      println(s"[INGESTION] products.parquet : ${ds.count()} lignes lues")
      ds
    } catch {
      case e: Exception =>
        println(s"[ERREUR INGESTION] Impossible de lire products.parquet : ${e.getMessage}")
        throw e
    }
  }

  def lireMerchants(spark: SparkSession): Dataset[Merchant] = {
    import spark.implicits._
    try {
      // merchants.csv : on laisse Spark deviner le schéma (inferSchema)
      val ds = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(ConfigLoader.merchantsPath)
        .as[Merchant]
      println(s"[INGESTION] merchants.csv : ${ds.count()} lignes lues")
      ds
    } catch {
      case e: Exception =>
        println(s"[ERREUR INGESTION] Impossible de lire merchants.csv : ${e.getMessage}")
        throw e
    }
  }
}