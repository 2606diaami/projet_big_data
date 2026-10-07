package com.ecommerce.analytics

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object ProductAnalytics {

  // top 10 des produits qui rapportent le plus de chiffre d'affaires
  def top10Produits(transactions: DataFrame, products: DataFrame): DataFrame = {
    transactions.groupBy("product_id")
      .agg(
        sum("amount").alias("ca_total"),
        count("*").alias("nb_ventes")
      )
      .join(products.select("product_id", "name", "category"), Seq("product_id"))
      .orderBy(col("ca_total").desc)
      .limit(10)
      .select("product_id", "name", "category", "nb_ventes", "ca_total")
  }

  // chiffre d'affaires regroupe par categorie et par region (region vient du marchand)
  def caCategorieRegion(transactions: DataFrame, merchants: DataFrame): DataFrame = {
    transactions.join(merchants.select("merchant_id", "region"), Seq("merchant_id"))
      .groupBy("category", "region")
      .agg(sum("amount").alias("ca_total"))
      .orderBy(col("ca_total").desc)
  }

  // chiffre d'affaires regroupe par mode de paiement et par mois
  def caPaiementPeriode(transactions: DataFrame): DataFrame = {
    transactions
      .withColumn("date_transaction", to_timestamp(col("timestamp"), "yyyyMMddHHmmss"))
      .withColumn("periode", date_format(col("date_transaction"), "yyyy-MM"))
      .groupBy("payment_method", "periode")
      .agg(sum("amount").alias("ca_total"))
      .orderBy("periode", "payment_method")
  }
}