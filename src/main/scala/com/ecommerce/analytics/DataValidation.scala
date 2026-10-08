package com.ecommerce.analytics

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import com.ecommerce.models.{Transaction, User, Product, Merchant}
import com.ecommerce.utils.ConfigLoader

object DataValidation {

  // Règle : amount > 0 ET timestamp doit avoir 14 caractères
  def validerTransactions(ds: Dataset[Transaction]): (DataFrame, DataFrame) = {
    val df = ds.toDF()

    val dfAvecRaisons = df.withColumn("rejection_reason",
      concat_ws(" | ",
        when(col("amount").isNull || col("amount") <= ConfigLoader.transactionMinAmount, lit("amount_null_ou_<=0")),
        when(col("timestamp").isNull || length(col("timestamp")) =!= 14, lit("timestamp_null_ou_invalide"))
      )
    )

    val valides = dfAvecRaisons.filter(col("rejection_reason") === "").drop("rejection_reason")
    val rejetees = dfAvecRaisons.filter(col("rejection_reason") =!= "")

    println(s"[VALIDATION] transactions : ${valides.count()} lignes valides, ${rejetees.count()} lignes rejetées")
    (valides, rejetees)
  }

  // Règle : age entre 16 et 100 ET annual_income > 0
  def validerUsers(ds: Dataset[User]): (DataFrame, DataFrame) = {
    val df = ds.toDF()

    val dfAvecRaisons = df.withColumn("rejection_reason",
      concat_ws(" | ",
        when(col("age") < ConfigLoader.userMinAge || col("age") > ConfigLoader.userMaxAge, lit("age_hors_intervalle")),
        when(col("annual_income") <= 0, lit("income <= 0"))
      )
    )

    val valides = dfAvecRaisons.filter(col("rejection_reason") === "").drop("rejection_reason")
    val rejetees = dfAvecRaisons.filter(col("rejection_reason") =!= "")

    println(s"[VALIDATION] users : ${valides.count()} lignes valides, ${rejetees.count()} lignes rejetées")
    (valides, rejetees)
  }

  // Règle : price > 0 ET rating entre 1 et 5
  def validerProducts(ds: Dataset[Product]): (DataFrame, DataFrame) = {
    val df = ds.toDF()

    val dfAvecRaisons = df.withColumn("rejection_reason",
      concat_ws(" | ",
        when(col("price") <= 0, lit("price <= 0")),
        when(col("rating") < ConfigLoader.productMinRating || col("rating") > ConfigLoader.productMaxRating, lit("rating_hors_intervalle"))
      )
    )

    val valides = dfAvecRaisons.filter(col("rejection_reason") === "").drop("rejection_reason")
    val rejetees = dfAvecRaisons.filter(col("rejection_reason") =!= "")

    println(s"[VALIDATION] products : ${valides.count()} lignes valides, ${rejetees.count()} lignes rejetées")
    (valides, rejetees)
  }

  // Règle : commission_rate entre 0 et 1
  def validerMerchants(ds: Dataset[Merchant]): (DataFrame, DataFrame) = {
    val df = ds.toDF()

    val dfAvecRaisons = df.withColumn("rejection_reason",
      concat_ws(" | ",
        when(col("commission_rate") < ConfigLoader.merchantMinCommission || col("commission_rate") > ConfigLoader.merchantMaxCommission, lit("commission_hors_intervalle"))
      )
    )

    val valides = dfAvecRaisons.filter(col("rejection_reason") === "").drop("rejection_reason")
    val rejetees = dfAvecRaisons.filter(col("rejection_reason") =!= "")

    println(s"[VALIDATION] merchants : ${valides.count()} lignes valides, ${rejetees.count()} lignes rejetées")
    (valides, rejetees)
  }
}