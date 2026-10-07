package com.ecommerce.analytics

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.ecommerce.analytics.TimeFeaturesUDF.extractTimeFeatures

class DataTransformation {

  def enrichTransactionData(
                             transactions: DataFrame,
                             users: DataFrame,
                             products: DataFrame,
                             merchants: DataFrame
                           ): DataFrame = {

    // 1) Colonnes utiles, avec renommage des homonymes
    val usersSel = users.select("user_id", "age", "annual_income", "city", "customer_segment")

    val productsSel = products.select(
      col("product_id"),
      col("name").as("product_name"),
      col("price").as("product_price"),
      col("rating"),
      col("stock")
    )

    val merchantsSel = merchants.select(
      col("merchant_id"),
      col("name").as("merchant_name"),
      col("category").as("merchant_category"),
      col("region"),
      col("commission_rate")
    )

    // 2) Jointures "left" : on garde toutes les transactions
    val joined = transactions
      .join(usersSel, Seq("user_id"), "left")
      .join(productsSel, Seq("product_id"), "left")
      .join(merchantsSel, Seq("merchant_id"), "left")

    // 3) Caractéristiques temporelles (UDF), struct éclatée en colonnes simples
    val withTime = joined
      .withColumn("time_features", extractTimeFeatures(col("timestamp")))
      .select(col("*"), col("time_features.*"))
      .drop("time_features")

    // 4) Date convertie en vrai Timestamp (nécessaire pour les fenêtres).
    //    On ne convertit que si l'UDF a validé le timestamp (hour non null),
    //    car to_timestamp lève une exception sur un texte mal formé.
    val withDate = withTime
      .withColumn(
        "transaction_date",
        when(col("hour").isNotNull, to_timestamp(col("timestamp"), "yyyyMMddHHmmss"))
      )

    // 5) Tranche d'âge (sans "otherwise" : age null => age_group null)
    val withAgeGroup = withDate.withColumn(
      "age_group",
      when(col("age") < 25, "Jeune")
        .when(col("age") < 45, "Adulte")
        .when(col("age") < 65, "Âge Moyen")
        .when(col("age") >= 65, "Senior")
    )

    // 6) Fonctions de fenêtrage de la Q3.2
    val byUserOrdered = Window.partitionBy("user_id")
      .orderBy(col("transaction_date"), col("transaction_id"))
    val byUser = Window.partitionBy("user_id")

    val ranked = withAgeGroup
      .withColumn("transaction_rank", row_number().over(byUserOrdered))
      .withColumn("total_transactions_user", count(lit(1)).over(byUser))

    // 7) Fenêtres glissantes de la Q3.3
    addTemporalWindows(ranked)

  }


  /** Q3.3 : montant cumulé sur 7 jours, utilisateur actif, délai depuis l'achat précédent.
   * Colonnes requises : user_id, transaction_id, transaction_date (Timestamp), amount. */
  def addTemporalWindows(df: DataFrame): DataFrame = {

    val byUserOrdered = Window.partitionBy("user_id")
      .orderBy(col("transaction_date"), col("transaction_id"))

    // Fenêtre glissante : de (instant - 7 jours) jusqu'à l'instant courant
    val sevenDaysInSeconds = 7L * 24 * 3600
    val last7Days = Window.partitionBy("user_id")
      .orderBy(col("transaction_date").cast("long"))
      .rangeBetween(-sevenDaysInSeconds, Window.currentRow)

    df
      .withColumn("montant_cumule_7j", sum("amount").over(last7Days))
      .withColumn("nb_jours_distincts_7j",
        size(collect_set(to_date(col("transaction_date"))).over(last7Days)))
      .withColumn("is_active_user",
        when(col("nb_jours_distincts_7j") >= 5, 1).otherwise(0))
      .withColumn("jours_depuis_achat_precedent",
        datediff(col("transaction_date"), lag(col("transaction_date"), 1).over(byUserOrdered)))
      .drop("nb_jours_distincts_7j")
  }
  /** Q3.4 (bonus) : écart au panier moyen et détection des transactions suspectes.
   * Colonnes requises : user_id, transaction_id, transaction_date, amount, day_period, payment_method. */
  def addSuspiciousFlags(df: DataFrame): DataFrame = {
    val byUser = Window.partitionBy("user_id")
    val byUserOrdered = Window.partitionBy("user_id")
      .orderBy(col("transaction_date"), col("transaction_id"))

    val dateEnSecondes = col("transaction_date").cast("long")

    df
      .withColumn("panier_moyen_user", avg("amount").over(byUser))
      .withColumn("ecart_panier_moyen_pct",
        when(col("panier_moyen_user") > 0,
          (col("amount") - col("panier_moyen_user")) / col("panier_moyen_user") * 100))
      .withColumn("delai_precedente_minutes",
        (dateEnSecondes - lag(dateEnSecondes, 1).over(byUserOrdered)) / 60.0)
      // when(...) sans otherwise vaut null si faux ou inconnu : coalesce le transforme en 0
      .withColumn("cond_montant", coalesce(when(col("ecart_panier_moyen_pct") > 300, 1), lit(0)))
      .withColumn("cond_night",   coalesce(when(col("day_period") === "Night", 1), lit(0)))
      .withColumn("cond_delai",   coalesce(when(col("delai_precedente_minutes") < 5, 1), lit(0)))
      .withColumn("cond_crypto",  coalesce(when(col("payment_method") === "CRYPTO", 1), lit(0)))
      .withColumn("nb_conditions",
        col("cond_montant") + col("cond_night") + col("cond_delai") + col("cond_crypto"))
      .withColumn("is_suspicious", when(col("nb_conditions") >= 2, 1).otherwise(0))
  }

  /** Q3.4 : uniquement les transactions suspectes, triées par montant décroissant. */
  def suspiciousTransactions(df: DataFrame): DataFrame =
    df.filter(col("is_suspicious") === 1)
      .select("transaction_id", "user_id", "merchant_id", "transaction_date", "amount",
        "panier_moyen_user", "ecart_panier_moyen_pct", "day_period",
        "delai_precedente_minutes", "payment_method",
        "cond_montant", "cond_night", "cond_delai", "cond_crypto",
        "nb_conditions", "is_suspicious")
      .orderBy(desc("amount"))
}