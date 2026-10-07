package com.ecommerce.analytics

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

// Q4.3 BONUS : segmentation RFM (Recence, Frequence, Montant) des clients
object RFMSegmentation {

  // calcule les 3 indicateurs RFM par utilisateur
  def calculerRFM(transactionsEnrichies: DataFrame): DataFrame = {
    // transaction_date peut etre en String ou deja en Timestamp selon la source,
    // on s'assure qu'elle est bien en Timestamp avant de calculer la recence
    val estDejaTimestamp = transactionsEnrichies.schema("transaction_date").dataType == TimestampType
    val df =
      if (estDejaTimestamp) transactionsEnrichies
      else transactionsEnrichies.withColumn("transaction_date", to_timestamp(col("transaction_date")))

    val dateMax = df.agg(max(col("transaction_date"))).first().getAs[java.sql.Timestamp](0)

    df.groupBy("user_id", "customer_segment")
      .agg(
        datediff(lit(dateMax), max(col("transaction_date"))).alias("recence_jours"),
        count("*").alias("frequence"),
        sum("amount").alias("montant")
      )
  }

  // attribue un score de 1 a 5 a chaque indicateur avec des quintiles (ntile)
  // pour la recence, le score 5 doit aller aux clients les PLUS RECENTS,
  // donc on trie en ordre decroissant pour que les grandes valeurs (moins recent) aient le score 1
  def attribuerScores(rfm: DataFrame): DataFrame = {
    val fenetreRecence = Window.orderBy(col("recence_jours").desc)
    val fenetreFrequence = Window.orderBy(col("frequence").asc)
    val fenetreMontant = Window.orderBy(col("montant").asc)

    rfm
      .withColumn("score_r", ntile(5).over(fenetreRecence))
      .withColumn("score_f", ntile(5).over(fenetreFrequence))
      .withColumn("score_m", ntile(5).over(fenetreMontant))
      .withColumn("score_rfm", concat(col("score_r"), col("score_f"), col("score_m")))
  }

  // deduit le segment metier a partir des 3 scores
  // (regles definies par le groupe, voir justification dans README.md)
  def determinerSegment(df: DataFrame): DataFrame = {
    df.withColumn("segment_rfm",
      when(col("score_r") >= 4 && col("score_f") >= 4 && col("score_m") >= 4, "Champions")
        .when(col("score_f") === 1 && col("score_r") >= 4, "Nouveaux")
        .when(col("score_r") <= 2 && col("score_f") <= 2 && col("score_m") <= 2, "Perdus")
        .when(col("score_r") <= 2 && (col("score_f") >= 3 || col("score_m") >= 3), "À risque")
        .otherwise("Clients fidèles")
    )
  }

  // assemble les 3 etapes au-dessus et selectionne les colonnes dans l'ordre demande
  def genererRFM(transactionsEnrichies: DataFrame): DataFrame = {
    val rfm = calculerRFM(transactionsEnrichies)
    val avecScores = attribuerScores(rfm)
    val avecSegment = determinerSegment(avecScores)

    avecSegment.select(
      "user_id", "customer_segment", "recence_jours", "frequence", "montant",
      "score_r", "score_f", "score_m", "score_rfm", "segment_rfm"
    )
  }

  // tableau croise : segment_rfm en ligne, customer_segment en colonne, + une colonne total
  def croiserAvecCustomerSegment(rfmFinal: DataFrame): DataFrame = {
    val pivot = rfmFinal.groupBy("segment_rfm").pivot("customer_segment").count().na.fill(0)
    val colonnesSegments = pivot.columns.filter(_ != "segment_rfm")

    pivot.withColumn("total", colonnesSegments.map(col).reduce(_ + _))
  }
}