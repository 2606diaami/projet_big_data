package com.ecommerce.analytics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DataQualityReport {

  // compte le nombre total de valeurs nulles dans toutes les colonnes d'un DataFrame
  def compterValeursNulles(df: DataFrame): Long = {
    val comptes = df.select(
      df.columns.map(c => count(when(col(c).isNull, lit(1))).alias(c)): _*
    ).first()

    df.columns.indices.map(i => comptes.getLong(i)).sum
  }

  // Q2.5 BONUS : compte les transactions dont une cle de reference (user_id,
  // product_id ou merchant_id) n'existe pas dans le dataset correspondant.
  // on utilise une anti-jointure : ca garde seulement les lignes de transactions
  // qui n'ont pas trouve de correspondance dans le referentiel
  def compterOrphelins(transactions: DataFrame, colTransaction: String, referentiel: DataFrame, colReferentiel: String): Long = {
    transactions.join(referentiel, transactions(colTransaction) === referentiel(colReferentiel), "left_anti").count()
  }

  // construit une ligne du rapport pour un dataset
  // les 3 derniers parametres (orphelins) servent seulement pour la ligne "transactions" (bonus Q2.5)
  // pour les autres datasets on laisse la valeur par defaut 0
  def construireLigneQualite(
                              nomDataset: String,
                              dfOriginal: DataFrame,
                              dfValides: DataFrame,
                              dfRejetees: DataFrame,
                              nbUserOrphelins: Long = 0L,
                              nbProductOrphelins: Long = 0L,
                              nbMerchantOrphelins: Long = 0L
                            ): (String, Long, Long, Long, Double, Long, Long, Long, Long) = {

    val nbLues = dfOriginal.count()
    val nbValides = dfValides.count()
    val nbRejetees = dfRejetees.count()

    // taux de rejet en %, arrondi a 2 decimales comme demande dans le sujet
    val tauxRejet =
      if (nbLues == 0) 0.0
      else BigDecimal(nbRejetees.toDouble / nbLues.toDouble * 100)
        .setScale(2, BigDecimal.RoundingMode.HALF_UP)
        .toDouble

    val nbValeursNulles = compterValeursNulles(dfOriginal)

    (nomDataset, nbLues, nbValides, nbRejetees, tauxRejet, nbValeursNulles,
      nbUserOrphelins, nbProductOrphelins, nbMerchantOrphelins)
  }

  // regroupe les lignes (une par dataset) dans un seul DataFrame final
  def genererRapport(
                      spark: SparkSession,
                      lignes: Seq[(String, Long, Long, Long, Double, Long, Long, Long, Long)]
                    ): DataFrame = {
    import spark.implicits._

    lignes.toDF(
      "dataset",
      "nb_lignes_lues",
      "nb_lignes_valides",
      "nb_lignes_rejetees",
      "taux_rejet",
      "nb_valeurs_nulles",
      "nb_user_id_orphelins",
      "nb_product_id_orphelins",
      "nb_merchant_id_orphelins"
    )
  }
}