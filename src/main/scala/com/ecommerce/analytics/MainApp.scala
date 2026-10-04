package com.ecommerce.analytics

import org.apache.spark.sql.SparkSession
import com.ecommerce.utils.DataFrameWriterUtils

object MainApp {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("EcommerceAnalytics")
      .getOrCreate()

    try {

      println("=== DEBUT DU PIPELINE ===")

      println("[PHASE 1] Ingestion des données...")
      // TODO : Membre A

      println("[PHASE 2] Validation des données...")
      // TODO : Membre A

      println("[PHASE 3] Transformation et enrichissement...")
      // TODO : Membre B
      val transactionsEnrichies = TestDataGenerator.genererTransactionsEnrichies(spark)

      SparkOptimizations.cacheAndMaterialize(transactionsEnrichies, "transactionsEnrichies")

      println("[PHASE 4] Calcul des indicateurs...")

      // Q4.1 : KPI par marchand
      val kpiMarchands = Analytics.kpiMarchands(transactionsEnrichies)
      println("=== KPI par marchand ===")
      kpiMarchands.show(false)
      DataFrameWriterUtils.ecrireCsvEtParquet(kpiMarchands, "kpi_marchands")

      // Q4.2 : Analyse de cohortes
      val cohortesRetention = Analytics.analyseCohortes(transactionsEnrichies)
      println("=== Matrice de rétention par cohorte ===")
      cohortesRetention.show(false)
      DataFrameWriterUtils.ecrireCsvEtParquet(cohortesRetention, "cohortes_retention")

      val meilleureCohorte = Analytics.meilleureCohorte3m(cohortesRetention)
      println("=== Meilleure cohorte à 3 mois ===")
      meilleureCohorte.show(false)
      DataFrameWriterUtils.ecrireCsvEtParquet(meilleureCohorte, "meilleure_cohorte_3m")

      SparkOptimizations.releaseCache(transactionsEnrichies, "transactionsEnrichies")

      println("=== PIPELINE TERMINE AVEC SUCCES ===")

    } catch {
      case e: Exception =>
        println(s"[ERREUR] Le pipeline a échoué : ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
      println("=== SparkSession arrêtée proprement ===")
    }
  }
}