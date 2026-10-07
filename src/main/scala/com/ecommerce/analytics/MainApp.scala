package com.ecommerce.analytics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws}
import com.ecommerce.utils.{ConfigLoader, DataFrameWriterUtils}

object MainApp {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark: SparkSession = SparkSession.builder()
      .master(ConfigLoader.sparkMaster)
      .appName(ConfigLoader.appName)
      .config("spark.sql.shuffle.partitions", ConfigLoader.shufflePartitions)
      .getOrCreate()

    try {
      println("=== DEBUT DU PIPELINE ===")

      // PHASE 1 : ingestion des 4 fichiers de donnees
      println("[PHASE 1] Ingestion des données...")
      val transactions = DataIngestion.lireTransactions(spark)
      val users = DataIngestion.lireUsers(spark)
      val products = DataIngestion.lireProducts(spark)
      val merchants = DataIngestion.lireMerchants(spark)

      // PHASE 2 : validation + ecriture des lignes rejetees + rapport de qualite
      println("[PHASE 2] Validation des données...")

      val (transValides, transRejetees) = DataValidation.validerTransactions(transactions)
      DataFrameWriterUtils.ecrireCsvEtParquet(transRejetees, "rejets_transactions", ConfigLoader.outputPath)

      val (usersValides, usersRejetees) = DataValidation.validerUsers(users)
      // preferred_categories est un tableau, il faut le convertir en texte pour le CSV
      val usersRejeteesCsv = usersRejetees.withColumn("preferred_categories", concat_ws(",", col("preferred_categories")))
      DataFrameWriterUtils.ecrireCsvEtParquet(usersRejeteesCsv, usersRejetees, "rejets_users", ConfigLoader.outputPath)

      val (productsValides, productsRejetees) = DataValidation.validerProducts(products)
      DataFrameWriterUtils.ecrireCsvEtParquet(productsRejetees, "rejets_products", ConfigLoader.outputPath)

      val (merchantsValides, merchantsRejetees) = DataValidation.validerMerchants(merchants)
      DataFrameWriterUtils.ecrireCsvEtParquet(merchantsRejetees, "rejets_merchants", ConfigLoader.outputPath)

      // Q2.5 BONUS : integrite referentielle, on compte les transactions "orphelines"
      println("[BONUS Q2.5] Vérification de l'intégrité référentielle...")
      val nbUserOrphelins = DataQualityReport.compterOrphelins(transactions.toDF(), "user_id", users.toDF(), "user_id")
      val nbProductOrphelins = DataQualityReport.compterOrphelins(transactions.toDF(), "product_id", products.toDF(), "product_id")
      val nbMerchantOrphelins = DataQualityReport.compterOrphelins(transactions.toDF(), "merchant_id", merchants.toDF(), "merchant_id")
      println(s"[BONUS Q2.5] user_id orphelins: $nbUserOrphelins, product_id orphelins: $nbProductOrphelins, merchant_id orphelins: $nbMerchantOrphelins")

      // rapport de qualite global (Q2.4 + bonus Q2.5)
      val ligneTrans = DataQualityReport.construireLigneQualite(
        "transactions", transactions.toDF(), transValides, transRejetees,
        nbUserOrphelins, nbProductOrphelins, nbMerchantOrphelins)
      val ligneUsers = DataQualityReport.construireLigneQualite("users", users.toDF(), usersValides, usersRejetees)
      val ligneProducts = DataQualityReport.construireLigneQualite("products", products.toDF(), productsValides, productsRejetees)
      val ligneMerchants = DataQualityReport.construireLigneQualite("merchants", merchants.toDF(), merchantsValides, merchantsRejetees)

      val rapport = DataQualityReport.genererRapport(spark, Seq(ligneTrans, ligneUsers, ligneProducts, ligneMerchants))
      println("=== RAPPORT DE QUALITE DES DONNEES ===")
      rapport.show(false)
      DataFrameWriterUtils.ecrireRapportQualiteUnique(rapport, ConfigLoader.outputPath)

      // PHASE 3 : transformation et enrichissement
      // NOTE : en attendant que la Partie 3 (transformation) soit prete, on utilise
      // encore les donnees simulees de TestDataGenerator pour tester la Partie 4/5/6.
      // Il faudra remplacer cette ligne par le vrai DataFrame enrichi une fois la
      // Partie 3 terminee (jointure transactions + users + products + merchants).
      println("[PHASE 3] Transformation et enrichissement...")
      val transactionsEnrichies = TestDataGenerator.genererTransactionsEnrichies(spark)
      SparkOptimizations.cacheAndMaterialize(transactionsEnrichies, "transactionsEnrichies")

      // PHASE 4 : calcul des indicateurs
      println("[PHASE 4] Calcul des indicateurs...")
      val kpiMarchands = Analytics.kpiMarchands(transactionsEnrichies)
      kpiMarchands.show(false)
      DataFrameWriterUtils.ecrireCsvEtParquet(kpiMarchands, "kpi_marchands", ConfigLoader.outputPath)

      val cohortesRetention = Analytics.analyseCohortes(transactionsEnrichies)
      cohortesRetention.show(false)
      DataFrameWriterUtils.ecrireCsvEtParquet(cohortesRetention, "cohortes_retention", ConfigLoader.outputPath)

      val meilleureCohorte = Analytics.meilleureCohorte3m(cohortesRetention)
      meilleureCohorte.show(false)
      DataFrameWriterUtils.ecrireCsvEtParquet(meilleureCohorte, "meilleure_cohorte_3m", ConfigLoader.outputPath)

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