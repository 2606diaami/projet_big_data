package com.ecommerce.analytics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws, to_timestamp}
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

      println("[PHASE 1] Ingestion des données...")
      val transactions = DataIngestion.lireTransactions(spark)
      val users = DataIngestion.lireUsers(spark)
      val products = DataIngestion.lireProducts(spark)
      val merchants = DataIngestion.lireMerchants(spark)

      println("[PHASE 2] Validation des données...")
      val (transValides, transRejetees) = DataValidation.validerTransactions(transactions)
      DataFrameWriterUtils.ecrireCsvEtParquet(transRejetees, "rejets_transactions", ConfigLoader.outputPath)

      val (usersValides, usersRejetees) = DataValidation.validerUsers(users)
      val usersRejeteesCsv = usersRejetees.withColumn("preferred_categories", concat_ws(",", col("preferred_categories")))
      DataFrameWriterUtils.ecrireCsvEtParquet(usersRejeteesCsv, usersRejetees, "rejets_users", ConfigLoader.outputPath)

      val (productsValides, productsRejetees) = DataValidation.validerProducts(products)
      DataFrameWriterUtils.ecrireCsvEtParquet(productsRejetees, "rejets_products", ConfigLoader.outputPath)

      val (merchantsValides, merchantsRejetees) = DataValidation.validerMerchants(merchants)
      DataFrameWriterUtils.ecrireCsvEtParquet(merchantsRejetees, "rejets_merchants", ConfigLoader.outputPath)

      println("[BONUS Q2.5] Vérification de l'intégrité référentielle...")
      val nbUserOrphelins = DataQualityReport.compterOrphelins(transactions.toDF(), "user_id", users.toDF(), "user_id")
      val nbProductOrphelins = DataQualityReport.compterOrphelins(transactions.toDF(), "product_id", products.toDF(), "product_id")
      val nbMerchantOrphelins = DataQualityReport.compterOrphelins(transactions.toDF(), "merchant_id", merchants.toDF(), "merchant_id")
      println(s"[BONUS Q2.5] user_id orphelins: $nbUserOrphelins, product_id orphelins: $nbProductOrphelins, merchant_id orphelins: $nbMerchantOrphelins")

      val ligneTrans = DataQualityReport.construireLigneQualite("transactions", transactions.toDF(), transValides, transRejetees, nbUserOrphelins, nbProductOrphelins, nbMerchantOrphelins)
      val ligneUsers = DataQualityReport.construireLigneQualite("users", users.toDF(), usersValides, usersRejetees)
      val ligneProducts = DataQualityReport.construireLigneQualite("products", products.toDF(), productsValides, productsRejetees)
      val ligneMerchants = DataQualityReport.construireLigneQualite("merchants", merchants.toDF(), merchantsValides, merchantsRejetees)

      val rapport = DataQualityReport.genererRapport(spark, Seq(ligneTrans, ligneUsers, ligneProducts, ligneMerchants))
      println("=== RAPPORT DE QUALITE DES DONNEES ===")
      rapport.show(false)
      DataFrameWriterUtils.ecrireRapportQualiteUnique(rapport, ConfigLoader.outputPath)

      // PHASE 3 : transformation reelle faite par le camarade (Partie B), recuperee via git
      println("[PHASE 3] Transformation et enrichissement...")
      val transformateur = new DataTransformation()
      val transactionsEnrichies = transformateur.enrichTransactionData(transValides, usersValides, productsValides, merchantsValides)
      SparkOptimizations.cacheAndMaterialize(transactionsEnrichies, "transactionsEnrichies")

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

      // BONUS Q4.3 : segmentation RFM, calculee sur les  transactions + users valides
      println("[BONUS Q4.3] Segmentation RFM des clients...")
      val transactionsPourRFM = transValides
        .join(usersValides.select("user_id", "customer_segment"), Seq("user_id"))
        .withColumn("transaction_date", to_timestamp(col("timestamp"), "yyyyMMddHHmmss"))
        .select("user_id", "customer_segment", "transaction_date", "amount")

      val rfmClients = RFMSegmentation.genererRFM(transactionsPourRFM)
      rfmClients.show(false)
      DataFrameWriterUtils.ecrireCsvEtParquet(rfmClients, "rfm_clients", ConfigLoader.outputPath)

      val rfmCroise = RFMSegmentation.croiserAvecCustomerSegment(rfmClients)
      rfmCroise.show(false)
      DataFrameWriterUtils.ecrireCsvEtParquet(rfmCroise, "rfm_x_customer_segment", ConfigLoader.outputPath)

      // BONUS Q4.4 : analyse produits et categories
      println("[BONUS Q4.4] Analyse produits et categories...")
      val top10Produits = ProductAnalytics.top10Produits(transValides, productsValides)
      top10Produits.show(false)
      DataFrameWriterUtils.ecrireCsvEtParquet(top10Produits, "top10_produits", ConfigLoader.outputPath)

      val caCategorieRegion = ProductAnalytics.caCategorieRegion(transValides, merchantsValides)
      caCategorieRegion.show(false)
      DataFrameWriterUtils.ecrireCsvEtParquet(caCategorieRegion, "ca_categorie_region", ConfigLoader.outputPath)

      val caPaiementPeriode = ProductAnalytics.caPaiementPeriode(transValides)
      caPaiementPeriode.show(false)
      DataFrameWriterUtils.ecrireCsvEtParquet(caPaiementPeriode, "ca_paiement_periode", ConfigLoader.outputPath)

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