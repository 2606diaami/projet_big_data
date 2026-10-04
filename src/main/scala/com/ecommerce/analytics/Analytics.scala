package com.ecommerce.analytics

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Analytics {

  def kpiMarchands(df: DataFrame): DataFrame = {

    val kpiBase = df.groupBy("merchant_id", "merchant_name", "merchant_category", "region")
      .agg(
        round(sum("amount"), 2).as("chiffre_affaires"),
        count("*").as("nb_transactions"),
        countDistinct("user_id").as("nb_clients_uniques"),
        round(avg("amount"), 2).as("montant_moyen"),
        round(sum(col("amount") * col("commission_rate")), 2).as("commission_totale")
      )

    val pivotAge = df.groupBy("merchant_id")
      .pivot("age_group", Seq("Jeune", "Adulte", "Âge Moyen", "Senior"))
      .agg(round(sum("amount"), 2))
      .na.fill(0)

    val pivotAgeRenomme = pivotAge
      .withColumnRenamed("Jeune", "ca_jeune")
      .withColumnRenamed("Adulte", "ca_adulte")
      .withColumnRenamed("Âge Moyen", "ca_age_moyen")
      .withColumnRenamed("Senior", "ca_senior")

    val kpiComplet = kpiBase.join(pivotAgeRenomme, "merchant_id")

    val fenetreCategorie = Window.partitionBy("merchant_category").orderBy(col("chiffre_affaires").desc)
    val fenetreRegion = Window.partitionBy("region").orderBy(col("chiffre_affaires").desc)

    kpiComplet
      .withColumn("rang_ca_categorie", rank().over(fenetreCategorie))
      .withColumn("rang_ca_region", rank().over(fenetreRegion))
      .select(
        "merchant_id", "merchant_name", "merchant_category", "region",
        "chiffre_affaires", "nb_transactions", "nb_clients_uniques", "montant_moyen",
        "rang_ca_categorie", "rang_ca_region", "commission_totale",
        "ca_jeune", "ca_adulte", "ca_age_moyen", "ca_senior"
      )
  }

  def analyseCohortes(df: DataFrame): DataFrame = {

    val avecMoisTransaction = df
      .withColumn("date_transaction", to_date(col("transaction_date")))
      .withColumn("mois_transaction", date_format(col("date_transaction"), "yyyy-MM"))

    val fenetreUtilisateur = Window.partitionBy("user_id")
    val avecCohorte = avecMoisTransaction
      .withColumn("cohort_month", min("mois_transaction").over(fenetreUtilisateur))

    val avecPeriode = avecCohorte
      .withColumn("date_cohorte", to_date(concat(col("cohort_month"), lit("-01"))))
      .withColumn("date_mois_transaction", to_date(concat(col("mois_transaction"), lit("-01"))))
      .withColumn("period_index",
        months_between(col("date_mois_transaction"), col("date_cohorte")).cast("int"))

    val tailleCohortes = avecPeriode
      .filter(col("period_index") === 0)
      .groupBy("cohort_month")
      .agg(countDistinct("user_id").as("taille_cohorte"))

    val activite = avecPeriode
      .groupBy("cohort_month", "period_index")
      .agg(
        countDistinct("user_id").as("nb_utilisateurs_actifs"),
        round(sum("amount"), 2).as("chiffre_affaires")
      )

    activite
      .join(tailleCohortes, "cohort_month")
      .withColumn("taux_retention",
        round(col("nb_utilisateurs_actifs") * 100.0 / col("taille_cohorte"), 2))
      .withColumn("revenu_moyen_par_utilisateur",
        round(col("chiffre_affaires") / col("nb_utilisateurs_actifs"), 2))
      .select(
        "cohort_month", "period_index", "taille_cohorte", "nb_utilisateurs_actifs",
        "taux_retention", "chiffre_affaires", "revenu_moyen_par_utilisateur"
      )
      .orderBy("cohort_month", "period_index")
  }

  def meilleureCohorte3m(cohortesRetention: DataFrame): DataFrame = {
    cohortesRetention
      .filter(col("period_index") === 3)
      .orderBy(col("taux_retention").desc)
      .limit(1)
      .select(
        col("cohort_month"),
        col("taille_cohorte"),
        col("nb_utilisateurs_actifs").as("nb_utilisateurs_actifs_m3"),
        col("taux_retention").as("taux_retention_m3")
      )
  }
}