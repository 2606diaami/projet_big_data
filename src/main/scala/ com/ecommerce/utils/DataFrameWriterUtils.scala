package com.ecommerce.utils

import org.apache.spark.sql.DataFrame
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object DataFrameWriterUtils {

  // fonction de base pour ecrire un DataFrame en CSV et Parquet en meme temps
  def ecrireCsvEtParquet(df: DataFrame, nom: String, outputPath: String = "output/"): Unit = {
    ecrireCsvEtParquet(df, df, nom, outputPath)
  }

  // ici on peut donner un DataFrame different pour le CSV et pour le Parquet
  // j'en ai besoin pour rejets_users car preferred_categories est un tableau
  // (il faut le transformer en texte pour le CSV mais le garder tel quel pour le Parquet)
  def ecrireCsvEtParquet(dfCsv: DataFrame, dfParquet: DataFrame, nom: String, outputPath: String): Unit = {
    dfCsv.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"${outputPath}csv/$nom")

    dfParquet.write
      .mode("overwrite")
      .parquet(s"${outputPath}parquet/$nom")

    println(s"[ECRITURE] '$nom' sauvegardé en CSV et Parquet dans $outputPath")
  }

  // pour le rapport de qualite il faut un seul fichier csv avec un nom precis
  // (rapport_qualite_yyyymmdd.csv) mais Spark ecrit toujours un dossier avec
  // un fichier part-*.csv dedans, donc je recupere ce fichier et je le renomme
  def ecrireRapportQualiteUnique(df: DataFrame, outputPath: String = "output/"): Unit = {
    val dateStr = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"))
    val dossierTemp = s"${outputPath}_temp_rapport_qualite"

    df.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(dossierTemp)

    // on cherche le fichier part-*.csv genere par Spark
    val dossier = new java.io.File(dossierTemp)
    val fichierPart = dossier.listFiles().find(f => f.getName.startsWith("part-") && f.getName.endsWith(".csv"))

    fichierPart match {
      case Some(f) =>
        val cible = new java.io.File(s"$outputPath/rapport_qualite_$dateStr.csv")
        cible.getParentFile.mkdirs()
        java.nio.file.Files.copy(f.toPath, cible.toPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING)
        println(s"[ECRITURE] Rapport de qualité sauvegardé : ${cible.getPath}")
      case None =>
        println("[ERREUR] Fichier part-*.csv introuvable pour le rapport de qualité")
    }

    // on supprime le dossier temporaire, on n'en a plus besoin
    def supprimerDossier(f: java.io.File): Unit = {
      if (f.isDirectory) f.listFiles().foreach(supprimerDossier)
      f.delete()
    }
    supprimerDossier(dossier)
  }
}