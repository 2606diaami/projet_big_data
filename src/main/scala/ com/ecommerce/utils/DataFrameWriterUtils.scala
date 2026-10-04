package com.ecommerce.utils

import org.apache.spark.sql.DataFrame

/**
 * Objet utilitaire centralisant l'écriture des résultats (voir NB Question 1.1
 * du sujet). Chaque résultat du projet doit être écrit deux fois : en CSV
 * (lisible/Excel) et en Parquet (format optimisé pour Spark). Au lieu que
 * chaque membre réécrive ce code, tout le monde appelle cette fonction unique.
 */
object DataFrameWriterUtils {

  /**
   * Ecrit un DataFrame en CSV ET en Parquet, dans <outputPath>/csv/<nom>/
   * et <outputPath>/parquet/<nom>/, en mode "overwrite" (écrase si ça existe déjà).
   *
   * @param df le résultat à sauvegarder (ex: kpiMarchands)
   * @param nom le nom du résultat (ex: "kpi_marchands") -> sert de nom de dossier
   * @param outputPath le dossier racine de sortie (ex: "output/")
   */
  def ecrireCsvEtParquet(df: DataFrame, nom: String, outputPath: String = "output/"): Unit = {

    df.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"${outputPath}csv/$nom")

    df.write
      .mode("overwrite")
      .parquet(s"${outputPath}parquet/$nom")

    println(s"[ECRITURE] '$nom' sauvegardé en CSV et Parquet dans $outputPath")
  }
}