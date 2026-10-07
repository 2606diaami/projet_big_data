package com.ecommerce.utils

import com.typesafe.config.{Config, ConfigFactory}

// cet objet charge le fichier application.conf une seule fois au demarrage
// comme ca aucun chemin ou parametre n'est code en dur dans le reste du projet
object ConfigLoader {
  private val config: Config = ConfigFactory.load().getConfig("app")

  val appName: String = config.getString("name")

  val sparkMaster: String = config.getString("spark.master")
  val shufflePartitions: Int = config.getInt("spark.shuffle.partitions")

  val enableCache: Boolean = config.getBoolean("optimization.enable-cache")
  val enableBroadcast: Boolean = config.getBoolean("optimization.enable-broadcast")

  val transactionsPath: String = config.getString("data.input.transactions")
  val usersPath: String = config.getString("data.input.users")
  val productsPath: String = config.getString("data.input.products")
  val merchantsPath: String = config.getString("data.input.merchants")

  val outputPath: String = config.getString("data.output.path")

  val transactionMinAmount: Double = config.getDouble("validation.transaction.min-amount")
  val userMinAge: Int = config.getInt("validation.user.min-age")
  val userMaxAge: Int = config.getInt("validation.user.max-age")
  val productMinRating: Double = config.getDouble("validation.product.min-rating")
  val productMaxRating: Double = config.getDouble("validation.product.max-rating")
  val merchantMinCommission: Double = config.getDouble("validation.merchant.min-commission")
  val merchantMaxCommission: Double = config.getDouble("validation.merchant.max-commission")
}