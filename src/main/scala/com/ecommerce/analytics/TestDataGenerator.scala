package com.ecommerce.analytics

import org.apache.spark.sql.{DataFrame, SparkSession}

case class TransactionEnrichie(
                                transaction_id: String, user_id: String, product_id: String, merchant_id: String,
                                amount: Double, transaction_date: String, location: String, payment_method: String,
                                category: String, age: Int, annual_income: Double, city: String, customer_segment: String,
                                age_group: String, merchant_name: String, merchant_category: String, region: String,
                                commission_rate: Double, hour: Int, day_of_week: String, month: String,
                                is_weekend: Int, day_period: String, is_working_hours: Int
                              )

object TestDataGenerator {
  def genererTransactionsEnrichies(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val donnees = Seq(
      TransactionEnrichie("TX0001", "U001", "P001", "M001", 221.80, "2025-01-15 19:08:09", "Paris", "CARD",
        "Home & Garden", 22, 41771.79, "Paris", "Standard", "Jeune",
        "GardenShop 1", "Home & Garden", "Ile-de-France", 0.0682,
        19, "Wednesday", "January", 0, "Evening", 0),
      TransactionEnrichie("TX0002", "U002", "P002", "M002", 1331.20, "2025-02-06 18:11:26", "Paris", "CARD",
        "Electronics", 37, 53809.44, "Strasbourg", "Premium", "Adulte",
        "ElecStore 2", "Electronics", "Ile-de-France", 0.0450,
        18, "Thursday", "February", 0, "Evening", 0),
      TransactionEnrichie("TX0003", "U003", "P003", "M003", 907.60, "2025-05-16 20:05:23", "Strasbourg", "CASH",
        "Electronics", 51, 61200.00, "Strasbourg", "VIP", "Âge Moyen",
        "ElecStore 2", "Electronics", "Grand Est", 0.0450,
        20, "Friday", "May", 0, "Evening", 0),
      TransactionEnrichie("TX0004", "U001", "P004", "M001", 761.61, "2025-01-25 08:02:19", "Paris", "CARD",
        "Home & Garden", 22, 41771.79, "Paris", "Standard", "Jeune",
        "GardenShop 1", "Home & Garden", "Ile-de-France", 0.0682,
        8, "Saturday", "January", 1, "Morning", 0),
      TransactionEnrichie("TX0005", "U004", "P005", "M004", 305.00, "2025-03-02 14:20:00", "Lyon", "CARD",
        "Beauty", 70, 22000.00, "Lyon", "Budget", "Senior",
        "BeautyPlus 4", "Beauty", "Auvergne-Rhône-Alpes", 0.0300,
        14, "Sunday", "March", 1, "Afternoon", 1),
      TransactionEnrichie("TX0006", "U001", "P001", "M001", 150.00, "2025-04-10 10:00:00", "Paris", "CARD",
        "Home & Garden", 22, 41771.79, "Paris", "Standard", "Jeune",
        "GardenShop 1", "Home & Garden", "Ile-de-France", 0.0682,
        10, "Thursday", "April", 0, "Morning", 1),
      TransactionEnrichie("TX0007", "U002", "P002", "M002", 500.00, "2025-05-06 11:00:00", "Paris", "CARD",
        "Electronics", 37, 53809.44, "Strasbourg", "Premium", "Adulte",
        "ElecStore 2", "Electronics", "Ile-de-France", 0.0450,
        11, "Tuesday", "May", 0, "Morning", 1)
    )
    donnees.toDF()
  }
}