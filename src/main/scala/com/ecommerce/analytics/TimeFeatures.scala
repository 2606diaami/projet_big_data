package com.ecommerce.analytics

import java.time.{DayOfWeek, LocalDateTime}
import java.time.format.{DateTimeFormatter, ResolverStyle, TextStyle}
import java.util.Locale

import scala.util.Try

import org.apache.spark.sql.functions.udf

/** Les 6 informations calculées pour un timestamp. Spark en fera une struct. */
case class TimeFeatures(
                         hour: Int,
                         day_of_week: String,
                         month: String,
                         is_weekend: Int,
                         day_period: String,
                         is_working_hours: Int
                       )

object TimeFeaturesUDF {

  // STRICT : refuse les dates impossibles (ex : 31 février)
  private val formatter = DateTimeFormatter
    .ofPattern("uuuuMMddHHmmss")
    .withResolverStyle(ResolverStyle.STRICT)

  def dayPeriod(hour: Int): String = {
    if (hour >= 6 && hour < 12) "Morning"
    else if (hour >= 12 && hour < 18) "Afternoon"
    else if (hour >= 18 && hour < 22) "Evening"
    else "Night"
  }

  /** Fonction pure : testable sans Spark. None si le timestamp est invalide. */
  def extract(ts: String): Option[TimeFeatures] = {
    Option(ts)
      .map(_.trim)
      .filter(_.length == 14)
      .flatMap(s => Try(LocalDateTime.parse(s, formatter)).toOption)
      .map { dt =>
        val h   = dt.getHour
        val dow = dt.getDayOfWeek
        TimeFeatures(
          hour             = h,
          day_of_week      = dow.getDisplayName(TextStyle.FULL, Locale.ENGLISH),
          month            = dt.getMonth.getDisplayName(TextStyle.FULL, Locale.ENGLISH),
          is_weekend       = if (dow == DayOfWeek.SATURDAY || dow == DayOfWeek.SUNDAY) 1 else 0,
          day_period       = dayPeriod(h),
          is_working_hours = if (h >= 9 && h < 17) 1 else 0
        )
      }
  }

  /** L'UDF Spark : enveloppe la fonction pure. */
  val extractTimeFeatures = udf((ts: String) => extract(ts))
}