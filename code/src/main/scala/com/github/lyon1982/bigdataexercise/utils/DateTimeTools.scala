package com.github.lyon1982.BigDataExercise.utils

import java.time.{LocalDate, Period}
import java.time.format.DateTimeFormatter

/**
  * Provides common datetime utilities.
  */
object DateTimeTools {

  // Short date pattern(e.g., 01.02.18)
  private val SHORT_DATE_PATTERN = DateTimeFormatter.ofPattern("MM.dd.yy")

  // Long date pattern(e.g., 1939-06-03)
  private val LONG_DATE_PATTERN = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  /**
    * Parse short date pattern string like MM.dd.yy(e.g. 10.21.17).
    *
    * @param dateInShortPattern short date string
    * @return local date object or null on illegal input
    */
  def parseShortDate(dateInShortPattern: String): LocalDate = parseDate(dateInShortPattern, SHORT_DATE_PATTERN)

  /**
    * Parse long date pattern string like yyyy-MM-dd(e.g. 1939-06-03).
    *
    * @param dateInLongPattern long date string
    * @return local date object or null on illegal input
    */
  def parseLongDate(dateInLongPattern: String): LocalDate = parseDate(dateInLongPattern, LONG_DATE_PATTERN)

  /**
    * Obtains the days consisting between two dates.
    *
    * @param from the start date(included)
    * @param to   the end date(excluded)
    * @return days between two dates
    */
  def calculateDaysBetweenDate(from: LocalDate, to: LocalDate): Int = {
    Period.between(from, to).getDays()
  }

  /**
    * Calculate the period in years between given date and today.
    *
    * @param date given date
    * @return years between given date and today
    */
  def calculateYearsFromNow(date: LocalDate): Int = {
    val now = LocalDate.now()
    Period.between(date, now).getYears()
  }

  /**
    * Calculate the period in days between two short date pattern string.
    *
    * @return period in days or -1 if any date is invalid
    */
  def calculateDaysBetweenShortDate(from: String, to: String): Int = {
    val start = parseShortDate(from)
    val end = parseShortDate(to)
    // if any date is invalid, return -1
    if (null == start || null == end) {
      return -1
    }
    calculateDaysBetweenDate(start, end)
  }

  /**
    * Calculate the period in years between given date and today.
    *
    * @param dateStr given date
    * @return years between given date and today or -1 if any date is invalid
    */
  def calculateYearsFromNow(dateStr: String): Int = {
    val date = parseLongDate(dateStr)
    if(null == date) {
      return -1
    }
    calculateYearsFromNow(date)
  }

  /**
    * Parse date pattern string.
    *
    * @param date    date string
    * @param pattern pattern
    * @return local date object or null on illegal input
    */
  private def parseDate(date: String, pattern: DateTimeFormatter): LocalDate = {
    // This method will be used a lot, so avoid using Option[LocalDate]
    // which can create more objects that could take more memory and GC time.
    try {
      LocalDate.parse(date, pattern)
    } catch {
      case _: Throwable => null
    }
  }

}
