package com.github.lyon1982.BigDataExercise.utils

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.scalatest.FunSuite

class DateTimeToolsTest extends FunSuite {

  test("A valid short date string should be correctly parsed.") {
    val inputDate = "05.16.16"
    val parsed = DateTimeTools.parseShortDate(inputDate)
    assert(2016 == parsed.getYear())
    assert(5 == parsed.getMonth().getValue())
    assert(16 == parsed.getDayOfMonth())
  }

  test("Invalid short date strings should be parsed to null without any exception thrown.") {
    assert(DateTimeTools.parseShortDate(null) == null)
    assert(DateTimeTools.parseShortDate("") == null)
    assert(DateTimeTools.parseShortDate("2017") == null)
    assert(DateTimeTools.parseShortDate("13.20.17") == null)
    assert(DateTimeTools.parseShortDate("2.30.15") == null)
  }

  test("A valid long date string should be correctly parsed.") {
    val inputDate = "2016-05-16"
    val parsed = DateTimeTools.parseLongDate(inputDate)
    assert(2016 == parsed.getYear())
    assert(5 == parsed.getMonth().getValue())
    assert(16 == parsed.getDayOfMonth())
  }

  test("Invalid long date strings should be parsed to null without any exception thrown.") {
    assert(DateTimeTools.parseLongDate(null) == null)
    assert(DateTimeTools.parseLongDate("") == null)
    assert(DateTimeTools.parseLongDate("2017") == null)
    assert(DateTimeTools.parseLongDate("2016-13-16") == null)
    assert(DateTimeTools.parseLongDate("2016-02-32") == null)
  }

  test("Given two valid date, it should calculate the period in days between them correctly.") {
    val to = LocalDate.now()
    val from = to.minusDays(5)
    val result = DateTimeTools.calculateDaysBetweenDate(from, to)
    assert(result == 5)
  }

  test("Given valid date, it should calculate the years till now.") {
    val today = LocalDate.now()
    val date = today.minusYears(5)
    assert(DateTimeTools.calculateYearsFromNow(date) == 5)
  }

  test("Given valid dates, it should calculate the days between them.") {
    assert(DateTimeTools.calculateDaysBetweenShortDate("12.10.17", "12.20.17") == 10)
  }

  test("Given invalid dates, calculation should return -1.") {
    assert(DateTimeTools.calculateDaysBetweenShortDate("", "") == -1)
    assert(DateTimeTools.calculateDaysBetweenShortDate("", "12.20.17") == -1)
    assert(DateTimeTools.calculateDaysBetweenShortDate("12.10.17", "") == -1)
    assert(DateTimeTools.calculateDaysBetweenShortDate("99.99.99", "12.20.17") == -1)
    assert(DateTimeTools.calculateDaysBetweenShortDate("12.10.17", "99.99.99") == -1)
  }

  test("Given valid dates, it should calculate the years from now.") {
    val today = LocalDate.now()
    val date = today.minusYears(5)
    assert(DateTimeTools.calculateYearsFromNow(date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))) == 5)
  }

  test("Given invalid dates, calculation of the years from now should return -1.") {
    assert(DateTimeTools.calculateYearsFromNow("") == -1)
    assert(DateTimeTools.calculateYearsFromNow("2017-12") == -1)
    assert(DateTimeTools.calculateYearsFromNow("2017-12-99") == -1)
  }

}
