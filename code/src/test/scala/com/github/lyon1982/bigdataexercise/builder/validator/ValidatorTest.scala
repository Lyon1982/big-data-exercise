package com.github.lyon1982.BigDataExercise.builder.validator

import org.apache.spark.sql.Row
import org.scalatest.FunSuite

class ValidatorTest extends FunSuite with Validator {

  implicit val columnValidators: Map[String, ValidateFunction] = Map(
    "Col1" -> validateNonEmptyString
    , "Col2" -> validateLongDatePattern
    , "Col3" -> validateOption("F", "M"))

  override def validate(row: Row): Boolean = validateColumns(row)

  test("validateNonEmptyString") {
    assert(validateNonEmptyString(null) == false)
    assert(validateNonEmptyString("") == false)
    assert(validateNonEmptyString("some str") == true)
  }

  test("validateShortDatePattern") {
    assert(validateShortDatePattern(null) == false)
    assert(validateShortDatePattern("") == false)
    assert(validateShortDatePattern("1.1.1") == false)
    assert(validateShortDatePattern("11.11.11") == true)
  }

  test("validateLongDatePattern") {
    assert(validateLongDatePattern(null) == false)
    assert(validateLongDatePattern("") == false)
    assert(validateLongDatePattern("1-1-1") == false)
    assert(validateLongDatePattern("11-11-11") == false)
    assert(validateLongDatePattern("1111-11-11") == true)
  }

  test("validateOption") {
    assert(validateOption("A", "B")(null) == false)
    assert(validateOption("A", "B")("") == false)
    assert(validateOption("A", "B")("C") == false)
    assert(validateOption("A", "B")("A") == true)
  }

  test("validateIntGreaterThan") {
    assert(validateIntGreaterThan(0)(null) == false)
    assert(validateIntGreaterThan(0)("") == false)
    assert(validateIntGreaterThan(0)(-1) == false)
    assert(validateIntGreaterThan(0)(0) == false)
    assert(validateIntGreaterThan(0)(1) == true)
  }

  test("validateColumns") {
    setColumns(Seq("Col1", "Col2", "Col3"))
    assert(validateColumns(Row("", "2012-12-12", "F")) == false)
    assert(validateColumns(Row("somestr", "2012-112-12", "F")) == false)
    assert(validateColumns(Row("somestr", "2012-12-12", "A")) == false)
    assert(validateColumns(Row("somestr", "2012-12-12", "F")) == true)
  }

}
