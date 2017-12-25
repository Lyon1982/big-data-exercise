package com.github.lyon1982.BigDataExercise.builder.validator

import org.apache.spark.sql.Row

/**
  * Validate Customer data.
  */
class CustomerValidator extends Validator {

  // only added columns needed for this exercise
  implicit val columnValidators: Map[String, ValidateFunction] = Map(
    "ClientId" -> validateNonEmptyString
    , "BirthDate" -> validateLongDatePattern
    , "Gender" -> validateOption("F", "M"))

  override def validate(row: Row): Boolean = validateColumns(row)

}
