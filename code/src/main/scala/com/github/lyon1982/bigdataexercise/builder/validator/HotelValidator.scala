package com.github.lyon1982.BigDataExercise.builder.validator

import org.apache.spark.sql.Row

/**
  * Validate Hotel data.
  */
class HotelValidator extends Validator {

  // only added columns needed for this exercise
  implicit val columnValidators: Map[String, ValidateFunction] = Map(
    "HotelId" -> validateNonEmptyString
    , "City" -> validateNonEmptyString
    , "Country" -> validateNonEmptyString)

  override def validate(row: Row): Boolean = validateColumns(row)

}
