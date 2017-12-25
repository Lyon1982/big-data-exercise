package com.github.lyon1982.BigDataExercise.builder.validator

import org.apache.spark.sql.Row

/**
  * Validate Hotel Booking data.
  */
class BookingValidator extends Validator {

  // only added columns needed for this exercise
  implicit val columnValidators: Map[String, ValidateFunction] = Map(
    "ClientId" -> validateNonEmptyString
    , "HotelId" -> validateNonEmptyString
    , "BookingDate" -> validateShortDatePattern
    , "StayDate" -> validateShortDatePattern
    , "StayDuration" -> validateIntGreaterThan(0))

  override def validate(row: Row): Boolean = validateColumns(row)

}
