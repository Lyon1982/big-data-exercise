package com.github.lyon1982.BigDataExercise.builder.validator

import org.apache.spark.sql.Row

/**
  * Dataframe data row validator.
  */
trait Validator {

  var columns: Seq[String] = Seq()

  /**
    * Set columns need to be validate.
    *
    * @param cols columns
    */
  def setColumns(cols: Seq[String]): Unit = {
    columns = Seq() ++ cols
  }

  /**
    * Validate.
    *
    * @param row data row
    * @return valid or not
    */
  def validate(row: Row): Boolean

  protected type ValidateFunction = (Any) => Boolean

  // return false when input is null or on exception
  private def returnFalseOnNullOrException = (func: ValidateFunction) => {
    (input: Any) => {
      try {
        input != null && func.apply(input)
      } catch {
        case e: Throwable => false
      }
    }
  }

  // Check if string is not empty
  protected def validateNonEmptyString: ValidateFunction = returnFalseOnNullOrException((str) => {
    !str.asInstanceOf[String].isEmpty
  })

  // Check if short date pattern match MM.dd.yy
  protected def validateShortDatePattern: ValidateFunction = returnFalseOnNullOrException((str) => {
    str.asInstanceOf[String].matches("\\d{2}\\.\\d{2}\\.\\d{2}")
  })

  // Check if long date pattern match yyyy-MM-dd
  protected def validateLongDatePattern: ValidateFunction = returnFalseOnNullOrException((str) => {
    str.asInstanceOf[String].matches("\\d{4}-\\d{2}-\\d{2}")
  })

  // Check if string is any of the given options
  protected def validateOption(options: String*): ValidateFunction = returnFalseOnNullOrException((str) => {
    options != null && options.contains(str)
  })

  // Check if int value greater than given number
  protected def validateIntGreaterThan(num: Int): ValidateFunction = returnFalseOnNullOrException((number) => {
    number.asInstanceOf[Int] > num
  })

  /**
    * Validate data row.
    *
    * @param row        data row
    * @param validators validators mapping for columns
    * @return valid or not
    */
  protected def validateColumns(row: Row)(implicit validators: Map[String, ValidateFunction]): Boolean = {
    row != null && columns.zipWithIndex.foldLeft[Boolean](true)((res, col) => {
      res && (validators.get(col._1) match {
        case None => true
        case Some(validator) => validator.apply(row.getString(col._2))
      })
    })
  }
}
