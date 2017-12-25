package com.github.lyon1982.BigDataExercise.builder.columngen

import com.github.lyon1982.BigDataExercise.utils.DateTimeTools
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Dataset, Row}

/**
  * Add booking stay interval column.
  *
  * @param bookingDateColumnName booking date column name
  * @param stayDateColumnName    stay date column name
  * @param intervalColumnName    interval column name
  * @param replace               whether replace the original column
  */
class BookingStayIntervalColumnGenerator(val bookingDateColumnName: String = "BookingDate", val stayDateColumnName: String = "StayDate", val intervalColumnName: String = "BookingStayInterval", replace: Boolean = false) extends ColumnGenerator {

  // Age calculation function
  private val calculateInterval = udf((bookingDate: String, stayDate: String) => {
    DateTimeTools.calculateDaysBetweenShortDate(bookingDate, stayDate)
  })

  /**
    * Add interval column to dataframe.
    *
    * @param df dataframe with booking date and stay date
    * @return dataframe with new interval column
    */
  override def generate(df: Dataset[Row]): Dataset[Row] = {
    val withNewColumn = df.withColumn(intervalColumnName, calculateInterval(df(bookingDateColumnName), df(stayDateColumnName)))
    if (replace) withNewColumn.drop(bookingDateColumnName, stayDateColumnName) else withNewColumn
  }
}
