package com.github.lyon1982.BigDataExercise

import com.github.lyon1982.BigDataExercise.builder.DataFrameBuilder
import com.github.lyon1982.BigDataExercise.builder.columngen.{BookingStayIntervalColumnGenerator, CustomerAgeColumnGenerator}
import com.github.lyon1982.BigDataExercise.builder.reader.CSVDataFrameReader
import com.github.lyon1982.BigDataExercise.builder.validator.{BookingValidator, CustomerValidator, HotelValidator}
import com.github.lyon1982.BigDataExercise.jobs.CustomerBookingAggregationJob
import com.github.lyon1982.BigDataExercise.jobs.HotelBookingAggregationJob
import com.github.lyon1982.BigDataExercise.model.schema.{CustomerSchema, HotelBookingSchema, HotelSchema}
import org.apache.spark.sql.SparkSession

object BigDataExerciseApp {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder.appName("Big Data Exercise Application").getOrCreate()

    val bookings = new DataFrameBuilder()
      .addNeededColumns("ClientId", "HotelId", "BookingDate", "StayDate", "StayDuration")
      .setDataValidator(new BookingValidator())
      .appendCustomColumn(new BookingStayIntervalColumnGenerator(replace = true))
      .create(new CSVDataFrameReader(path = "/vagrant_app/data/hotel_bookings.csv", schema = HotelBookingSchema.schema))

    val customers = new DataFrameBuilder()
      .addNeededColumns("ClientID", "BirthDate", "Gender")
      .setDataValidator(new CustomerValidator())
      .appendCustomColumn(new CustomerAgeColumnGenerator(replace = true))
      .create(new CSVDataFrameReader(path = "/vagrant_app/data/customers.csv", schema = CustomerSchema.schema))

    val hotels = new DataFrameBuilder()
      .addNeededColumns("HotelId", "City", "Country")
      .setDataValidator(new HotelValidator())
      .appendCustomColumn(new CustomerAgeColumnGenerator())
      .create(new CSVDataFrameReader(path = "/vagrant_app/data/hotels.csv", schema = HotelSchema.schema))

    val (genderGroupedSummary, ageGroupedSummary) = new CustomerBookingAggregationJob().run(bookings, customers)

    val (countryGroupedSummary, cityGroupedSummary) = new HotelBookingAggregationJob().run(bookings, hotels)

    // Dataset 1: A dataset containing the interval between booking and stay date per customer gender, age and hotel country
    val dataset1 = genderGroupedSummary.union(ageGroupedSummary).union(countryGroupedSummary).drop("StayDuration")

    // Dataset 2: A dataset containing the stay length (duration between stay start and stay end) by city and country
    val dataset2 = cityGroupedSummary.union(countryGroupedSummary).drop("BookingStayInterval")

    // Dataset 3: A dataset containing the stay length by age and gender
    val dataset3 = ageGroupedSummary.union(genderGroupedSummary).drop("BookingStayInterval")

    spark.stop()
  }

}
