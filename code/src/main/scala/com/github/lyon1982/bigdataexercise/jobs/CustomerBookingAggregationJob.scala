package com.github.lyon1982.BigDataExercise.jobs

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

class CustomerBookingAggregationJob {

  def run(bookings: Dataset[Row], customers: Dataset[Row])(implicit spark: SparkSession): (Dataset[Row], Dataset[Row]) = {
    import spark.implicits._

    val clientGroupedBookings = bookings
      .groupBy("ClientId")
      .agg(count("ClientId").as("count"), sum("BookingStayInterval").as("BookingStayInterval"), sum("StayDuration").as("StayDuration"))

    // Persist for future calculation
    // TODO: Repartition based on the cluster and actual data size
    val customerBookings = clientGroupedBookings
      .join(customers, $"ClientId", "inner")
      .persist(StorageLevel.MEMORY_ONLY_SER)

    // BookingStayInterval and StayDuration Summary by Customer Gender
    val genderGroupedSummary = customerBookings
      .groupBy($"Gender".as("CustomerGender"))
      .agg(sum("count").as("count"), sum("BookingStayInterval").as("BookingStayInterval"), sum("StayDuration").as("StayDuration"))

    // BookingStayInterval and StayDuration Summary by Customer Age
    val ageGroupedSummary = customerBookings
      .groupBy($"Age".as("CustomerAge"))
      .agg(sum("count").as("count"), sum("BookingStayInterval").as("BookingStayInterval"), sum("StayDuration").as("StayDuration"))

    // release resources
    customerBookings.unpersist()

    (genderGroupedSummary, ageGroupedSummary)
  }

}
