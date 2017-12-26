package com.github.lyon1982.BigDataExercise.jobs

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

class HotelBookingAggregationJob {

  def run(bookings: Dataset[Row], hotels: Dataset[Row])(implicit spark: SparkSession): (Dataset[Row], Dataset[Row]) = {
    import spark.implicits._

    val hotelGroupedBookings = bookings
      .groupBy("HotelId")
      .agg(count("HotelId").as("count"), sum("BookingStayInterval").as("BookingStayInterval"), sum("StayDuration").as("StayDuration"))

    // Persist for future calculation
    // TODO: Repartition based on the cluster and actual data size
    val hotelBookings = hotelGroupedBookings
      .join(hotels, hotelGroupedBookings("HotelId") === hotels("HotelId"), "inner")
      .persist(StorageLevel.MEMORY_ONLY_SER)

    // BookingStayInterval and StayDuration Summary by Hotel Country
    val countryGroupedSummary = hotelBookings
      .groupBy($"Country".as("HotelCountry"))
      .agg(sum("count").as("Count"), sum("BookingStayInterval").as("BookingStayInterval"), sum("StayDuration").as("StayDuration"))

    // BookingStayInterval and StayDuration Summary by Hotel City
    val cityGroupedSummary = hotelBookings
      .groupBy($"City".as("HotelCity"))
      .agg(sum("count").as("Count"), sum("BookingStayInterval").as("BookingStayInterval"), sum("StayDuration").as("StayDuration"))

    // release resources
    hotelBookings.unpersist()

    (countryGroupedSummary, cityGroupedSummary)
  }

}
