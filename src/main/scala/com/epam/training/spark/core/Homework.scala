package com.epam.training.spark.core

import java.time.LocalDate

import com.epam.training.spark.core.domain.Climate
import com.epam.training.spark.core.domain.MaybeDoubleType.{InvalidDouble, MaybeDouble, ValidDouble}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Homework {
  val DELIMITER = ";"
  val RAW_BUDAPEST_DATA = "data/budapest_daily_1901-2010.csv"
  val OUTPUT_DUR = "output"

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("EPAM BigData training Spark Core homework")
      .setIfMissing("spark.master", "local[2]")
      .setIfMissing("spark.sql.shuffle.partitions", "10")
    val sc = new SparkContext(sparkConf)

    processData(sc)

    sc.stop()

  }

  def processData(sc: SparkContext): Unit = {

    /**
      * Task 1
      * Read raw data from provided file, remove header, split rows by delimiter
      */
    val rawData: RDD[List[String]] = getRawDataWithoutHeader(sc, Homework.RAW_BUDAPEST_DATA)

    /**
      * Task 2
      * Find errors or missing values in the data
      */
    val errors: List[Int] = findErrors(rawData)
    println(errors)

    /**
      * Task 3
      * Map raw data to Climate type
      */
    val climateRdd: RDD[Climate] = mapToClimate(rawData)

    /**
      * Task 4
      * List average temperature for a given day in every year
      */
    val averageTemeperatureRdd: RDD[Double] = averageTemperature(climateRdd, 1, 2)

    /**
      * Task 5
      * Predict temperature based on mean temperature for every year including 1 day before and after
      * For the given month 1 and day 2 (2nd January) include days 1st January and 3rd January in the calculation
      */
    val predictedTemperature: Double = predictTemperature(climateRdd, 1, 2)
    println(s"Predicted temperature: $predictedTemperature")

  }

  def getRawDataWithoutHeader(sc: SparkContext, rawDataPath: String): RDD[List[String]] = {
    sc.textFile(rawDataPath).mapPartitionsWithIndex((idx, iter) => if (idx==0) iter.drop(1) else iter).map(line => line.split(DELIMITER, -1).toList)
  }

  def findErrors(rawData: RDD[List[String]]): List[Int] = {
    rawData.map(dataLine => dataLine.map(data => if (data.isEmpty) 1 else 0)).reduce((errors1, errors2) => (errors1 zip errors2).map{ case (error1, error2) => error1 + error2})
  }

  def mapToClimate(rawData: RDD[List[String]]): RDD[Climate] = {
    rawData.map(data => Climate(data(0), data(1), data(2), data(3), data(4), data(5), data(6)))
  }

  def averageTemperature(climateData: RDD[Climate], month: Int, dayOfMonth: Int): RDD[Double] = {
    climateData.filter(climate => climate.observationDate.getMonthValue == month && climate.observationDate.getDayOfMonth == dayOfMonth).map(climate => climate.meanTemperature.value)
  }

  def predictTemperature(climateData: RDD[Climate], month: Int, dayOfMonth: Int): Double = {
    val (sumTemprates, numMeasurements) = climateData.filter(isTemperateValid)
      .filter(climate => isOnSameOrAdjacentCalendarDate(climate.observationDate, month, dayOfMonth))
      .map(climate => (climate.meanTemperature.value, 1d)).reduce((a,b) => (a._1 + b._1 , a._2 + b._2))
    sumTemprates / numMeasurements
  }

  private def isTemperateValid(climate: Climate) = {
    climate.meanTemperature.value != Double.NaN
  }

  private def isOnSameOrAdjacentCalendarDate(date: LocalDate, month: Int, dayOfMonth: Int): Boolean = {
    isOnCalendarDay(date, month, dayOfMonth) || isOnCalendarDay(date.plusDays(1), month, dayOfMonth) || isOnCalendarDay(date.minusDays(1), month, dayOfMonth)
  }

  private[this] def isOnCalendarDay(date: LocalDate, month: Int, dayOfMonth: Int): Boolean = {
    date.getMonthValue == month && date.getDayOfMonth == dayOfMonth
  }

}


