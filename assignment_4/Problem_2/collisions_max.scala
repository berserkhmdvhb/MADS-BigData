// Problem 2: Analyzing Traffic Safety Data

// This is the my attempt to solve exercise 2 form the Exercise sheet 4

// spark-shell --jars /home/maxsinner/Documents/Documents/MADS_21_22/sem2/Big_Data_Analytics/esri-geometry-api-1.0.jar/


// Imports

import com.esri.core.geometry.{ GeometryEngine, SpatialReference, Geometry, Point, Line, OperatorBuffer, Point2D, OperatorIntersects, OperatorDistance, Polyline }
import com.github.nscala_time.time.Imports.{ DateTime, Duration } //

import GeoJsonProtocol._ //

import java.text.SimpleDateFormat

import org.apache.spark.{ HashPartitioner, Partitioner }
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.util.StatCounter

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import spray.json._ //
import scala.math._
import RichGeometry._ //

import java.lang.Math //
import scala.collection.mutable.ListBuffer //

// 2 a)
    case class Collision(
      date:  org.joda.time.LocalDate,
      time: org.joda.time.LocalTime,
      dateTime: org.joda.time.DateTime,
      borough: String,
      latitude: Double,
      longitude: Double,
      location: Point,
      onStreetName: String,
      crossStreetName: String,
      offStreetName: String,
      numberOfPersonsInjured: Int,
      numberOfPersonsKilled: Int,
      contributingFactorVehicle1: String,
      contributingFactorVehicle2: String,
      vehicleTypeCode1: String,
      vehicleTypeCode2: String)

    def parseCollision(line: String): Collision = {
      val fields = line.split(',')
      val formatterDate = new SimpleDateFormat("MM/dd/yyyy")
      val date = new org.joda.time.LocalDate(formatterDate.parse(fields(0)))
      val formatterTime = new SimpleDateFormat("HH:mm")
      val time = new org.joda.time.LocalTime(formatterTime.parse(fields(1)))
      val formatterDateTime = new SimpleDateFormat("MM/dd/yyyy HH:mm")
      val dateTime = new org.joda.time.DateTime(formatterDateTime.parse(fields(0) + " " +  fields(1)))
      val borough = fields(2)
      val latitude = fields(4).toDouble
      val longitude = fields(5).toDouble
      val location = new Point(longitude, latitude)
      val onStreetName = fields(8).trim()
      val crossStreetName = fields(9).trim()
      val offStreetName = fields(10)
      var numberOfPersonsInjured = 0
      if (!fields(11).isEmpty) {
        numberOfPersonsInjured = fields(11).toInt
      }
      var numberOfPersonsKilled = 0
      if (!fields(12).isEmpty) {
        numberOfPersonsKilled = fields(12).toInt
      }
      val contributingFactorVehicle1 = fields(19)
      val contributingFactorVehicle2 = fields(20)
      val vehicleTypeCode1 = fields(25)
      var vehicleTypeCode2 = ""
      if (fields.length >= 27) {
        vehicleTypeCode2 = fields(26)
      }
      Collision(date, time, dateTime, borough, latitude, longitude,
         location, onStreetName, crossStreetName, offStreetName,
         numberOfPersonsInjured, numberOfPersonsKilled, contributingFactorVehicle1,
         contributingFactorVehicle2, vehicleTypeCode1, vehicleTypeCode2)
    }
    val collisionsRaw = sc.textFile("/home/maxsinner/Documents/Documents/MADS_21_22/sem2/Big_Data_Analytics/NYPD_Motor_Vehicle_Collisions.csv").sample(false, 1.0) //
    val collisionsParsed = collisionsRaw.filter(line => !line.startsWith("DATE") && !line.split(',')(4).isEmpty).map(parseCollision)
    val collisions = collisionsParsed.filter(coll => !coll.onStreetName.isEmpty && !coll.crossStreetName.isEmpty).cache()
    collisions.take(5).foreach(println)
    // b)
    val crossings = collisions.map(coll => ((coll.borough, coll.onStreetName, coll.crossStreetName), coll.numberOfPersonsInjured + coll.numberOfPersonsKilled)).reduceByKey(_ + _)
    crossings.sortBy(_._2, false).take(5).foreach(println)
    val factors1 = collisions.map(coll => ((coll.borough, coll.onStreetName, coll.crossStreetName, coll.contributingFactorVehicle1), 1))//.filter(x => !x._1._3.isEmpty)
    val factors2 = collisions.map(coll => ((coll.borough, coll.onStreetName, coll.crossStreetName, coll.contributingFactorVehicle2), 1))//.filter(x => !x._1._3.isEmpty)
    val factors = factors1.union(factors2)
    val factorsCount = factors.reduceByKey(_ + _)
    val factorsCountMerged = factorsCount.map(x => ((x._1._1, x._1._2, x._1._3), (x._1._3, x._2))).groupByKey()
    val factorsCountMergedTop5 = factorsCountMerged.map(x => ((x._1._1, x._1._2, x._1._3), x._2.toList.sortBy(_._2)(Ordering[Int].reverse).take(5)))
    val finalMerged = crossings.join(factorsCountMergedTop5)
    val mostDangerous = finalMerged.sortBy(_._2._1, false).take(25)
    mostDangerous.foreach(println)
    // c)
    val collisiontimes = collisions.map(coll => ((coll.date, coll.time), coll.numberOfPersonsInjured + coll.numberOfPersonsKilled)).reduceByKey(_ + _)
    collisiontimes.sortBy(_._2, false).take(5).foreach(println)
    val timefactors1 = collisions.map(coll => ((coll.date, coll.time, coll.contributingFactorVehicle1), 1))
    val timefactors2 = collisions.map(coll => ((coll.date, coll.time, coll.contributingFactorVehicle2), 1))
    val timefactors = timefactors1.union(timefactors2)
    val timefactorsCount = timefactors.reduceByKey(_ + _)
    val timefactorsCountMerged = timefactorsCount.map(x => ((x._1._1, x._1._2), (x._1._3, x._2))).groupByKey()
    val timefactorsCountMergedTop5 = timefactorsCountMerged.map(x => ((x._1._1, x._1._2), x._2.toList.sortBy(-_._2).take(5)))
    val timefinalMerged = collisiontimes.join(timefactorsCountMergedTop5)
    val timemostDangerous = timefinalMerged.sortBy(_._2._1, false).take(25)
    timemostDangerous.foreach(println)
    // d)
    val vehicles1 = collisions.map(coll => (coll.vehicleTypeCode1, coll.numberOfPersonsInjured + coll.numberOfPersonsKilled))
    val vehicles2 = collisions.map(coll => (coll.vehicleTypeCode2, coll.numberOfPersonsInjured + coll.numberOfPersonsKilled))
    val vehicles = vehicles1.union(vehicles2)
    val vehiclesCount = vehicles.reduceByKey(_ + _)
    val sortedvehiclesCount = vehiclesCount.sortBy(-_._2)
    sortedvehiclesCount.take(5).foreach(println)    
    // e) //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Injured cases
    // e b)
    val crossings_injured = collisions.map(coll => ((coll.borough, coll.onStreetName, coll.crossStreetName), coll.numberOfPersonsInjured)).reduceByKey(_ + _)
    crossings_injured.sortBy(_._2, false).take(5).foreach(println)
    val factors1_ = collisions.map(coll => ((coll.borough,coll.onStreetName, coll.crossStreetName, coll.contributingFactorVehicle1), 1))
    val factors2_ = collisions.map(coll => ((coll.borough,coll.onStreetName, coll.crossStreetName, coll.contributingFactorVehicle2), 1))
    val factors_ = factors1_.union(factors2_)
    val factorsCount_ = factors_.reduceByKey(_ + _)
    val factorsCountMerged_ = factorsCount_.map(x => ((x._1._1, x._1._2,x._1._3), (x._1._3, x._2))).groupByKey()
    val factorsCountMergedTop5_ = factorsCountMerged_.map(x => ((x._1._1, x._1._2, x._1._3), x._2.toList.sortBy(_._2)(Ordering[Int].reverse).take(5)))
    val finalMerged_injured = crossings_injured.join(factorsCountMergedTop5_)
    val mostDangerous_injured = finalMerged_injured.sortBy(_._2._1, false).take(25)
    mostDangerous_injured.foreach(println)
    // Killed cases
    // e b)
    val crossings_killed = collisions.map(coll => ((coll.borough, coll.onStreetName, coll.crossStreetName), coll.numberOfPersonsKilled)).reduceByKey(_ + _)
    crossings_killed.sortBy(_._2, false).take(5).foreach(println)
    val factors1_1 = collisions.map(coll => ((coll.borough,coll.onStreetName, coll.crossStreetName, coll.contributingFactorVehicle1), 1))
    val factors2_1 = collisions.map(coll => ((coll.borough,coll.onStreetName, coll.crossStreetName, coll.contributingFactorVehicle2), 1))
    val factors_1 = factors1_1.union(factors2_1)
    val factorsCount_1 = factors_1.reduceByKey(_ + _)
    val factorsCountMerged_1 = factorsCount.map(x => ((x._1._1, x._1._2, x._1._3), (x._1._3, x._2))).groupByKey()
    val factorsCountMergedTop5_1 = factorsCountMerged_1.map(x => ((x._1._1, x._1._2, x._1._3), x._2.toList.sortBy(_._2)(Ordering[Int].reverse).take(5)))
    val finalMerged_killed = crossings_killed.join(factorsCountMergedTop5_1)
    val mostDangerous_killed = finalMerged_killed.sortBy(_._2._1, false).take(25)
    mostDangerous_killed.foreach(println)
    // Injured cases 
    // e c)
    val collisiontimes_injured = collisions.map(coll => ((coll.date, coll.time), coll.numberOfPersonsInjured)).reduceByKey(_ + _)
    collisiontimes_injured.sortBy(_._2, false).take(5).foreach(println)
    val timefactors1 = collisions.map(coll => ((coll.date, coll.time, coll.contributingFactorVehicle1), 1))
    val timefactors2 = collisions.map(coll => ((coll.date, coll.time, coll.contributingFactorVehicle2), 1))
    val timefactors = timefactors1.union(timefactors2)
    val timefactorsCount = timefactors.reduceByKey(_ + _)
    val timefactorsCountMerged = timefactorsCount.map(x => ((x._1._1, x._1._2), (x._1._3, x._2))).groupByKey()
    val timefactorsCountMergedTop5 = timefactorsCountMerged.map(x => ((x._1._1, x._1._2), x._2.toList.sortBy(-_._2).take(5)))
    val timefinalMerged_injured = collisiontimes_injured.join(timefactorsCountMergedTop5)
    val timemostDangerous_injured = timefinalMerged_injured.sortBy(_._2._1, false).take(25)
    timemostDangerous_injured.foreach(println)
    // Killed cases
    // e c)
    val collisiontimes_killed = collisions.map(coll => ((coll.date, coll.time), coll.numberOfPersonsKilled)).reduceByKey(_ + _)
    collisiontimes_killed.sortBy(_._2, false).take(5).foreach(println)
    val timefactors1 = collisions.map(coll => ((coll.date, coll.time, coll.contributingFactorVehicle1), 1))
    val timefactors2 = collisions.map(coll => ((coll.date, coll.time, coll.contributingFactorVehicle2), 1))
    val timefactors = timefactors1.union(timefactors2)
    val timefactorsCount = timefactors.reduceByKey(_ + _)
    val timefactorsCountMerged = timefactorsCount.map(x => ((x._1._1, x._1._2), (x._1._3, x._2))).groupByKey()
    val timefactorsCountMergedTop10 = timefactorsCountMerged.map(x => ((x._1._1, x._1._2), x._2.toList.sortBy(-_._2).take(25)))
    val timefinalMerged_killed = collisiontimes_killed.join(timefactorsCountMergedTop10)
    val timemostDangerous_killed = timefinalMerged_killed.sortBy(_._2._1, false).take(25)
    timemostDangerous_killed.foreach(println)
    // Injured cases 
    // e d)
    val vehicles1_injured = collisions.map(coll => (coll.vehicleTypeCode1, coll.numberOfPersonsInjured))
    val vehicles2_injured = collisions.map(coll => (coll.vehicleTypeCode2, coll.numberOfPersonsInjured))
    val vehicles_injured = vehicles1_injured.union(vehicles2_injured)
    val vehiclesCount_injured = vehicles_injured.reduceByKey(_ + _)
    val sortedvehiclesCount_injured = vehiclesCount_injured.sortBy(-_._2)
    sortedvehiclesCount_injured.take(5).foreach(println)
    // Killed cases
    // e d)
    val vehicles1_killed = collisions.map(coll => (coll.vehicleTypeCode1, coll.numberOfPersonsKilled))
    val vehicles2_killed = collisions.map(coll => (coll.vehicleTypeCode2, coll.numberOfPersonsKilled))
    val vehicles_killed = vehicles1_killed.union(vehicles2_killed)
    val vehiclesCount_killed = vehicles_killed.reduceByKey(_ + _)
    val sortedvehiclesCount_killed = vehiclesCount_killed.sortBy(-_._2)
    sortedvehiclesCount_killed.take(5).foreach(println)



