import com.esri.core.geometry.{ GeometryEngine, SpatialReference, Geometry, Point }
import com.github.nscala_time.time.Imports.{ DateTime, Duration }

import GeoJsonProtocol._ // this contains our custom GeoJson types

import java.text.SimpleDateFormat

import org.apache.spark.{ HashPartitioner, Partitioner }
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.util.StatCounter

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import spray.json._

val conf = new SparkConf()
  .setMaster("local")
  .setAppName("RunTaxiTrips")

sc.setLogLevel("ERROR")

val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

def point(longitude: String, latitude: String): Point = {
  new Point(longitude.toDouble, latitude.toDouble)
}

case class TaxiTrip (
                      pickupTime:  org.joda.time.DateTime,
                      dropoffTime: org.joda.time.DateTime,
                      pickupLoc:   com.esri.core.geometry.Point,
                      dropoffLoc:  com.esri.core.geometry.Point) extends java.io.Serializable

def parse(line: String): (String, TaxiTrip) = {
  val fields = line.split(',')
  val license = fields(1)
  val pickupTime = new org.joda.time.DateTime(formatter.parse(fields(5)))
  val dropoffTime = new org.joda.time.DateTime(formatter.parse(fields(6)))
  val pickupLoc = point(fields(10), fields(11))
  val dropoffLoc = point(fields(12), fields(13))
  val trip = TaxiTrip(pickupTime, dropoffTime, pickupLoc, dropoffLoc)
  (license, trip)
}

def safe[S, T](f: S => T): S => Either[T, (S, Exception)] = {
  new Function[S, Either[T, (S, Exception)]] with Serializable {
    def apply(s: S): Either[T, (S, Exception)] = {
      try {
        Left(f(s))
      } catch {
        case e: Exception => Right((s, e))
      }
    }
  }
}

//----------------- Parse & Filter the Taxi Trips -------------------------

val taxiRaw = sc.textFile("/home/hamed/Documents/datasets/geo/nyc-taxi-trips").sample(false, 0.01) // use 1 percent sample size for debugging!
val taxiParsed = taxiRaw.map(safe(parse))
taxiParsed.cache()

val taxiBad = taxiParsed.collect({
  case t if t.isRight => t.right.get
})

val taxiGood = taxiParsed.collect({
  case t if t.isLeft => t.left.get
})
taxiGood.cache() // cache good lines for later re-use

println("\n" + taxiGood.count() + " taxi trips parsed.")
println(taxiBad.count() + " taxi trips dropped.")

def getHours(trip: TaxiTrip): Long = {
  val d = new Duration(
    trip.pickupTime,
    trip.dropoffTime)
  d.getStandardHours
}



println("\nDistribution of trip durations in hours:")
taxiGood.values.map(getHours).countByValue().
  toList.sorted.foreach(println)

val taxiClean = taxiGood.filter {
  case (lic, trip) =>
    val hrs = getHours(trip)
    0 <= hrs && hrs < 3
}

val taxiDone = taxiClean.filter {
  case (lic, trip) =>
    val zero = new Point(0.0, 0.0)
    !(zero.equals(trip.pickupLoc) || zero.equals(trip.dropoffLoc))
}
taxiDone.cache()

//----------------- Parse the NYC Boroughs Polygons -----------------------

val geojson = scala.io.Source.
  fromFile("/home/hamed/Documents/datasets/geo/nyc-borough-boundaries-polygon.geojson").mkString

val features = geojson.parseJson.convertTo[FeatureCollection]

// look up the borough for some test point
val p = new Point(-73.994499, 40.75066)
val b = features.find(f => f.geometry.contains(p))

val areaSortedFeatures = features.sortBy(f => {
  val borough = f("boroughCode").convertTo[Int]
  (borough, -f.geometry.area2D())
})

val bFeatures = sc.broadcast(areaSortedFeatures)

def borough(trip: TaxiTrip): Option[String] = {
  val feature: Option[Feature] = bFeatures.value.find(f => {
    f.geometry.contains(trip.dropoffLoc)
  })
  feature.map(f => {
    f("borough").convertTo[String]
  })
}


def boroughpickup(trip: TaxiTrip): Option[String] = {
  val feature: Option[Feature] = bFeatures.value.find(f => {
    f.geometry.contains(trip.pickupLoc)
  })
  feature.map(f => {
    f("borough").convertTo[String]
  })
}
println("\nDistribution of trips per borough:")
taxiClean.values.map(borough).countByValue().foreach(println)




//--------------------------------------------------------------------------//



//subtask (a) (b)

// Trips that have different locations for pickupLoc and dropoffLoc values of that trip
val diffLocs = taxiDone.filter{case (lic, trip) => !trip.pickupLoc.equals(trip.dropoffLoc)}

// Trips that have the same location for pickupLoc and dropoffLoc values of that trip:
val eqLocs = taxiDone.filter{case (lic, trip) => trip.pickupLoc.equals(trip.dropoffLoc)}


taxiDone.count()
diffLocs.count()
eqLocs.count()

diffLocs.cache()
eqLocs.cache()

// Store durations of both types of trips
val diffTripDurations = diffLocs.values.map(getHours)
val eqTripDurations = eqLocs.values.map(getHours)

// compute count, summation, and average duration of trips (both for same and different locations)

diffTripDurations.count
diffTripDurations.sum
diffTripDurations.mean


eqTripDurations.count
eqTripDurations.sum
eqTripDurations.mean

// subtask (c) (d)

//------- By Boroughs-----//
val durationBoroughsDiff: RDD[(Option[String], Long)] =
  diffLocs.map{case (lic, trips) => (borough(trips), getHours(trips))}

val durationBoroughsEq: RDD[(Option[String], Long)] =
  eqLocs.map{case (lic, trips) => (borough(trips), getHours(trips))}

durationBoroughsDiff.cache()
durationBoroughsEq.cache()
//boroughHoursDiff.groupBy(x => x._2).map(x => x._1.count)

// Report statistics about hours grouped by boroughs (for trips with different boroughs)

// count
durationBoroughsDiff.groupByKey().count()

// sum
val sumByBoroughsDiff = durationBoroughsDiff.reduceByKey((x, y) => x + y)
sumByBoroughsDiff.collect().foreach(println)


// average
val totalsByBoroughsDiff = durationBoroughsDiff.mapValues(x => (x, 1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
val avgByBoroughsDiff = totalsByBoroughsDiff.mapValues(x => if (x._2 == 0) 0 else (x._1.toFloat / x._2))
avgByBoroughsDiff.collect().foreach(println)

// Report statistics about hours grouped by boroughs (for trips with identical boroughs)

// count
durationBoroughsEq.groupByKey().count()

// sum
val sumByBoroughsEq = durationBoroughsEq.reduceByKey((x, y) => x + y)
sumByBoroughsEq.collect().foreach(println)


// average
val totalsByBoroughsEq = durationBoroughsEq.mapValues(x => (x, 1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
val avgByBoroughsEq = totalsByBoroughsEq.mapValues(x => if (x._2 == 0) 0 else (x._1.toFloat / x._2))
avgByBoroughsEq.collect().foreach(println)









//------- By Days-----//


val durationDaysDiff: RDD[(Int, Long)] =
  diffLocs.map{case (lic, trips) => (trips.pickupTime.getDayOfWeek, getHours(trips))}

val durationDaysEq: RDD[(Int, Long)] =
  eqLocs.map{case (lic, trips) => (trips.pickupTime.getDayOfWeek, getHours(trips))}


durationDaysDiff.cache()
durationDaysEq.cache()

// Report statistics about hours grouped by days of the week (for trips with different boroughs)

// count
durationDaysDiff.groupByKey().count()

// sum
val sumByDaysDiff = durationDaysDiff.reduceByKey((x, y) => x + y)
sumByDaysDiff.collect().foreach(println)


// average
val totalsByDaysDiff = durationDaysDiff.mapValues(x => (x, 1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
val avgByDaysDiff = totalsByDaysDiff.mapValues(x => if (x._2 == 0) 0 else (x._1.toFloat / x._2))
avgByDaysDiff.collect().foreach(println)

// Report statistics about hours grouped by days of the week (for trips with identical boroughs)

// count
durationDaysEq.groupByKey().count()

// sum
val sumByDaysEq = durationDaysEq.reduceByKey((x, y) => x + y)
sumByDaysEq.collect().foreach(println)


// average
val totalsByDaysEq = durationDaysEq.mapValues(x => (x, 1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
val avgByDaysEq = totalsByDaysEq.mapValues(x => if (x._2 == 0) 0 else (x._1.toFloat / x._2))
avgByDaysEq.collect().foreach(println)





// subtask (e)

//----------------- Helper Classes for "Sessionization" -------------------

class FirstKeyPartitioner[K1, K2](partitions: Int) extends org.apache.spark.Partitioner {
  val delegate = new org.apache.spark.HashPartitioner(partitions)
  override def numPartitions: Int = delegate.numPartitions
  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[(K1, K2)]
    delegate.getPartition(k._1)
  }
}

def secondaryKey(trip: TaxiTrip) = trip.pickupTime.getMillis

def split(t1: TaxiTrip, t2: TaxiTrip): Boolean = {
  val p1 = t1.pickupTime
  val p2 = t2.pickupTime
  val d = new Duration(p1, p2)
  d.getStandardHours >= 4
}

def groupSorted[K, V, S](
                          it:        Iterator[((K, S), V)],
                          splitFunc: (V, V) => Boolean): Iterator[(K, List[V])] = {
  val res = List[(K, ArrayBuffer[V])]()
  it.foldLeft(res)((list, next) => list match {
    case Nil =>
      val ((lic, _), trip) = next
      List((lic, ArrayBuffer(trip)))
    case cur :: rest =>
      val (curLic, trips) = cur
      val ((lic, _), trip) = next
      if (!lic.equals(curLic) || splitFunc(trips.last, trip)) {
        (lic, ArrayBuffer(trip)) :: list
      } else {
        trips.append(trip)
        list
      }
  }).map { case (lic, buf) => (lic, buf.toList) }.iterator
}

def groupByKeyAndSortValues[K: Ordering: ClassTag, V: ClassTag, S: Ordering](
                                                                              rdd:              RDD[(K, V)],
                                                                              secondaryKeyFunc: V => S,
                                                                              splitFunc:        (V, V) => Boolean,
                                                                              numPartitions:    Int): RDD[(K, List[V])] = {
  val presess = rdd.map {
    case (lic, trip) => ((lic, secondaryKeyFunc(trip)), trip)
  }
  val partitioner = new FirstKeyPartitioner[K, S](numPartitions)
  presess.repartitionAndSortWithinPartitions(partitioner).mapPartitions(groupSorted(_, splitFunc))
}

val sessions = groupByKeyAndSortValues(taxiDone, secondaryKey, split, 30) // use fixed amount of 30 partitions
sessions.cache()

println("\nSome sample sessions:")
sessions.take(5).foreach(println)

//----------------- Final Analysis of the Trip Durations ------------------


// fetch both borough and dayhour for all pairs of trips
def hourBoroughDuration(t1: TaxiTrip, t2: TaxiTrip) = {
  val b = borough(t1)
  val h = t1.pickupTime.getHourOfDay
  val d = new Duration(t1.dropoffTime, t2.pickupTime)
  ((b, h), d)
}



val hourBoroughDurations: RDD[((Option[String], Int), Duration)] =
  sessions.values.flatMap(trips => {
    val iter: Iterator[Seq[TaxiTrip]] = trips.sliding(2)
    val viter = iter.filter(_.size == 2)
    viter.map(p => hourBoroughDuration(p(0), p(1)))
  }).cache()


println("\nDistribution of wait-times in hours:")
hourBoroughDurations.map{case ((b, h), d) => d}.map(_.getStandardHours).countByValue().toList.
  sorted.foreach(println)



println("\nFinal stats of wait-times per borough and per dayhour:")
hourBoroughDurations.filter {
  case ((b, h), d) => d.getMillis >= 0
}.mapValues(d => {
  val s = new StatCounter()
  s.merge(d.getStandardSeconds)
}).
  reduceByKey((a, b) => a.merge(b)).collect().foreach(println)



println("\nFinal stats of wait-times per borough:")
hourBoroughDurations.filter {
  case (b, d) => d.getMillis >= 0
}.mapValues(d => {
  val s = new StatCounter()
  s.merge(d.getStandardSeconds)
}).
  reduceByKey((a, b) => a.merge(b)).collect().foreach(println)



// subtask (f)

// Implementing Haversine Distance
case class Location(lat: Double, lon: Double)

trait DistanceCalcular {
  def calculateDistanceInKilometer(userLocation: Location, warehouseLocation: Location): Int
}

class DistanceCalculatorImpl extends DistanceCalcular {

  private val AVERAGE_RADIUS_OF_EARTH_KM = 6371

  override def calculateDistanceInKilometer(userLocation: Location, warehouseLocation: Location): Int = {
    val latDistance = Math.toRadians(userLocation.lat - warehouseLocation.lat)
    val lngDistance = Math.toRadians(userLocation.lon - warehouseLocation.lon)
    val sinLat = Math.sin(latDistance / 2)
    val sinLng = Math.sin(lngDistance / 2)
    val a = sinLat * sinLat +
      (Math.cos(Math.toRadians(userLocation.lat)) *
        Math.cos(Math.toRadians(warehouseLocation.lat)) *
        sinLng * sinLng)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    (AVERAGE_RADIUS_OF_EARTH_KM * c).toInt
  }
}

//new DistanceCalculatorImpl().calculateDistanceInKilometer(Location(10, 20), Location(40, 20))



// Akin to getHours, but get minutes instead of hours
def getMins(trip: TaxiTrip): Long = {
  val d = new Duration(
    trip.pickupTime,
    trip.dropoffTime)
  d.getStandardMinutes
}

// In this map we fetch (X,Y) coordinates of both pickupLoc and dropoffLoc so as to
// calculate Haversine distance between the locations and store them as "dist"
// Then, we normalize the durations (in time unit) by the distance (in space unit)
// We map the taxiDone to (dayhour, Normalized Duration)
// Finally, we store the result in durationDayhoursNorm
val durationDayhoursNorm: RDD[(Int, Float)] =
  taxiDone.map{case (lic, trips) =>
    val X1 = trips.pickupLoc.getX
    val Y1 = trips.pickupLoc.getY
    val X2 = trips.dropoffLoc.getX
    val Y2 = trips.dropoffLoc.getY
    val dist = new DistanceCalculatorImpl().calculateDistanceInKilometer(Location(X1,Y1), Location(X2, Y2))
    val durationNorm = if (dist == 0) getMins(trips) else (getMins(trips).toFloat / dist)
    //val durationNorm = getMins(trips) / dist
    (trips.pickupTime.getHourOfDay, durationNorm)}


// average
// Now we compute the average of trips grouped by dayhour
val totalsByDayhours = durationDayhoursNorm.mapValues(x => (x, 1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
val avgByDayhours = totalsByDayhours.mapValues(x => if (x._2 == 0) 0 else (x._1.toFloat / x._2))

// At the end, we sort the averages by dayhours in descending order
avgByDayhours.sortByKey(false).collect().foreach(println)
