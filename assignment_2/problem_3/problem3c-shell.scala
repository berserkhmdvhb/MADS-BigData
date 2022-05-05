// Reduce some of the debugging output of Spark
import org.apache.log4j.Logger
import org.apache.log4j.Level

Logger.getLogger("org").setLevel(Level.ERROR)
Logger.getLogger("akka").setLevel(Level.ERROR)

// Import the basic recommender libraries from Spark's MLlib package
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation._

val start = System.nanoTime

// Input
val sampleSize = 1 // 1 for full dataset, smaller percentage for debugging
val rawArtistAlias = sc.textFile("../Data/audioscrobbler/artist_alias.txt")
val rawArtistData = sc.textFile("../Data/audioscrobbler/artist_data.txt")
val rawUserArtistData = sc.textFile("../Data/audioscrobbler/user_artist_data.txt").sample(false, sampleSize)

// RDD of artist/id pairs
val artistByID = rawArtistData.flatMap { 
  line => val (id, name) = line.span(_ != '\t')
  if (name.isEmpty) { 
    None 
  } else { 
    try { 
      Some((id.toInt, name.trim)) } 
    catch { case e: NumberFormatException => None } 
  } 
} 

// resolve artist aliases
val artistAlias = rawArtistAlias.flatMap { line => 
  val tokens = line.split('\t') 
  if (tokens(0).isEmpty) { 
    None 
  } else {
    Some((tokens(0).toInt, tokens(1).toInt)) 
  } 
}.collectAsMap()

// Broadcast the local aliases map since it is going to be part of the closure of our training function
val bArtistAlias = sc.broadcast(artistAlias)

// Prepare and cache the training data
val trainData = rawUserArtistData.map { 
  line => val Array(userID, artistID, count) = 
    line.split(' ').map(_.toInt) 
    val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID) 
    Rating(userID, finalArtistID, count) 
}.cache()


///////////////////////////////////////////////////////////////////////////////////////////////
// Start of the subtask (c)
///////////////////////////////////////////////////////////////////////////////////////////////
// chosen artists:
// 10273820	Ne Obliviscaris
// 1010227	Amon Amarth
// 6825344	Machine Head
// 2121555	Volbeat
// 10026633	Rammstein
// 7029024	Cynic
// 1283438	Pink Floyd
// 1212435	Gojira
// 1016114	Insomnium
// 2152973	Agalloch

val new_artists = Array(10273820,1010227,6825344,2121555,10026633,7029024,1283438,1212435,1016114,2152973)

// create a new user
// ctrl+f told me this was an unused ID
val newUserID = 1111119

// use random number generator of scala for the ratings
// anything between 50 and 200
// this user always listens to the same stuff, heh
import scala.util.Random
val r = scala.util.Random
r.setSeed(1000L)

val new_ratings = for (i <- 1 to 10) yield {
  val rat = 50 + r.nextInt(150)
  rat
}

// build the ratings from all of the above
val ArrayNewRatings = for (i <- 0 to 9) yield {
  val finalArtID = bArtistAlias.value.getOrElse(new_artists(i),new_artists(i))
  Rating(newUserID, finalArtID, new_ratings(i))
}

// make it an RDD
val finalAddedRatings = sc.parallelize(ArrayNewRatings)

// add these ratings to the trainData
val newTrainData = trainData.union(finalAddedRatings)
newTrainData.cache()


// train the model
// use the best hyperparameters we got in (b)
val rank = 25
val lambda = 0.01
val alpha = 1.0
val newModel = ALS.trainImplicit(newTrainData, rank, 5, lambda, alpha) 


// get recommendations
val newRecommendations = newModel.recommendProducts(newUserID, 25)
newRecommendations.foreach{
    rating =>
        val artistID = rating.product
        println(artistByID.lookup(artistID).head)
}
// manually judged afterwards


val stop = System.nanoTime()
val diff_in_s = (stop - start) / 1e9d
val secs = (diff_in_s % 60).toInt
val mins = ((diff_in_s / 60.0) % 60).toInt
Console.println(s"Elapsed time: ${mins} min ${secs} s") 

