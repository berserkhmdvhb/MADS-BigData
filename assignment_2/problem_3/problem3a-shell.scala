// Reduce some of the debugging output of Spark
import org.apache.log4j.Logger
import org.apache.log4j.Level

Logger.getLogger("org").setLevel(Level.ERROR)
Logger.getLogger("akka").setLevel(Level.ERROR)

// Import the basic recommender libraries from Spark's MLlib package
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation._
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

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


////////////////////////////////////////////////////////////////////////////////////////////////////////
// Start of subtask (a)
////////////////////////////////////////////////////////////////////////////////////////////////////////
// (i)
////////////////////////////////////////////////////////////////////////////////////////////////////////
// take only those that have listened to at least 100 distinct artists
val userDistinctArtistsCounts = trainData.map(line => (line.user, line.product)).countByKey()
val trainData100 = trainData.filter{
    line => 100 <= (userDistinctArtistsCounts.get(line.user).get)
}
trainData100.cache()
// check if it is the correct amount
trainData100.map(line => (line.user, line.product)).reduceByKey(_ + _).distinct().count()

////////////////////////////////////////////////////////////////////////////////////////////////////////
// (ii)
////////////////////////////////////////////////////////////////////////////////////////////////////////

// first put random 90% of listened to artists for every user into the training set
// other 10% go into the test set

// get a list of the qualifying users
val users100 = trainData100.map{
  case Rating(user, product, rating) => user
}.distinct().collect()

users100.size // check how many we've got

val ratingsByUser = trainData100.map(rat => (rat.user, rat)).groupByKey()

val splits = ratingsByUser.map(x => x._2.splitAt((x._2.size*0.9).toInt))

val trainData90 = splits.flatMap( x=> x._1)
val testData10 = splits.flatMap( x=> x._2)

trainData90.cache
testData10.cache

// train the model
val model = ALS.trainImplicit(trainData90, 10, 5, 0.01, 1.0)


////////////////////////////////////////////////////////////////////////////////////////////////////////
// (iii)
////////////////////////////////////////////////////////////////////////////////////////////////////////

// 100 random users from testData10
val someUsers = testData10.map(x => x.user).distinct.takeSample(false,100,10) // take 100 random users, no replacement, 10 is the seed

// provided function to get the actual listened to artists for a user
def actualArtistsForUser(someUser: Int): Set[Int] = {
  val rawArtistsForUser = rawUserArtistData.map(_.split(' ')).
    filter { case Array(user,_,_) => user.toInt == someUser }
  rawArtistsForUser.map(x => x(1).toInt).collect().toSet
}

// function to compute a baseline to compare the model to
// returns the same most listened to artists to each user
// see RunRecommender-shell.scala
val artistsTotalCount = trainData.map(r => (r.product, r.rating)).reduceByKey(_ + _).collect().sortBy(-_._2)
def predictMostPopular(user: Int, numArtists: Int) = { 
  val topArtists = artistsTotalCount.take(numArtists)
  topArtists.map{case (artist, rating) => Rating(user, artist, rating)}
}

val AUCbyUser = someUsers.map{
    currUser => 
        val actualArtists = actualArtistsForUser(currUser)
        val recs = model.recommendProducts(currUser, 25) // recommend 25 top artists for this user

        val predictionAndLabels = recs.map {
            case Rating(user, artist, rating) =>
            if (actualArtists.contains(artist)){
                (rating, 1.0)
            } else {
                (rating, 0.0)
                }
        }

        val metrics = new BinaryClassificationMetrics(sc.parallelize(predictionAndLabels))
        val AUC_model = metrics.areaUnderROC

        // also compute baseline for comparison
        val recs_BL = predictMostPopular(currUser, 25)
        val predictionAndLabels_BL = recs_BL.map {
            case Rating(user, artist, rating) =>
            if (actualArtists.contains(artist)) {
                (rating, 1.0)
            } else {
                (rating, 0.0)
            }
        }

    val metrics_BL = new BinaryClassificationMetrics(sc.parallelize(predictionAndLabels_BL))
    val AUC_BL = metrics_BL.areaUnderROC
    
    (AUC_model, AUC_BL)
}

val meanAUC_model = AUCbyUser.map(tup => tup._1).sum / someUsers.size
val meanAUC_BL = AUCbyUser.map(tup => tup._2).sum / someUsers.size


println("ALS-Recommender AUC: " + (meanAUC_model))
println("Most-Popular AUC:    " + (meanAUC_BL))

val stop = System.nanoTime()
val diff_in_s = (stop - start) / 1e9d
val secs = (diff_in_s % 60).toInt
val mins = ((diff_in_s / 60.0) % 60).toInt
Console.println(s"Elapsed time: ${mins} min ${secs} s") 

