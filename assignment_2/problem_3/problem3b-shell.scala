// Reduce some of the debugging output of Spark
import org.apache.log4j.Logger
import org.apache.log4j.Level

Logger.getLogger("org").setLevel(Level.ERROR)
Logger.getLogger("akka").setLevel(Level.ERROR)

// Import the basic recommender libraries from Spark's MLlib package
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation._
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.evaluation.MulticlassMetrics

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
trainData.first // show first entry


//////////////////////////////////////////////////////////////////////////
// Necessary parts from (a)
//////////////////////////////////////////////////////////////////////////
val userDistinctArtistsCounts = trainData.map(line => (line.user, line.product)).countByKey() // this gives the indicated amount
// val userDistinctArtistsCounts = trainData.map(line => (line.user, line.product)).distinct().countByKey()
// val userArtistsCounts_test = trainData.countDistinctByKey()

val trainData100 = trainData.filter{
    line => 100 <= (userDistinctArtistsCounts.get(line.user).get)
}
trainData100.cache()

// check if we have the correct amount of users
trainData100.map(line => (line.user, line.product)).reduceByKey(_ + _).distinct().count()
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

// 100 random users from testData10
val amountUsers = 100
val amountRatings = 25
val someUsers = testData10.map(x => x.user).distinct.takeSample(false,amountUsers,10)

// provided function to get the actual listened to artists for a user
def actualArtistsForUser(someUser: Int): Set[Int] = {
  val rawArtistsForUser = rawUserArtistData.map(_.split(' ')).
    filter { case Array(user,_,_) => user.toInt == someUser }
  rawArtistsForUser.map(x => x(1).toInt).collect().toSet
}

/////////////////////////////////////////////////////////////////////////////
// (b)
/////////////////////////////////////////////////////////////////////////////
// "manual" 5-fold crossvalidation and search for hyperparameters

// first produce the 5 different sets for the crossvalidation

val testSets = trainData90.randomSplit(Array(0.2,0.2,0.2,0.2,0.2))
// the corresponding training sets are given by the union of the other k-1 validation sets
val trainSets = for (idx1 <- 0 to 4) yield {
  var trainSet = sc.emptyRDD[Rating]
  for (idx2 <- 0 to 4 if idx2 != idx1) {
    trainSet = trainSet.union(testSets(idx2))
  }
  trainSet
}
trainSets.foreach(x => x.cache())

val amountUsers = 100
val amountRatings = 25
// create users sets for validation in the crossvalidation
val validationUsers = for (i <- 0 to 4) yield {
  val users = testData10.map(x => x.user).distinct.takeSample(false,amountUsers,10+i)
  users
}
val startCV = System.nanoTime

// now perform the crossvalidation
val evaluations = for {rank <- Array(10,25, 50);
  lambda <- Array(1.0, 0.01, 0.001);
  alpha <- Array(1.0, 10.0, 100.0)}
  yield {
    Console.println(s"R = ${rank}, L = ${lambda}, A = ${alpha}")

    var AUC = 0.0
    // 5-fold crossvalidation for the current hyperparameter setting
    for (i <- 0 to 4) {
      Console.println(s"Fold ${i+1}/5...")
      val model = ALS.trainImplicit(trainSets(i), rank, 10, lambda, alpha)

      // use the same validationsUsers for each hyperparameter combination
      validationUsers(i).map{ currUser =>
        val actualArtists = actualArtistsForUser(currUser)

        val recommendations = model.recommendProducts(currUser, amountRatings)
        val predictionAndLabels = recommendations.map { 
          case Rating(user, artist, rating) =>
            if (actualArtists.contains(artist)) {
              (rating, 1.0)
            } else {
              (rating, 0.0)
            }
          }

          val metrics = new BinaryClassificationMetrics(sc.parallelize(predictionAndLabels))
          AUC += metrics.areaUnderROC
      }

      AUC /= amountUsers
    }
    AUC /= 5.0
    ((rank, lambda, alpha), AUC)
}

val stopCV = System.nanoTime()
val diff_in_s = (stopCV - startCV) / 1e9d
val secs = (diff_in_s % 60).toInt
val mins = ((diff_in_s / 60.0) % 60).toInt
Console.println(s"Time for the crossvalidation: ${mins} min ${secs} s") 



val evalsSorted = evaluations.sortBy(_._2).reverse
val bestHyperParameters = evalsSorted(0)

// extract the best parameters

val rank = bestHyperParameters._1._1
val lambda = bestHyperParameters._1._2
val alpha = bestHyperParameters._1._3

// let them know in the shell
Console.println(s"Best Parameters:\n rank = ${rank}\n lambda = ${lambda}" +
  s"\n alpha = ${alpha}")


// train a model with the best parameters
val bestModel = ALS.trainImplicit(trainData90, rank, 10, lambda, alpha)

// get users from the testData100 set and calculate the desired metrics
val testUsers = testData10.map(x => x.user).distinct.takeSample(false,amountUsers,37)


val evals = testUsers.map{ currUser =>
  val val actualArtists = actualArtistsForUser(currUser)

        val recommendations = bestModel.recommendProducts(currUser, amountRatings)
        val predictionAndLabels = recommendations.map { 
          case Rating(user, artist, rating) =>
            if (actualArtists.contains(artist)) {
              (rating, 1.0)
            } else {
              (rating, 0.0)
            }
          }

          val metrics = new BinaryClassificationMetrics(sc.parallelize(predictionAndLabels))
          val multi_metrics = new MulticlassMetrics(sc.parallelize(predictionAndLabels))
          val AUC = metrics.areaUnderROC
          val accuracy = multi_metrics.accuracy
          val precision = metrics.precisionByThreshold
          val recall = metrics.recallByThreshold
          (AUC, accuracy, precision, recall)
}

val AUCs = evals.map{x => x._1}
val accuracies = evals.map{x => x._2}
val precisionsByThreshold = evals.map{x => x._3}
val recallsByThreshold = evals.map{x => x._4}

// now calculate the means
val meanAUC = AUCs.sum / amountUsers
val meanAcc = accuracies.sum / amountUsers

Console.println(s"Metrics for the best model:\n" +
  s"AUC: ${meanAUC}\n" +
  s"Accuracy: ${meanAcc}\n")
val stop = System.nanoTime()
val diff_in_s = (stop - startCV) / 1e9d
val secs = (diff_in_s % 60).toInt
val mins = ((diff_in_s / 60.0) % 60).toInt
Console.println(s"Elapsed total time: ${mins} min ${secs} s") 

