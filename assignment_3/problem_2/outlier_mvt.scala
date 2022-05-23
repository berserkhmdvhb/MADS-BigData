// ------------------------ Disable excessive logging -------------------------

import org.apache.log4j.Logger
import org.apache.log4j.Level

Logger.getLogger("org").setLevel(Level.ERROR)
Logger.getLogger("akka").setLevel(Level.ERROR)

// ------------------------ Start of the exercise -----------------------------
// necessary imports
// NOTE: using the new dataframe-based ml library instead of the rdd-based mllib
// this changes quite a few things, eg. model.fit instead of model.run
import org.apache.spark.ml.clustering._
import org.apache.spark.ml.linalg._
import org.apache.spark.rdd._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._


import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator


//start timing the code
val start = System.nanoTime()

// load the data
val sampleSize = 1 // as always reduce samplesize for debugging
val dataPath = "../../Data/gearbox_readings/csvs/" // path to the directory of csv files
val rawData_df = spark.read.option("inferSchema", "true").csv(dataPath).sample(false,sampleSize)
// inferSchema set to true so that the values are read as doubles instead of strings

// let's have a look at the data, first 10 rows
rawData_df.show(10)
// -> three columns, input voltage, output voltage, tachometer reading
// they're all double values

// get statistics of our data
rawData_df.describe().show()

// let's rename our columns for ease of use and clarity
val namedData = rawData_df.withColumnRenamed("_c0", "inVolt").withColumnRenamed("_c1", "outVolt").withColumnRenamed("_c2", "tacho")
namedData.show(10)
namedData.persist()

// write it with header into a directory to visualise with python later
namedData.sample(false, fraction=0.01).write.option("header",true).csv("./output/data4plots/raw_sample") 

// At this point, refer to the visualise_cluster.py file to have a look at the
// raw data. This gives us a pretty good idea about potential clusters already.

// ----------------------- Normalize the data ---------------------------------
// we will normalize all our columns
// always a good idea when an algorithm relies on distance-based
// measures, which is the case for kmeans

val inputCols = Array("inVolt","outVolt","tacho")
val assembler = new VectorAssembler().setInputCols(inputCols).setOutputCol("features")
val transformVector = assembler.transform(namedData)
val scaler = new StandardScaler().setInputCol("features").setOutputCol("normFeatures").setWithStd(true).setWithMean(false)
val scalerModel = scaler.fit(transformVector)
val normData = scalerModel.transform(transformVector) // the last column of this contains the normalised features
normData.cache()
namedData.unpersist()

// ------------------- Hyper-parameter Tuning -----------------------------------------------
// Paremeters looked at:
//     - number of clusters k
// for the distance measure only Euclidean really makes sense, since we have numeric features
// one could also look at random initialisation of the centroids instead of k-means||

val kValues = (2 to 10).toArray

val scores = kValues.par.map{ k =>
    val kmeans = new KMeans()
    kmeans.setK(k)
    kmeans.setFeaturesCol("normFeatures")

    val model = kmeans.fit(normData)
    val preds = model.transform(normData)
    val evaluator = new ClusteringEvaluator()
    val silhouette = evaluator.evaluate(preds)
    val cost = model.summary.trainingCost

    (k, silhouette, cost)
}.toArray.sortBy(_._1) // sort in ascending value for k

val bestK = (scores.maxBy(_._2))._1 // get the k-value with the best silhouette score
// may be inefficient to fit the model again, but oh well
val kmeans = new KMeans()
kmeans.setK(bestK)
kmeans.setFeaturesCol("normFeatures")

val model = kmeans.fit(normData)
val centroids = model.clusterCenters // these are normalized, so use the normalized data for the outlier detection, too!!!

val predictions_df = model.transform(normData) // prediction is the cluster-index (row-wise)
predictions_df.cache()
// use these predictions to colour the data points in the visualisation
predictions_df.show(20) // take a look at the predictions

// save the prediction df for plotting
// make sure to include equal amounts of samples from
// each cluster when working with the whole data set!
val dataWithPreds = predictions_df.drop("features", "normFeatures")
val dataWithPreds_sampled = dataWithPreds.sample(false, 0.01) // take 1%
dataWithPreds_sampled.write.option("header", true).csv("./output/data4plots/clusters")

// function to calculate the distance to the respective centroid
def distance2centroid (v: Vector, clusterId: Int) : Double = {
    val c = centroids(clusterId)
    val distance = Math.sqrt(Vectors.sqdist(v,c))
    return distance
}
val distanceUDF = udf(distance2centroid _) // register it as a UDF so we can apply it on a column-wise basis

// add a column containing the distance of that point to its centroid
val preds_with_dists = predictions_df.withColumn("dist", distanceUDF(predictions_df("normFeatures"), predictions_df("prediction")))

// get max distances
val maxDistPerCluster = for (idx <- 0 to bestK-1) yield {
    val filtered_df = preds_with_dists.filter(preds_with_dists("prediction") === idx)
    val max_dist = filtered_df.select(max("dist")).head()(0)
    max_dist.asInstanceOf[Double]
}

////////////////// Function to determine if a point is an outlier /////////////////////////////
// Criterion used for detecting outliers: 
// outlyingness of a vector (point) given by
// O_i = ||x_i - c_pi||/d_max
// where x_i is the considered point, c_pi the centroid of the cluster x_i was assigned to
// d_max is the maximal distance between any point of the cluster and the respective centroid
// the threshold is set to 0.9
// taken from: Outlier Detection using Improved Genetic K-means
// in International Journal of Computer Applications (2011)
// by M.H. Marghny and Ahmed I. Taloba
////////////
def isOutlier (dist: Double, clusterId: Int) : Boolean = {
    val max_dist = maxDistPerCluster(clusterId).toDouble
    val outlyingness = dist / max_dist
    return outlyingness > 0.9 // 0.9 is usually a sensible value for this
}

val isOutlierUDF = udf(isOutlier _)

// apply the outlier detection function
val final_df = preds_with_dists.withColumn("outlier", isOutlierUDF(preds_with_dists("dist"), preds_with_dists("prediction")))

// filter out our outliers as given by our function above
val outliers = final_df.filter(final_df("outlier") === true)
outliers.show()
outliers.drop("features", "normFeatures").write.option("header",true).csv("./output/outliers") // write the outliers to a csv file 

//calculate runtime
val stop = System.nanoTime()
val diff_in_s = (stop - start) / 1e9d
val secs = (diff_in_s % 60).toInt
val mins = ((diff_in_s / 60.0) % 60).toInt
Console.println(s"Elapsed time: ${mins} min ${secs} s")

// exit shell after the script is done
sys.exit 