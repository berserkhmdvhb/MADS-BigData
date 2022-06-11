// ------------------------ Disable excessive logging -------------------------

import org.apache.log4j.Logger
import org.apache.log4j.Level

Logger.getLogger("org").setLevel(Level.ERROR)
Logger.getLogger("akka").setLevel(Level.ERROR)

// ------------------------ Start of the exercise -----------------------------
// necessary imports
// NOTE: using the new dataframe-based ml library instead of the rdd-based mllib
// this changes quite a few things, eg. model.fit instead of model.run
import org.apache.spark.ml.linalg._
import org.apache.spark.rdd._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel,RandomForestClassifier}
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator,ParamGridBuilder}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

//start timing the code
val start = System.nanoTime()

// ----------------------- Load the data --------------------------------------


val sampleSize = 0.005 // the data set is huge, so take a sampleset for debugging
val path2data = "../../Data/HIGGS/"
val rawData_df = spark.read.option("inferSchema", "true").csv(path2data).sample(false,sampleSize)
// inferSchema set to true so that the values are read as doubles instead of strings


// ----------------------- Data Inspection ------------------------------------

// let's have a look at the data, first 10 rows
rawData_df.show(10)
// only double values
// the first column is the label, the other 28 are features

// get statistics of our data
rawData_df.describe().show()

// check for missing values

// check if data set is balanced wrt the label
rawData_df.groupBy("_c0").count().show()


// ----------------------- Train/Test splits ----------------------------------

val Array(trainData, testData) = rawData_df.randomSplit(Array(0.9,0.1))
trainData.persist()
testData.persist()

// -------------------- Prepare the pipelines ---------------------------------
// label is binary, given as 0 or 1, so no need to index the label
// features are strictly numerical, so no need to index them
// it is enough to combine them via a vectorassembler into a single feature column
val label = "_c0"
val features = rawData_df.columns.filter(! _.contains(label))

val assembler = new VectorAssembler().setInputCols(features).setOutputCol("features")

// define the models we'll compare
val randForest = new RandomForestClassifier().setLabelCol(label).setFeaturesCol("features")
val modelMLP = (new MultilayerPerceptronClassifier()
                    .setLabelCol(label)
                    .setFeaturesCol("features"))

// set stages for the pipelines
val stagesForest = Array(assembler, randForest)
val stagesMLP = Array(assembler, modelMLP)

// now put it into pipelines
val pipeForest = new Pipeline().setStages(stagesForest)
val pipeMLP = new Pipeline().setStages(stagesMLP)

// also need an evaluator, binary is enough as we only have two classes
val evaluator = (new BinaryClassificationEvaluator()
                    .setLabelCol(label)
                    .setMetricName("areaUnderROC"))

// set parametergrids we want to cover
val paramGridForest = (new ParamGridBuilder()
                            .addGrid(randForest.numTrees, 5::10::20::50::Nil)
                            .addGrid(randForest.maxDepth, 5::10::20::Nil)
                            .build())


// set an array of layer configurations for the MLP
val layers = Array(Array(features.size, 64, 64, 2), Array(features.size, 64, 128, 128,2))
val paramGridMLP = (new ParamGridBuilder()
                        .addGrid(modelMLP.solver, "gd"::"l-bfgs"::Nil)
                        .addGrid(modelMLP.layers, layers(0)::layers(1)::Nil)
                        .addGrid(modelMLP.stepSize, 0.1::0.01::0.001::Nil)
                        .build())

// now build our crossvalidators with everything that came before
val numFolds = 5

val cvForest = ((new CrossValidator().setEstimator(pipeForest)
                                    .setEvaluator(evaluator)
                                    .setEstimatorParamMaps(paramGridForest)
                                    .setNumFolds(numFolds))
                                    .setParallelism(3))

val cvMLP = (new CrossValidator().setEstimator(pipeMLP)
                                    .setEvaluator(evaluator)
                                    .setEstimatorParamMaps(paramGridMLP)
                                    .setNumFolds(numFolds)
                                    .setParallelism(3))

// --------------------- Hyperparameter Tuning -------------------------------
val startRF = System.nanoTime()
val bestForest = cvForest.fit(trainData)
//calculate runtime for training RF
val stopRF = System.nanoTime()
val diff_in_s = (stopRF - startRF) / 1e9d
val secs = (diff_in_s % 60).toInt
val mins = ((diff_in_s / 60.0) % 60).toInt
Console.println(s"RF Training Time: ${mins} min ${secs} s")


val startMLP = System.nanoTime()
val bestMLP = cvMLP.fit(trainData)
//calculate runtime for training MLP
val stopMLP = System.nanoTime()
val diff_in_s = (stopMLP - startMLP) / 1e9d
val secs = (diff_in_s % 60).toInt
val mins = ((diff_in_s / 60.0) % 60).toInt
Console.println(s"MLP Training Time: ${mins} min ${secs} s")

// --------------------- Comparison of Models --------------------------------
val predsForest = (bestForest.transform(testData)
                        .select(label, "prediction", "probability", "rawPrediction"))
val predsMLP = (bestMLP.transform(testData)
                        .select(label, "prediction", "probability", "rawPrediction"))

// first ROC
println("ROC for Random Forest = " + evaluator.evaluate(predsForest))
println("ROC for MLP = " + evaluator.evaluate(predsMLP))


// and accuracy
val acc_evaluator = (new MulticlassClassificationEvaluator()
                        .setLabelCol(label)
                        .setPredictionCol("prediction")
                        .setMetricName("accuracy"))

println("Acc for Random Forest = " + acc_evaluator.evaluate(predsForest))
println("Acc for MLP = " + acc_evaluator.evaluate(predsMLP))


//calculate runtime
val stop = System.nanoTime()
val diff_in_s = (stop - start) / 1e9d
val secs = (diff_in_s % 60).toInt
val mins = ((diff_in_s / 60.0) % 60).toInt
Console.println(s"Elapsed time: ${mins} min ${secs} s")


// we outta here
sys.exit