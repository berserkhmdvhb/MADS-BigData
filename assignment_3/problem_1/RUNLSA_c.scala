import org.apache.spark.{SparkConf, SparkContext}
import scala.util.matching.Regex
import scala.io.Source
import org.apache.spark.sql.Row
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import scala.io.Source
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.io.Source.fromFile
import java.util.Properties
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ling.CoreAnnotations._
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, typedLit, when}


object RUNLSA_c {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Correct arguments: <input-directory> <output-directory> <stopwords-directory>")
      System.exit(1)
    }
    val start = System.nanoTime()
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val sparkConf = new SparkConf().setAppName("RUNLSA_c")
    val ctx = new SparkContext(sparkConf)
    ctx.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._




    ///subtask (a)/////

    // store paths from "args" input
    //val path = "/home/hamed/Documents/datasets/wiki_pedia_plots/wiki_movie_plots_deduped.csv"
    // val path = "/home/hamed/Documents/datasets/wiki_pedia_plots/stopwords.txt"
    val input_directory = args(0)
    val output_directory = args(1)
    val stopwords_directory = args(2)


    // Define the schema for the dataframe about movies
    val schema_movies = StructType(
      Array(
        StructField("Release Year", IntegerType, false),
        StructField("Title", StringType, false),
        StructField("Origin/Ethnicity", StringType, false),
        StructField("Director", StringType, false),
        StructField("Cast", StringType, false),
        StructField("Genre", StringType, false),
        StructField("Wiki Page", StringType, false),
        StructField("Plot", StringType, false)
      )
    )


    // Read the dataframe from file
    // We tried to obviate the need of parsing function by defining the schema and also to keep as much information
    val df_temp = spark.read.option("header", true).option("multiLine",true).option("mode", "DROPMALFORMED").schema(schema_movies).csv(input_directory)
    //.sample(false, sampleSize)


    val df = df_temp.select("Title", "Genre", "Plot")
    //val df = df_temp2.na.drop()


    ///////subtask (b)//////

    // Select columns title a RDD consisting of pairs (title, plot) and then
    // convert them to a RDD of (string, string)
    val rddPlotsTitle = df.select("Title", "Plot").rdd.map { row => (row.getString(0), "\"" + row.getString(1) + "\"") }

    // count number of docs
    df.cache()
    val numDocs = df.count()
    val bNumDocs = sc.broadcast(numDocs)


    // defined for removing letters
    def isOnlyLetters(str: String): Boolean = {
      str.forall(c => Character.isLetter(c))
    }

    // load stopwords
    val bStopWords = sc.broadcast(
      fromFile(stopwords_directory).getLines().toSet)


    // create a NLP pipeline consisting of:
    // 1. Sentence splitting
    // 2. Tokenization
    // 3. Part-Of-Speech (POS) tagging
    // 4. Lemmatization
    def createNLPPipeline(): StanfordCoreNLP = {
      val props = new Properties()
      props.put("annotators", "tokenize, ssplit, pos, lemma")
      new StanfordCoreNLP(props)
    }


    // defined function for the purpose of lemmatizing tokens obtained from plots while removing stopwords
    def plainTextToLemmas(text: String, pipeline: StanfordCoreNLP): Seq[String] = {
      val doc = new Annotation(text)
      pipeline.annotate(doc)
      val lemmas = new ArrayBuffer[String]()
      val sentences = doc.get(classOf[SentencesAnnotation])
      for (
        sentence <- sentences;
        token <- sentence.get(classOf[TokensAnnotation])
      ) {
        val lemma = token.get(classOf[LemmaAnnotation]).toLowerCase
        if (lemma.length > 2 && !bStopWords.value.contains(lemma)
          && isOnlyLetters(lemma)) {
          lemmas += lemma
        }
      }
      lemmas
    }

    // applying the plainTexttoLemmas on plots
    val lemmatized: RDD[(String, Seq[String])] =
      rddPlotsTitle.mapPartitions(it => {
        val pipeline = createNLPPipeline()
        it.map {
          case (title, contents) =>
            (title, plainTextToLemmas(contents, pipeline))
        }
      })


    // Adding features (lemmatized plots) to dataframe
    val schema_features = StructType(
      Array(
        StructField("title", StringType, false),
        StructField("features", StringType, false)
      )
    )
    val df_lemmas = lemmatized.toDF().withColumnRenamed("_1","Title").withColumnRenamed("_2","Features")
    //.withColumn("id",monotonicallyIncreasingId)

    val features_df = df_lemmas.join(df_temp, Seq("Title"))
    //val features_df = df.join(df_lemmas, df("Title") ===  df_lemmas("Title"),"inner")
    features_df.cache()
    features_df.write.csv(output_directory)

  }
}
