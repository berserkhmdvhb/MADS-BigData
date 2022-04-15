import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._ //implicit conversions 

import scala.util.matching.Regex
import scala.io.Source

object SparkWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Correct arguments: <input-directory> <output-directory>")
      System.exit(1)
    }
    val start = System.nanoTime()

    val sparkConf = new SparkConf().setAppName("SparkWordCount")
    val ctx = new SparkContext(sparkConf)
    ctx.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive","true")
    val textFile = ctx.textFile(args(0)) // this creates a textfile RDD from the file we pass in the command line as the first parameter

    // here comes the stuff we add

    
    val limit = 1000 // we only want the 1000 most frequent tokens 


    // first define the patterns we want
    // make them filter functions

    def is_word(token: String): Boolean = {
      val pattern = "[a-z-_]*".r
      return pattern.pattern.matcher(token).matches
    }
    def is_nbr(token: String): Boolean = {
      val pattern = "\\d+(\\.\\d+)?(?=$| )|(?<=^| )\\.\\d+(?=$| )".r
      return pattern.pattern.matcher(token).matches
    }


    // transform everything to lower-case, as requested
    val lowerCase_RDD = textFile.flatMap(line => line.split(" "))
                      .map(token => token.toLowerCase())
    //lowerCase_RDD.persist() // keep it cached, should we?
    // now filter according to the patterns for words and numbers
    //val words_RDD, numbers_RDD = (lowerCase_RDD.filter(f) for f in (is_word, is_nbr))
    val words_RDD = lowerCase_RDD.filter(is_word(_))
    val numbers_RDD = lowerCase_RDD.filter(is_nbr(_))
    /* val counts = textFile.flatMap(line => line.split(" "))
                 .map(word => (word, 1))
                 .reduceByKey(_ + _) // reduce by key returns a distributed dataset
    counts.saveAsTextFile(args(1))  */
    
    // calculate counts for words and numbers separately
    // maybe even use count by value?
    val words_cnt = words_RDD.map(word => (word, 1))
                              .reduceByKey(_+_)
    val nbrs_cnt = numbers_RDD.map(nbr => (nbr, 1))
                              .reduceByKey(_+_)

    // sort in descending order by value, apply the limit at 1000
    // take creates a list, so make into an RDD again with
    // ctx.parallelize
    val words_sorted = ctx.parallelize(words_cnt.sortBy(_._2, false).take(limit)) // false for descending order
    val nbrs_sorted = ctx.parallelize(nbrs_cnt.sortBy(_._2, false).take(limit))



    // now save them in two separate files
    // the coalesce stuff is dangerous if the machine
    // does not have enough memory
    // same in hadoop
    // that's why we usually define output DIRECTORIES
    // hence why we are supposed to only take
    // the top 1000?
    words_sorted.coalesce(1, shuffle=true).saveAsTextFile(args(1) + "/words")
    nbrs_sorted.coalesce(1, shuffle=true).saveAsTextFile(args(1) + "/numbers")

    ctx.stop()

    val stop = System.nanoTime()
    val diff_in_s = (stop - start) / 1e9d
    val secs = (diff_in_s % 60).toInt
    val mins = ((diff_in_s / 60.0) % 60).toInt
    Console.println(s"Elapsed time: ${mins} min ${secs} s")
  }
}
