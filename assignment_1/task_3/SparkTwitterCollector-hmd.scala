import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.matching.Regex
import scala.io.Source
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object SparkTwitterCollector {
  
  def main(args: Array[String]) {
    
    if (args.length < 7) {
      System.err.println("Correct arguments: <output-path> <time-window-secs> <timeout-secs> <twitter-consumer-key> <twitter-consumer-secret> <twitter-access-token> <twitter-access-token-secret>")
      System.exit(1)
    }

    val outputDirectory = args(0)
    val windowSecs = args(1).toLong
    val timeoutSecs = args(2).toInt
    val partitionsPerInterval = 1


    System.setProperty("twitter4j.oauth.consumerKey", args(3))
    System.setProperty("twitter4j.oauth.consumerSecret", args(4))
    System.setProperty("twitter4j.oauth.accessToken", args(5))
    System.setProperty("twitter4j.oauth.accessTokenSecret", args(6))

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(windowSecs))
    val tweetStream = TwitterUtils.createStream(ssc, Some(new OAuthAuthorization(new ConfigurationBuilder().build())))
    //the regex required for getting strings containing hashtags followed by a alphanum
    val rgx = "^#[A-Za-z0-9]*".r
    //split the tweets by space and then apply regex's filter
    val hashtags = tweetStream.flatMap(status => status.getText.split(" ")).filter(x => (rgx.pattern.matcher(x).matches))
    
    var limit = 1000
    
    //sorting the top {limit} tweets based on value

    //Tom's Method of Sorting
    //transform spark stream to RDD (spark context) so as to apply sortBy	  	
    val tweets_sorted = tweets_cnt.transform(rdd => rdd.sortBy(_._2, false))
    //TODO: Apply "limit" function to a stream. "limit" is not part of the stream library function.
    //val tweets_sorted_lim = tweets_sorted.transform(rdd => rdd.take(limit))
    
    //Hamed Method of sorting
    //val tweets_cnt = hashtags.map(word => (word, 1)).reduceByKey(_ + _)
    //val tweets_sorted= tweets_cnt .map(item => item.swap).sortByKey(false, 1).map(item => item.swap).take(limit)
	    
    
    tweets_sorted_lim.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0)
        rdd.repartition(partitionsPerInterval).saveAsTextFile(outputDirectory + "/tweets_" + time.milliseconds.toString)
    })
    ssc.start()
    ssc.awaitTerminationOrTimeout(timeoutSecs * 1000)
  }
}

