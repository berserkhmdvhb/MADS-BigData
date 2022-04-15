import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

import scala.util.matching.Regex
import scala.io.Source

object SparkTwitterCollector {
  
  def main(args: Array[String]): Unit = {
    
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
    val rgx = sc.broadcast("^#[A-Za-z0-9]*".r)
    val limit = sc.broadcast(1000)


    tweetStream.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0){

        val hashtags = rdd.flatMap(status => status.getText.split(" ")).filter(x => (rgx.value.pattern.matcher(x).matches))
        val hashtags_cnt = hashtags.map(hash => (hash, 1)).reduceByKey(_+_)
        val hashtags_srted = sc.parallelize(hashtags_cnt.sortBy(_._2, false).take(limit.value))

        hashtags_srted.coalesce(partitionsPerInterval, shuffle=true).saveAsTextFile(outputDirectory + "/hashtags_sortedByCount_" + time.milliseconds.toString)
        hashtags.repartition(partitionsPerInterval).saveAsTextFile(outputDirectory + "/hashtags_" + time.milliseconds.toString)


        val hashtag_pairs_cnt = sc.parallelize(rdd.map(status => status.getText.split(" ").filter(token => (rgx.value.pattern.matcher(token).matches)).sorted)
                            .flatMap(_.combinations(2))
                            .map(pair => (pair.mkString(":"),1))
                            .reduceByKey(_+_)
                            .sortBy(_._2, false)
                            .take(limit.value))
        hashtag_pairs_cnt.coalesce(partitionsPerInterval, shuffle=true)
                         .saveAsTextFile(outputDirectory + "/hashtag_pairs_" + time.milliseconds.toString)


        rdd.repartition(partitionsPerInterval).saveAsTextFile(outputDirectory + "/tweets_" + time.milliseconds.toString)
      }
    })

    ssc.start()
    ssc.awaitTerminationOrTimeout(timeoutSecs * 1000)
  }
}