package com.twitterSentiment.spark

import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
 
object stream {
  

   def main(args: Array[String]) {  
       System.setProperty("twitter4j.oauth.consumerKey", "vLhtvIyyDMzbASPhAZSruXmbk")
       System.setProperty("twitter4j.oauth.consumerSecret", ":ukc0PFOcnXqHnDVIHYBM1J9pLbkvdmhQeihcq2C0SQ5nkxmlDs")
       System.setProperty("twitter4j.oauth.accessToken", "1279599286234292225-RJ9CEV6Ozwv0lnZlYhvnvp1hFo2N6h")
       System.setProperty("twitter4j.oauth.accessTokenSecret", "fR1GhMKKgpvHpph6Af4J1rzOUvGBpA9AYxs2CwgYo1jXn")      
        
       val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(1))
       //val filters = Array("#blackoutday2020")
        val tweets = TwitterUtils.createStream(ssc, None)
        val statuses = tweets.map(status => status.getText())
        statuses.print()
        
        ssc.start()
        ssc.awaitTermination()
   }
   
}