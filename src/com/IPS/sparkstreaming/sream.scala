package com.IPS.sparkstreaming

import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import Utilities._
 
object stream {
  

   def main(args: Array[String]) {  
       System.setProperty("twitter4j.oauth.consumerKey", "bGEiKhE1n6ddgMncofTnXm3IG")
       System.setProperty("twitter4j.oauth.consumerSecret", "wdMZfxbqUhmnI89XwesfavRBrotdvPeebp5jWTkkZ2wvs36rfk")
       System.setProperty("twitter4j.oauth.accessToken", "1279599286234292225-GFgPIPJP2Jslf2lMFeOLZ6yHT0IWUX")
       System.setProperty("twitter4j.oauth.accessTokenSecret", "FSQGdVixfbBs4E9CW9fG18rohK78v4V5gMmEhdqN2IlUr")      
        
       val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(1))
       val filters = Array("eurusd")
        val tweets = TwitterUtils.createStream(ssc, None, filters)
        val statuses = tweets.map(status => status.getText())
        statuses.print()
        
        ssc.start()
        ssc.awaitTermination()
   }
   
}