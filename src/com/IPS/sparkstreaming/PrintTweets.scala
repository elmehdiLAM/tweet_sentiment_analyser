
package com.IPS.sparkstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.Level
import Utilities._
import com.IPS.sparkstreaming.SentimentAnalyzer._

/** Simple application to listen to a stream of Tweets and print them out */
object PrintTweets {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    
    setupTwitter()
          
        
  
    
    val ssc = new StreamingContext("local[2]", "SentimentAnalysis", Seconds(1))
    
    
    setupLogging()
    
    val tweets = TwitterUtils.createStream(ssc, None ).filter(x=> x.isPossiblySensitive()==false && x.getLang()=="en")
    
    val statuses = tweets.map(status => status.getText())
    
    val sentiment=statuses.map(x=>(x,SentimentAnalyzer.mainSentiment(x)))
    
    
    sentiment.print()
    
    
    
   var K:Long =0 
    sentiment.foreachRDD((rdd,time)=>{
      if (rdd.count()> 0){
       val repData= rdd.repartition(1).cache()
       repData.saveAsTextFile("tweets_"+time.milliseconds.toString)
       K += repData.count()
       println("nombre cumiler des tweets : "+ K )
       if(K>1000){
         System.exit(0)
       }
      }
    })
    
    
    // Kick it all off
    ssc.start()
    ssc.awaitTermination()
  }  
}