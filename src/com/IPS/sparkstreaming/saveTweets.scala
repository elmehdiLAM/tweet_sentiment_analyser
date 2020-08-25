package com.IPS.sparkstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.Level
import Utilities._

object saveTweets{
  
  
  
  
  
  def main(args : Array[String]){
    // remplissage des donnes de twitter 
    setupTwitter()
    // creation du stream context
    val sc= new StreamingContext("local[2]","twitterStream",Seconds(1))
    // s'authentifier
    setupLogging()
    
    val tweets=TwitterUtils.createStream(sc,None)
    // retourner que le texte des tweets
    val text= tweets.map(x => x.getText())
    
   // mettre les tweets en fichier tout simplement
    text.saveAsTextFiles("twitter", "txt")
    // pour stocker ces donne la en memoire morte 
   // var K:Long =0 
    //text.foreachRDD((rdd,time)=>{
      //if (rdd.count()> 0){
       //val repData= rdd.repartition(1).cache()
       //repData.saveAsTextFile("tweets_"+time.milliseconds.toString)
       //K += repData.count()
       //println("nombre cumiler des tweets : "+ K )
       //if(K>1000){
        // System.exit(0)
       //}
      //}
    //})
    sc.checkpoint("C:/checkpoint/")
    sc.start()
    sc.awaitTermination()
    }
    
  
    
    
  }
