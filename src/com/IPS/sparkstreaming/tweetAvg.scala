package com.IPS.sparkstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.Level
import Utilities._

object tweetAvg{
  
  def main(args : Array[String]){
    
    setupTwitter()
    
    val ssc = new StreamingContext("local[2]","tweetAvg",Seconds(1))
    
    setupLogging()
    
    val tweets = TwitterUtils.createStream(ssc,None )
    
    val text=tweets.map(x=>  x.getText())
    
    val words=text.flatMap(x=>x.split("\\s"))
    
    
    val hashtags=words.filter(x=>x.startsWith("#"))
    
    val hashtags1=hashtags.map(x=>(x,1))
    
    val sor=hashtags1.reduceByKeyAndWindow((x,y)=>x+y,(x,y)=>x-y , Seconds(300), Seconds(1))
    
    val sorted=sor.transform(x=> x.sortBy(y=>y._2, false))
    
    sorted.print()
    
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
    
    
  }
  
}