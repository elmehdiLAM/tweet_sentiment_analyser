package com.IPS.sparkstreaming

import com.IPS.sparkstreaming.SentimentAnalyzer._

object test1{
  
   def main(args : Array[String]){
     val input="RT @realbdw: ‘Star Wars: The Empire Strikes Back’ Leads Weekend Box Office 23 Years After Reissue https://t.co/8v7v2EOxYW via @variety"
     val sentiment = SentimentAnalyzer.mainSentiment(input)
     println(sentiment)
   }
}