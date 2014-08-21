package pl.icm.edu.spark.example

import collection.JavaConversions._
import org.apache.spark.{SparkConf, SparkContext}

/*** SimpleApp.scala ***/

object SimpleApp {
  def main(args: Array[String]) {
    val file = "file:///etc/passwd" // Should be some file on your system
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))
    val data = sc.textFile(file, 2)
    val words = data.map(x => x.split(":"))
    val sizes = words.map(x => x.length)
    val min = sizes.reduce(math.min)
    val max = sizes.reduce(math.max)
    //println(s"Lengths: $size.")
    println(s"Min value: $min. Max value: $max.")
  }
}

