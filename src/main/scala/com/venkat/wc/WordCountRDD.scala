package com.venkat.wc

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession


object WordCountRDD {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder
      .master("local")
      .appName("WordCount")
      .getOrCreate()


    val sc = sparkSession.sparkContext

    val inputPath = "/home/venkatram/IdeaProjects/sparkasg/input.txt" //args(0)
    val outputPath = "/home/venkatram/IdeaProjects/sparkasg/output" //args(1)

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val inputPathExists = fs.exists(new Path(inputPath))
    val outPathExists = fs.exists(new Path(outputPath))

    if (!inputPathExists) {
      println("Invalid input path")
      return
    }

    if (outPathExists) {
      fs.delete(new Path(outputPath), true)
    }

    val wc = sc.textFile(inputPath)
      .flatMap(rec => rec.split(" "))
      .map(rec => (rec, 1))
      .reduceByKey((acc, value) => acc + value)

    wc.foreach(println)
    println("******************************")

    /*    wc.map(rec => rec.productIterator.mkString(("\t")))
            .saveAsObjectFile(outputPath)*/

    wc.map(rec => rec._1 + "\t" + rec._2)
      .saveAsTextFile(outputPath)
  }

}
