package com.learningspark.chap2

import org.apache.spark.{SparkConf, SparkContext}

import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
  * Created by matheussilveira on 21/04/2017.
  */
object LearningSparkChap2{

  def startContext(): SparkContext={
    // start spark conf with app name and cluster (local)
    val conf = new SparkConf().setAppName("LearningSparkChap2").setMaster("local")
    // return a spark context
    new SparkContext(conf)
  }


  def getLines(sc: SparkContext, file: String) {
    val lines = sc.textFile(file)

    println(lines.count())

    println(lines.first())

  }

  def linesWPython(sc: SparkContext, file: String): Unit ={
    val lines = sc.textFile(file)
    val pythonLines = lines.filter(line => line.contains("Python"))


    // prints the 1st lines with python
    println(pythonLines.first())
  }

  def linesWPythonRefac(sc: SparkContext, file: String): Unit ={
    val lines = sc.textFile(file)
    val pythonLines = lines.filter(line => isPythonIn(line))

    // with this foreach, all lines w/ python will be printed
    pythonLines.foreach(println)
  }

  def isPythonIn(line:String): Boolean={
    line.contains("Python")
  }

  def countWords(sc: SparkContext, file: String) {
    val input = sc.textFile(file)

    val words = input.flatMap(line => line.split(" "))

    val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }

    counts.sortBy(_._2).foreach(println)
  }

  def main(args: Array[String]): Unit = {
    // display only errors on console
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // starts spark context to be used within this object
    val sc = startContext()

    // file that will be used in this chapter's examples
    val file = "/Users/matheussilveira/workspace/learning-spark/README.md"

    println("\n /////// Get Lines:\n")
    getLines(sc,file)

    println("\n /////// Lines with Python:\n")
    linesWPython(sc,file)

    println("\n /////// Lines with Python Refactored:\n")
    linesWPythonRefac(sc,file)

    println("\n /////// Word Counter:\n")
    countWords(sc,file)
  }
}