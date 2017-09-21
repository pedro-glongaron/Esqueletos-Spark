package com.examples.streaming

import org.apache.spark.{ Logging, SparkConf }
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{ Seconds, StreamingContext }

object ScalaSparkStreamingHdfs extends Logging {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Please provide 2 parameters <directoryToMonitor> <microbatchtime>")
      System.exit(1)
    }
    val directoryToMonitor = args(0)
    val microBatchTime = args(1).toInt
    val sparkConf = new SparkConf().setAppName("ScalaSparkStreamingHdfs")
    val sparkStreamingContext = new StreamingContext(sparkConf, Seconds(microBatchTime))

    logInfo("Value of microBatchTime " + microBatchTime)
    logInfo("DirectoryToMonitor " + directoryToMonitor)

    val directoryStream = sparkStreamingContext.textFileStream(directoryToMonitor)

    logInfo("After starting directoryStream")
    directoryStream.foreachRDD { fileRdd =>
      if (fileRdd.count() != 0)
        processNewFile(fileRdd)
    }

    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()
    logInfo("Exiting HDFSFileStream.main")
  }

  def processNewFile(fileRDD: RDD[String]): Unit = {
    logDebug("Entering processNewFile ")
    fileRDD.foreach { line =>
      println(line)
    }
    logDebug("Exiting processNewFile ")

  }

}
