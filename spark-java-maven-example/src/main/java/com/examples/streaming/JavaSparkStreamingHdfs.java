package com.examples.streaming;

import java.util.Arrays;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.api.java.JavaRDD;

/**
 * The LogAnalyzerImportStreamingFile illustrates how to run Spark Streaming,
 * but instead of monitoring a socket, it monitors a directory and feeds in any
 * new files to streaming.
 *
 * Once you get this program up and running, feed apache access log files into
 * that directory.
 *
 * Example command to run: % ${YOUR_SPARK_HOME}/bin/spark-submit --class
 * "com.databricks.apps.logs.chapter2.LogAnalyzerStreamingImportDirectory"
 * --master spark://YOUR_SPARK_MASTER YOUR_LOCAL_LOGS_DIRECTORY
 * target/log-analyzer-1.0.jar
 */
public class JavaSparkStreamingHdfs {
	private static final Duration SLIDE_INTERVAL = new Duration(10 * 1000);

	final static Logger logger = Logger.getLogger(JavaSparkStreamingHdfs.class);

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("JavaSparkStreamingHdfs");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext jssc = new JavaStreamingContext(sc, SLIDE_INTERVAL);

		// Specify a directory to monitor for log files.
		if (args.length == 0) {
			System.out.println("Must specify an access logs directory.");
			System.exit(-1);
		}
		String directory = args[0];

		// This methods monitors a directory for new files to read in for
		// streaming.
		JavaDStream<String> directoryStream = jssc.textFileStream(directory);

		directoryStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			@Override
			public void call(JavaRDD<String> file) {
				if (file.count() != 0) {
					processNewFile(file);
				}
			}
		});

		// Start the streaming server.
		jssc.start(); // Start the computation
		jssc.awaitTermination(); // Wait for the computation to terminate
	}

	public static void processNewFile(JavaRDD fileRDD) {
		fileRDD.foreach(new VoidFunction<String>() {
			@Override
			public void call(String line) {
				if (!line.equals("")) {
					logger.info("Imprimiendo una linea del fichero "+line);
				}
			}
		});
	}

}