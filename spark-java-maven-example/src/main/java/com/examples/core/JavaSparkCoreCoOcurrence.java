package com.examples.core;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.List;
import java.util.ArrayList;

public class JavaSparkCoreCoOcurrence {
	private static final FlatMapFunction<String, String> WORDS_EXTRACTOR =
		      new FlatMapFunction<String, String>() {
		        /**
				 * 
				 */
				private static final long serialVersionUID = 6514900807032236998L;

				@Override
		        public Iterable<String> call(String s) throws Exception {
		        	String[] splitted = s.split("\\W");
		        	List<String> lista= new ArrayList<String>();
		        	for (int i=0;i< splitted.length-2;i++){
		        		lista.add(splitted[i]+","+splitted[i+1]);
		        	}
		          return lista;
		        }
		      };

		  private static final PairFunction<String, String, Integer> WORDS_MAPPER =
		      new PairFunction<String, String, Integer>() {
		        /**
				 * 
				 */
				private static final long serialVersionUID = 475353142624359497L;

				@Override
		        public Tuple2<String, Integer> call(String s) throws Exception {
		          return new Tuple2<String, Integer>(s, 1);
		        }
		      };

		  private static final Function2<Integer, Integer, Integer> WORDS_REDUCER =
		      new Function2<Integer, Integer, Integer>() {
		        /**
				 * 
				 */
				private static final long serialVersionUID = -3858594876429981773L;

				@Override
		        public Integer call(Integer a, Integer b) throws Exception {
		          return a + b;
		        }
		      };
		      
		  private static final PairFunction<Tuple2<String,Integer>,Integer,String> KEY_SWAP =
				      new PairFunction<Tuple2<String,Integer>,Integer,String>() {
				        /**
						 * 
						 */
						private static final long serialVersionUID = 3275511195387433281L;

						@Override
				        public Tuple2<Integer,String> call(Tuple2<String,Integer> a) throws Exception {
				          return a.swap();
				        }
				      };

		  public static void main(String[] args) {
		    if (args.length < 2) {
		      System.err.println("Please provide the input file full path as argument");
		      System.exit(1);
		    }

		    SparkConf conf = new SparkConf().setAppName("org.sparkexample.WordCount");
		    @SuppressWarnings("resource")
			JavaSparkContext context = new JavaSparkContext(conf);

		    JavaRDD<String> file = context.textFile(args[0]);
		    JavaRDD<String> words = file.flatMap(WORDS_EXTRACTOR);
		    JavaPairRDD<String, Integer> pairs = words.mapToPair(WORDS_MAPPER);
		    JavaPairRDD<String, Integer> counter = pairs.reduceByKey(WORDS_REDUCER);
		    JavaPairRDD<Integer,String> swapped = counter.mapToPair(KEY_SWAP);
		    JavaPairRDD<Integer,String> ordered = swapped.sortByKey(true);
		    ordered.saveAsTextFile(args[1]);
		  }
}
