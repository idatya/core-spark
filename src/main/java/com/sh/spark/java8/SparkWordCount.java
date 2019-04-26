package com.sh.spark.java8;

import java.io.File;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkWordCount {
	public static void main(String[] args) throws Exception {
		/*System.out.println(System.getProperty("hadoop.home.dir"));
		String inputPath = args[0];*/
		String outputPath = "file:///home/impadmin/aditya/data/output";
		FileUtils.deleteQuietly(new File(outputPath));

		JavaSparkContext sc = new JavaSparkContext("local", "sparkwordcount");

		JavaRDD<String> rdd = sc.textFile("file:///home/impadmin/aditya/data/test3_clean.csv");

		JavaPairRDD<String, Integer> counts = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator())
				.mapToPair(x -> new Tuple2<String, Integer>(x, 1)).reduceByKey((x, y) -> x + y);

		counts.saveAsTextFile(outputPath);
		sc.close();
	}
}
