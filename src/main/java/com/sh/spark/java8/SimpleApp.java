package com.sh.spark.java8;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

public class SimpleApp {
	public static void main(String[] args) {
		String logFile = "/home/impadmin/Downloads/temp/number.txt"; // Should be some file on your system
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
				.set("spark.executor.memory", "1g");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logData = sc.textFile(logFile).cache();

		long num1s = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) {
				return s.contains("1");
			}
		}).count();

		long num2s = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) {
				return s.contains("2");
			}
		}).count();

		System.out.println("Lines with 1: " + num1s + ", lines with 2: " + num2s);
	}
}
