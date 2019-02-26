package com.sh.spark.core;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataScienceExample {
	public static void main(String[] args) {
		SparkSession sparkSession = getSparkSession();
		Dataset<Row> df = sparkSession.read().text("/home/impadmin/aditya/data/test_clean.txt");
		System.out.println(df.first().mkString());
	}

	public static SparkSession getSparkSession() {
		SparkSession session = SparkSession.builder().master("local[2]").config("deploy-mode", "cluster")
				.appName("DataScienceExample").getOrCreate();
		return session;
	}
}
