package com.sh.spark.common;

import org.apache.spark.sql.SparkSession;

public class CommonUtils {
	public static SparkSession getSparkSession() {
		SparkSession session = SparkSession.builder().master("local[2]").config("deploy-mode", "cluster")
				.appName("DataScienceExample").getOrCreate();
		return session;
	}
}
