package com.sh.spark.core;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.sh.spark.common.CommonUtils;

public class MapExample {
public static void main(String[] args) {
	SparkSession sparkSession = CommonUtils.getSparkSession();
	Dataset<Row> df = sparkSession.read().text("/home/impadmin/aditya/data/test_clean.txt");
}
}
