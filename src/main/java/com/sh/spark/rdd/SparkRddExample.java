package com.sh.spark.rdd;

import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

public class SparkRddExample {
	public static void main(String[] args) {

		SparkSession session = SparkSession.builder().master("local[2]").appName("SparkRddExample")
				.config("spark.hadoop.validateOutputSpecs", "false").getOrCreate();
		SparkContext sc = session.sparkContext();
		JavaRDD<String> data1 = sc.textFile("/home/impadmin/aditya/data/spark-log.txt", 2).toJavaRDD();
		JavaRDD<String> data2 = sc.textFile("/home/impadmin/aditya/data/output.txt", 2).toJavaRDD();

		System.out.println(data1.count());
		System.out.println(data2.count());

		JavaRDD<String> unionData = data1.union(data2);
		JavaRDD<String> cacheData = unionData.cache();
		
		cacheData.coalesce(1, false).saveAsTextFile("/home/impadmin/aditya/data/union-data/");
		System.out.println("---------------------------------------------------------------");
		data1.foreach(element -> {
			System.out.println(element);
		});
		System.out.println("---------------------------------------------------------------");

		List<String> collect = data1.collect();
		System.out.println(collect.size());
		collect.stream().forEach(System.out::println);
		
		JavaRDD<String> filterData = cacheData.filter(data -> data.matches("[A-Za-z0-9_\\s.\\/\\:]+"));
		System.out.println("Filter data count =" + filterData.count());
		filterData.coalesce(1).saveAsTextFile("/home/impadmin/aditya/data/filter-data-coalesce/");
		
	}
}
