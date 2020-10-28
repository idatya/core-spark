package com.sh.spark.java8;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkControlPartitionSizeLocally {
	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext("local", "localpartitionsizecontrol");
		String input = "four score and seven years ago our fathers brought forth on this continent "
				+ "a new nation conceived in liberty and "
				+ "dedicated to the propostion that all men are created equal";
		List<String> lst = Arrays.asList(StringUtils.split(input, ' '));
		
		List lst2 = new ArrayList();
		lst2.add(1);
		lst2.add("string");

		for (int i = 1; i <= 30; i++) {			
			JavaRDD rdd = sc.parallelize(lst2, i);
			System.out.println(rdd.partitions().size());
		}
	}
}