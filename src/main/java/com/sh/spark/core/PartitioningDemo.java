package com.sh.spark.core;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import com.sh.spark.common.CommonUtils;

public class PartitioningDemo {
	public static void main(String[] args) {
		SparkSession sesion = CommonUtils.getSparkSession();
		Dataset<String> df = sesion.read().textFile("/home/impadmin/aditya/data/number.txt");
		System.out.println(df.rdd().getNumPartitions());
		Dataset<String> repartition = df.repartition(4);
		System.out.println(repartition.rdd().getNumPartitions());
		//repartition.write().text("/home/impadmin/aditya/data/partition-output");
		Dataset<String> repartitionToReduce = repartition.repartition(2);
		System.out.println(repartitionToReduce.rdd().getNumPartitions());
		Dataset<String> repartitionToReduceWithCoalesce = repartition.coalesce(3);
		System.out.println(repartitionToReduceWithCoalesce.rdd().getNumPartitions());
		repartition.write().text("/home/impadmin/aditya/data/partition-output2");
		Dataset<String> repartitionToReduceWithCoalesce2 = repartition.coalesce(6);
		System.out.println(repartitionToReduceWithCoalesce2.rdd().getNumPartitions());
		repartition.write().text("/home/impadmin/aditya/data/partition-output3");
	}

}
