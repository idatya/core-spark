package com.sh.spark.core;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.DoubleSummaryStatistics;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.sh.spark.common.CommonUtils;
import com.sh.spark.domain.QueryLogData;

public class LogAnalysis implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static void main(String[] args) {

		System.out.println("Start");

		SparkSession session = CommonUtils.getSparkSession();
		// test(session);
		@SuppressWarnings("serial")
		JavaRDD<QueryLogData> logRDD = session.read().textFile("/home/impadmin/Downloads/temp/VZ_sample_100.csv")
				.javaRDD().filter(l -> !l.startsWith("UserName#")).map(new Function<String, QueryLogData>() {
					@Override
					public QueryLogData call(String line) throws Exception {
						String[] parts = line.split("#");
						QueryLogData log = new QueryLogData();
						log.setUserName(parts[0]);
						log.setAppID(parts[1]);
						log.setClientID(parts[2]);
						log.setStartTime(parts[3]);
						log.setAmpCpuTime(Double.parseDouble(parts[4]));
						log.setTotalIOCount(Double.parseDouble(parts[5]));
						log.setParserCPUTime(Double.parseDouble(parts[6]));
						log.setFirstRespTime(parts[7]);
						log.setFirstStepTime(parts[8]);
						log.setProcID(parts[9]);
						log.setQueryID(parts[10]);
						log.setMaxAMPCPUT(Double.parseDouble(parts[11]));
						log.setMaxAmpIO(Double.parseDouble(parts[12]));
						log.setTotalCPU(Double.parseDouble(parts[13]));
						log.setTotalIO(Double.parseDouble(parts[14]));
						log.setQueryExecutionTime(Double.parseDouble(parts[15]));
						log.setDatabase(parts[16]);
						log.setQueryText(parts[17]);
						return log;
					}
				});
		 System.out.println(logRDD.count());

		// substractExcercise(session, logRDD);

		// printRDDData(logRDD);

		Dataset<Row> logDF = session.createDataFrame(logRDD, QueryLogData.class);
		logDF.printSchema();
		// logDF.show(30,false);

		logDF.registerTempTable("log");
		Dataset<Row> sql = session.sql("SELECT count(*) FROM log");
		sql.show(false);

		logDF.groupBy("userName").count().show(false);
		/*
		 * logDF.groupBy("appID").count().show(false);
		 * logDF.groupBy("clientID").count().show(false);
		 */
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.S");
		JavaRDD<Date> startDateRDD = logRDD.map(x -> x.getStartTime()).map(x -> formatter.parse(x));
		
		//sample(startDateRDD);

		Date max = startDateRDD.max(new DateComparator());
		System.out.println("Max: " + max);

		Date min = startDateRDD.min(new DateComparator());
		System.out.println("Min: " + min);

		DoubleSummaryStatistics summaryStatistics = logRDD.collect().stream().mapToDouble(x -> x.getAmpCpuTime())
				.summaryStatistics();
		System.out.println(summaryStatistics);
	}

	private static void sample(JavaRDD<Date> startDateRDD) {
		List<Date> takeSample = startDateRDD.takeSample(false, 10);
		takeSample.stream().forEach(System.out::println);
		
		startDateRDD.collect().forEach(System.out::println);

		Date max2 = takeSample.stream().max(new DateComparator()).get();
		System.out.println("max2 " + max2);

		Date min2 = takeSample.stream().min(new DateComparator()).get();
		System.out.println("min2 " + min2);
	}

	private static <T> void printRDDData(JavaRDD<T> logRDD) {
		Iterator<T> data = logRDD.take(10).iterator();

		while (data.hasNext()) {
			System.out.println(data.next().toString());

		}
	}

	private static void substractExcercise(SparkSession session, JavaRDD<QueryLogData> logRDD) {
		@SuppressWarnings({ "resource", "serial" })
		JavaRDD<QueryLogData> javaRDDNoHeader = logRDD
				.subtract(new JavaSparkContext(session.sparkContext()).parallelize(new ArrayList<QueryLogData>() {
					{
						add(logRDD.first());
					}
				}));

		System.out.println(javaRDDNoHeader.count());
	}

	private static void test(SparkSession session) {
		Dataset<String> df = session.read().textFile("/home/impadmin/Downloads/temp/VZ_sample_100.csv");
		long recordCount = df.count();
		System.out.println(recordCount);
		String header = df.head();
		System.out.println(header);
		String headerString = header.replace("#", ",");
		System.out.println(headerString);

		JavaRDD<String> data = df.javaRDD();
		System.out.println();
		df.printSchema();

		StructField[] structFields = new StructField[18];
		int i = 0;
		for (String head : headerString.split(",")) {
			StructField structField = new StructField(head, DataTypes.StringType, true, Metadata.empty());
			structFields[i] = structField;
			i++;
		}

		for (StructField field : structFields) {
			System.out.println(field.name() + " : " + field.dataType());

		}

		StructType schema = DataTypes.createStructType(structFields);

	}
}

class DateComparator implements Comparator<Date>, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public int compare(Date d1, Date d2) {
		return d1.compareTo(d2);
	}
}
