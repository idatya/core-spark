package com.sh.spark.java;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class SparkSQLJavaExample {
	
	public static class Person implements Serializable {
        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }
	
	public static void main(String[] args) {
        SparkSession session = getSparkSession();
        readFromHdfs(session);
        readFromLocal(session);
        datasetCreationExample(session);
    }
	
	public static SparkSession getSparkSession() {
		SparkSession session = SparkSession.builder().master("local[2]").config("deploy-mode", "cluster")
				.appName("DataScienceExample").getOrCreate();
		return session;
	}

    
    
    private static void readFromHdfs(SparkSession session) {
        Dataset<Row> df = session.read().text("hdfs://192.168.218.63:8020/user/impadmin/assessment/VZ_sample_100.csv");
        Dataset<String> dfString = session.read().textFile("hdfs://192.168.218.63:8020/user/impadmin/assessment/VZ_sample_100.csv");
        //dfString = session.read().text("hdfs://192.168.218.63:8020/user/impadmin/assessment/VZ_sample_100.csv"); //compile time error
        df.show();
        df.show(false);
    }
    

    private static void readFromLocal(SparkSession session) {
        Dataset<Row> df = session.read().text("/home/impadmin/aditya/data/test3_clean.csv");
        df.show();
        df.show(false);

        Dataset<Row> df2 = session.read().text("file:///home/impadmin/aditya/data/test3_clean.csv");
        df2.show();
        df2.show(false);

        Dataset<Row> df3 = session.read().csv("file:///home/impadmin/aditya/data/test3_clean.csv");
        df3.show();
        df3.show(false);
    }
    
    private static void datasetCreationExample(SparkSession session) {
        Person person = new Person();
        person.setName("aditya");
        person.setAge(40);

        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = session.createDataset(Collections.singletonList(person), personEncoder);
        javaBeanDS.show();

        // Encoders for most common types are provided in class Encoders

        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDS = session.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
        Dataset<Integer> transformedDS = primitiveDS.map((MapFunction<Integer, Integer>) val -> val + 1,
                integerEncoder);
        transformedDS.collect(); // Returns [2, 3, 4]
        transformedDS.show();
        /* type safty exaple od dataset
        primitiveDS = transformedDS;
        Dataset<String> dfString = session.read().textFile("hdfs://192.168.218.63:8020/user/impadmin/assessment/VZ_sample_100.csv");
        primitiveDS = dfString;
        */
        // DataFrames can be converted to a Dataset by providing a class.
        // Mapping based on name
        String path = "/home/impadmin/aditya/data/person.json";
        Dataset<Row> df = session.read().json("/home/impadmin/aditya/data/person.json");
        df.show();
        Dataset<Person> peopleDS = session.read().json(path).as(personEncoder);
        peopleDS.show();

    }
}
