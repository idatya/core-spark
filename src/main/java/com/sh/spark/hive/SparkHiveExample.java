package com.sh.spark.hive;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkHiveExample {

    public static void main(String[] args) throws AnalysisException {
        SparkSession session = getHiveContext();
        dataFrameBasicExample(session);
    }

    private static SparkSession getHiveContext() throws AnalysisException {
        SparkSession session = SparkSession.builder().appName("TestApp")
                .config("spark.sql.warehouse.dir", "/apps/hive/warehouse").config("spark.master", "local")
                .config("deploy-mode", "cluster")
                // .config("hive.metastore.uris", "spark://192.168.218.63:9083")
                //.master(String master)
                //Sets the Spark master URL to connect to, such as "local" to run locally, "local[4]" to run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
                .config("hive.metastore.uris", "thrift://192.168.218.63:9083").config("spark.logConf", true)
                .config("spark.dynamicAllocation.enabled", true).enableHiveSupport().getOrCreate();
        return session;

    }

    private static void dataFrameBasicExample(SparkSession session) throws AnalysisException {
        Dataset<Row> std = session.sql("select * from trial.student");
        std.show();
        std.printSchema();
        // Select only the "studentid" column
        std.select("studentid").show();
        std.select("studentid", "studentname").show();
        std.select(std.col("marks").plus(1)).show();
        std.withColumn("marks", std.col("marks").gt("60.0")).show();
        std.filter(std.col("marks").gt(60.0)).show();

        // df.groupBy("studentid").count().show();

        std.createOrReplaceTempView("studentView");
        Dataset<Row> viewDF = session.sql("SELECT * FROM studentView");
        viewDF.show();

        // Register the DataFrame as a global temporary view
        std.createGlobalTempView("studentView");

        // Global temporary view is tied to a system preserved database
        // `global_temp`
        session.sql("SELECT * FROM global_temp.studentView").show();

        // Global temporary view is cross-session
        session.newSession().sql("SELECT * FROM global_temp.studentView").show();
    }
}
