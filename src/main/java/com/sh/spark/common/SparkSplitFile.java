package com.sh.spark.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSplitFile {
    public static void main(String[] args) throws IOException {
        SparkSession session = getSparkSession();
        splitFileOnHdfs(session);
    }

    private static void splitFileOnHdfs(SparkSession session) {
        Dataset<Row> df = session.read()
                .text("hdfs://impetus-1330.impetus.co.in:8020/user/impadmin/assessment/VZ_sample_100.csv");
        df.show();
        df.show(false);
        List<Row> rowslist = df.collectAsList();
        int size = rowslist.size();
        System.out.println(rowslist.size());
        int i = 0;
        int j = 1;
        int k = 0;
        Dataset<Row> dfNew = session.emptyDataFrame();
        List<Row> temp = new ArrayList<Row>();
        for (Row row : rowslist) {
            k++;
            System.out.println(row);
            temp.add(row);
            i++;
            if (i == 1000 || k == size) {
                dfNew = session.createDataFrame(temp, df.schema());
                dfNew.write().text("hdfs://impetus-1330.impetus.co.in:8020/user/impadmin/assessment/split_data22" + j);
                i = 0;
                j++;
                temp = new ArrayList<Row>();
            }
        }
    }

    public static SparkSession getSparkSession() throws IOException {
        System.setProperty("java.security.krb5.conf", "/home/impadmin/Downloads/ESI/conf/krb5.conf");
        UserGroupInformation.loginUserFromKeytab("impadmin@IMPETUS.CO.IN","/home/impadmin/Downloads/ESI/conf/impadmin.keytab");
        SparkConf conf = new SparkConf();
        conf.setMaster("local[1]");
        conf.setAppName("SparkSplitJob");
        SparkSession session = SparkSession.builder().config(conf).getOrCreate();
        return session;
    }

}
