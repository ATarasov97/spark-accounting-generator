package com;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class Generator {

    private static Random random = new Random();
    private static String[] regions = {"26", "77", "52", "39", "38"};

    private static String generateINN() {
        StringBuilder inn = new StringBuilder();
        inn.append(regions[random.nextInt(regions.length)]);
        for (int i = 0; i < 10; i++) {
            inn.append(random.nextInt(10));
        }
        return inn.toString();
    }

    private static String generateKPP() {
        StringBuilder kpp = new StringBuilder();
        for (int i = 0; i < 6; i++) {
            kpp.append(random.nextInt(10));
        }
        return kpp.toString();
    }


    public static void main(String[] args) {
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .config("spark.sql.warehouse.dir", warehouseLocation).master("local[*]")
                .enableHiveSupport()
                .getOrCreate();


        //set up the spark configuration and create contexts
//        SparkConf sparkConf = new SparkConf().setAppName("SparkSessionZipsExample").setMaster("local");
//        JavaSparkContext sc = new JavaSparkContext(sparkConf);
//        SQLContext spark = new org.apache.spark.sql.SQLContext(sc);


        spark.sql("CREATE TABLE IF NOT EXISTS seller1 (key INT, inn_1 STRING, kpp_1 INT, inn_2 STRING," +
                " kpp_2 INT, money DOUBLE, tax DOUBLE)");
        List<Record> records = new ArrayList<>();
        for (int key = 1; key < 100; key++) {
            String inn1 = generateINN();
            String inn2 = generateINN();
            String kpp1 = generateKPP();
            String kpp2 = generateKPP();
            double money = random.nextFloat();
            double tax = random.nextFloat();
            String select = "select " + key + " as key, " + inn1 + " as inn_1, " + kpp1 + " as kpp_1, " +
                    inn2 + " as inn_2, " + kpp2 + " as kpp_2, " + money +
                    " as money, " + tax + " as tax";
            Dataset<Row> recordsDF = spark.sql(select);
            recordsDF.write().mode("append").saveAsTable("seller1");
        }
        spark.sql("SELECT * FROM seller1");
    }
}