package com;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.xml.crypto.Data;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class Generator {

    private static Random random = new Random();
    private static String[] regions = {"26", "77", "52", "39", "38"};

    private String generateINN() {
        StringBuilder inn = new StringBuilder();
        inn.append(regions[random.nextInt(regions.length)]);
        for (int i = 0; i < 10; i++) {
            inn.append(random.nextInt(10));
        }
        return inn.toString();
    }

    private String generateKPP() {
        StringBuilder kpp = new StringBuilder();
        for (int i = 0; i < 6; i++) {
            kpp.append(random.nextInt(10));
        }
        return kpp.toString();
    }

    public void generateSellerAndCustomerTables(SparkSession spark) {
        spark.sql("DROP TABLE IF EXISTS seller");
        spark.sql("DROP TABLE IF EXISTS customer");
        List<Record> seller = new ArrayList<>();
        List<Record> customer = new ArrayList<>();

        for (int key =1; key < 1000000; key++) {
            String inn1 = generateINN();
            String inn2 = generateINN();
            String kpp1 = generateKPP();
            String kpp2 = generateKPP();
            double money = random.nextFloat();
            double tax = random.nextFloat();
            Record sellerRecord = new Record();
            sellerRecord.setKey(key);
            sellerRecord.setInn_1(inn1);
            sellerRecord.setKpp_1(kpp1);
            sellerRecord.setInn_2(inn2);
            sellerRecord.setKpp_2(kpp2);
            sellerRecord.setMoney(money);
            sellerRecord.setTax(tax);
            seller.add(sellerRecord);
            Record customerRecord = new Record();
            customerRecord.setKey(key);
            customerRecord.setInn_1(inn2);
            customerRecord.setKpp_1(kpp2);
            if (random.nextFloat() < 0.05) {
                customerRecord.setInn_2(inn1.replace("0", "8"));
            }
            customerRecord.setKpp_2(kpp1);
            customerRecord.setMoney(money);
            customerRecord.setTax(tax);
            customer.add(customerRecord);
        }

        Dataset<Row> sellerDF = spark.createDataFrame(seller, Record.class);
        sellerDF.write().mode("append").saveAsTable("seller");
        Dataset<Row> customerDF = spark.createDataFrame(customer, Record.class);
        customerDF.write().mode("append").saveAsTable("customer");

    }


    public static void main(String[] args) {
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .config("spark.sql.warehouse.dir", warehouseLocation).master("local[*]")
                .enableHiveSupport()
                .getOrCreate();

        Generator generator = new Generator();
        generator.generateSellerAndCustomerTables(spark);

        //Dataset<Row> sellerShow = spark.sql("SELECT * FROM default.seller");
       // sellerShow.show();
       // Dataset<Row> customerShow = spark.sql("SELECT * FROM default.customer");
        //customerShow.show();
    }
}