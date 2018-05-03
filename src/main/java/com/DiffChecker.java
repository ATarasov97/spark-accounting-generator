package com;

import java.io.File;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DiffChecker {
  private static String SQL_STRING = "select\n" +
      "MIN(table_name) as table_name,\n" +
      "  inn_1,\n" +
      "  kpp_1,\n" +
      "  inn_2,\n" +
      "  kpp_2,\n" +
      "  money,\n" +
      "  tax\n" +
      " FROM\n" +
      "(\n" +
      "  select\n" +
      "'seller' as table_name,\n" +
      "  inn_1,\n" +
      "  kpp_1,\n" +
      "  inn_2,\n" +
      "  kpp_2,\n" +
      "  money,\n" +
      "  tax\n" +
      "  from default.seller\n" +
      "  union all\n" +
      "  select\n" +
      "'customer' as table_name,\n" +
      "  inn_1,\n" +
      "  kpp_1,\n" +
      "  inn_2,\n" +
      "  kpp_2,\n" +
      "  money,\n" +
      "  tax\n" +
      "  from default.customer\n" +
      ") tmp\n" +
      "group by\n" +
      "  inn_1,\n" +
      "  kpp_1,\n" +
      "  inn_2,\n" +
      "  kpp_2,\n" +
      "  money,\n" +
      "  tax\n" +
      "having count(*) = 1  \n";

  public static String SQL_MIST = "SELECT * from diff where table_name = 'customer'";


  public static String SQL_MIST_COUNT = "SELECT inn_2 as customer, count(*) as COUNT from diff where table_name = 'customer' \n"+
      "GROUP BY inn_2";

  public static void DiffTableGenerate(SparkSession spark) {
    Dataset<Row> diffDF = spark.sql(SQL_STRING);
    diffDF.show();
    spark.sql("DROP TABLE IF EXISTS diff");
    diffDF.write().mode("append").saveAsTable("diff");
  }

  public static void DiffCounterGenerate(SparkSession spark) {
    Dataset<Row> diffDF = spark.sql(SQL_MIST);
    diffDF.show();
    spark.sql("DROP TABLE IF EXISTS mistakes");
    diffDF.write().mode("append").saveAsTable("mistakes");
    diffDF = spark.sql(SQL_MIST_COUNT);
    diffDF.show();
    spark.sql("DROP TABLE IF EXISTS mistakes_count");
    diffDF.write().mode("append").saveAsTable("mistakes_count");
  }

  public static void main(String[] args) {
    String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
    SparkSession spark = SparkSession
        .builder()
        .appName("Java Spark Hive Example")
        .config("spark.sql.warehouse.dir", warehouseLocation).master("local[*]")
        .enableHiveSupport()
        .getOrCreate();
    //Generator g = new Generator();
    //g.generateSellerAndCustomerTables(spark);
    //DiffTableGenerate(spark);
    DiffCounterGenerate(spark);
  }
}
