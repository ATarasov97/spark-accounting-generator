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


  public static String SQL_MIST_COUNT = "select region, sum(count) as COUNT from" +
      "(SELECT substr(inn_2,0,2) as region," +
      " count(*) as COUNT " +
      "from diff " +
      "where table_name = 'customer' \n" +
      "GROUP BY inn_2) tmp group by region";

  public static void diffTableGenerate(SparkSession spark) {
    Dataset<Row> diffDF = spark.sql(SQL_STRING);
    diffDF.show();
    spark.sql("DROP TABLE IF EXISTS diff");
    diffDF.write().mode("append").saveAsTable("diff");
  }

  public static void diffCounterGenerate(SparkSession spark) {
    Dataset<Row> diffDF = spark.sql(SQL_MIST);
    diffDF.show();
    spark.sql("DROP TABLE IF EXISTS mistakes");
    diffDF.write().mode("append").saveAsTable("mistakes");
    diffDF = spark.sql(SQL_MIST_COUNT);
    diffDF.show();
    spark.sql("DROP TABLE IF EXISTS mistakes_count");
    diffDF.write().mode("append").saveAsTable("mistakes_count");
  }

  public static void toCsv(Dataset<Row> df, String name) {
    df.write().csv(name + ".csv");
  }

  public static void diffCsvGenerate(SparkSession spark) {
    Dataset<Row> df = spark.sql("select * from mistakes");
    toCsv(df, "mistakes");
    df = spark.sql("select * from mistakes_count");
    toCsv(df, "mistakesCountByRegion");
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
    diffCsvGenerate(spark);
  }
}
