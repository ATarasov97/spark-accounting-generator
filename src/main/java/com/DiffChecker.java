package com;

import java.io.File;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DiffChecker {
  private static String s = "select 'customer' as table_name,\n" +
      "  inn_2 as inn_1,\n" +
      "  kpp_2 as kpp_1,\n" +
      "  inn_1 as inn_2,\n" +
      "  kpp_1 as kpp_2,\n" +
      "  money,\n" +
      "  tax\n" +
      "  from default.customer " +
      "minus " +
      "select * from seller\n" ;

  private static String SQL_STRING =
      "select\n" +
      "  t2.inn_1,\n" +
      "  t2.kpp_1,\n" +
      "  t2.inn_2,\n" +
      "  t2.kpp_2,\n" +
      "  t2.money,\n" +
      "  t2.tax\n" +
      " FROM\n" +
    //  "(\n" +      ") tmp\n" +
          "select\n" +
     // "'seller' as table_name,\n" +
      "  inn_1,\n" +
      "  kpp_1,\n" +
      "  inn_2,\n" +
      "  kpp_2,\n" +
      "  money,\n" +
      "  tax\n" +
      "  from default.seller) t1\n" +
      "  right join \n" +
      "  (select\n" +
     // "'customer' as table_name,\n" +
          "  inn_2 as inn_1,\n" +
          "  kpp_2 as kpp_1,\n" +
          "  inn_1 as inn_2,\n" +
      "  kpp_1 as kpp_2,\n" +

      "  money,\n" +
      "  tax\n" +
      "  from default.customer) t2 on t1.rownum = t2.rownum where t1.inn_2 <> t2.inn_2\n" +


  public static String SQL_MIST = "SELECT * from diff where table_name = 'customer'";


  public static String SQL_MIST_COUNT =
      "select tmp1.region as REG, " +
          "tmp1.COUNT as MISTAKES, " +
          "tmp2.KEK as ALL " +
          "from(" +

          "select region, sum(count) as COUNT from" +
          "(SELECT substr(inn_2,0,2) as region," +
          " count(*) as COUNT " +
          "from diff " +
          "where table_name = 'customer' \n" +
          "GROUP BY inn_2) tmp group by region ) tmp1 " +
          "right join " +
          "(select region , count(*) as KEK from " +
          "(select substr(inn_2,0,2) as region from default.customer) group by region) tmp2 " +
          "on tmp1.region = tmp2.region";


  public static void diffTableGenerate(SparkSession spark) {
    Dataset<Row> diffDF = spark.sql(s);
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
    //toCsv(df, "mistakes");
    df = spark.sql(SQL_MIST_COUNT);
    df.show();
    //toCsv(df, "mistakesCountByRegion");
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
    diffTableGenerate(spark);
    diffCsvGenerate(spark);
  }
}
