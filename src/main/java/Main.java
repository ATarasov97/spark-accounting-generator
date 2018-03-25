import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;

public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("inn")
                .getOrCreate();
    }
}