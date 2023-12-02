package org.cloud.data;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

public class ReadDeltaTable {
    public static void main(String[] args) throws NoSuchTableException {
        System.out.println("Hello World");
        System.setProperty("hadoop.home.dir", "D:\\sparksetup\\hadoop");
        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("local[*]")
                //.set("spark.jars.packages", ",io.delta:delta-core_2.12:2.4.0")
                .set("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
                .set("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .set("spark.sql.warehouse.dir", "file:///D:/sparksetup/spark_warehouse");
        SparkSession spark = SparkSession.builder().appName("Example Iceberg Spark App").config(sparkConf).getOrCreate();
        spark.read().format("delta").load("D:\\sparksetup\\sparkdata\\trips").show();
    }
}
