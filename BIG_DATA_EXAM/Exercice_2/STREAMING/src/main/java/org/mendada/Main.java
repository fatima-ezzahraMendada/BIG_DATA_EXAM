package org.mendada;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

public class Main {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        // Configuration Spark
        SparkSession spark = SparkSession.builder()
                .appName("PlanesWithMostIncidents")
                .master("local[*]")
                .getOrCreate();


        // Read incidents data format CSV
        Dataset<Row> incidentsDF = spark.read()
                .format("csv")
                .option("header", true)
                .load("incidents.csv");

        // Create temporary view to perform SQL requests 
        incidentsDF.createOrReplaceTempView("incidents");

        //SQL requests to get the airplane with much incidents
        String query = "SELECT no_avion, COUNT(*) AS incident_count " +
                "FROM incidents " +
                "GROUP BY no_avion " +
                "ORDER BY incident_count DESC " +
                "LIMIT 1";
        
        //Execute SQL request
        Dataset<Row> result = spark.sql(query);

        //Display result
        result.show();

        
        spark.close();

    }
}