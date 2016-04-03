package com.lsp.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

public class SparkSQLJDBC {
	public static void main(String[] args) {
		SparkConf conf=new SparkConf().setAppName("spark wordcount writtern by java").setMaster("local");
		
		JavaSparkContext sc=new JavaSparkContext(conf);
		
		SQLContext sqlContext=new SQLContext(sc);
		
		DataFrameReader reader=sqlContext.read().format("jdbc");
		reader.option("url", "jdbc:mysql://localhost:3306");
		reader.option("dbtable", "jdbc:mysql://localhost:3306");
		reader.option("driver", "com.mysql.jdbc.Driver");
		reader.option("user", "root");
		reader.option("password", "root");
		
		DataFrame mysqlDF=reader.load();
		
		
		sc.close();
	}
}
