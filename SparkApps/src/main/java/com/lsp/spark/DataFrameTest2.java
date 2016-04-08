package com.lsp.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;


public class DataFrameTest2 {

    public static void main(String[] args) {

        SparkConf conf=new SparkConf().setMaster("local").setAppName("test");

        JavaSparkContext sc= new JavaSparkContext(conf);
        
        SQLContext sqlContext=new SQLContext(sc);
        
        // 动态构造DataFrame的元数据，来自数据库或者json文件
        List<StructField> structFields=new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        
        // 构建StructType，用于最后DataFrame元数据的描述
        StructType structType=DataTypes.createStructType(structFields);
        
        RDD personsRDD = null;
		// 基于MetaData以及RDD<Row>来构造DataFrame
        DataFrame personsDF=sqlContext.createDataFrame(personsRDD, structType);
        
        // 注册临时表，供后续的SQL查询操作
        personsDF.registerTempTable("persons");
        
        // 对数据进行多维度分析
        DataFrame result=sqlContext.sql("select * from persons where age>8");
    }
}
