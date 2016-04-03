package com.lsp.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class RDDCogroup {
	public static void main(String[] args) {
		SparkConf conf=new SparkConf().setAppName("rdd cogroup").setMaster("local");
		
		JavaSparkContext sc=new JavaSparkContext(conf);
		
		List<Tuple2<Integer,String>> namelist=Arrays.asList(
				new Tuple2<Integer,String>(1, "Spark"),
				new Tuple2<Integer,String>(2, "Tachyon"),
				new Tuple2<Integer,String>(3, "Hadoop")
		);
		
		List<Tuple2<Integer,Integer>> scorelist=Arrays.asList(
				new Tuple2<Integer,Integer>(1, 100),
				new Tuple2<Integer,Integer>(2, 90),
				new Tuple2<Integer,Integer>(3, 70),
				new Tuple2<Integer,Integer>(1, 90),
				new Tuple2<Integer,Integer>(2, 95),
				new Tuple2<Integer,Integer>(2, 60)
		);
		
		JavaPairRDD<Integer, String>  names=sc.parallelizePairs(namelist);
		JavaPairRDD<Integer, Integer>  scores=sc.parallelizePairs(scorelist);
		
		JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>
		nameAndScores=names.cogroup(scores);
		
		nameAndScores.foreach(new VoidFunction<Tuple2<Integer,Tuple2<Iterable<String>,Iterable<Integer>>>>() {
			
			public void call(
					Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> tuple)
					throws Exception {
				System.out.println("ID="+tuple._1+",Name="+tuple._2._1+",Score="+tuple._2._2);
			}
		});
		
		/*
		 * 	ID=1,Name=[Spark],Score=[100, 90]
			ID=3,Name=[Hadoop],Score=[70]
			ID=2,Name=[Tachyon],Score=[90, 95, 60]
		 */
		sc.stop();
	}
}
