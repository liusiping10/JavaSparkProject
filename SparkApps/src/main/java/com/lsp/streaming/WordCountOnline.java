package com.lsp.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class WordCountOnline {
	public static void main(String[] args) {
		SparkConf conf=new SparkConf().
				setAppName("WordCountOnline").
				setMaster("local[2]");
		// local模式必须设置2个线程，用于接收和处理（一个线程只用于接收，无处理）
		// 集群模式，每个executor肯定不止一个线程，每个executor一般分配5个Cores
		
		JavaStreamingContext jsc=new JavaStreamingContext(conf, Durations.seconds(5));
		
		JavaDStream<String> lines=jsc.socketTextStream("master", 9999);
		
		/*
		 * print不会直接触发job的执行
		 * 对于Spark Streaming而言具体是否触发真正job的运行是基于
		 * 设置的Duration时间间隔的
		 * Spark Streaming应用要想执行具体的job,对DStream就必须outpu Stream操作
		 * output Stream有很多类型的函数触发：print,save
		 * 最重要的是foreachRDD
		 * Spark Streaming处理的结果一般放在Redis,DB,DashBoard
		 * foreachRDD主要就是用来完成这些功能，而且可以随意自定义具体数据存放的位置
		 */
		lines.print();
		
		/**
		 * Spark Streaming执行引擎是Driver开始运行，Driver启动的时候位于一条新的线程中
		 * 其内部有消息接收应用程序本身或者Executor中的消息
		 */
		jsc.start();
		jsc.awaitTermination();
		jsc.close();
	}
}
