package com.lsp.spark;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 分组TOP N算法 不同类型中的每种类型数据的TOP N元素
 * 
 * @author liusp
 *
 */
public class TopNGroup {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("TopNGroup").setMaster(
				"local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile("D://data/TopNGroup.txt");

		// 把每行数据变成符合要求的key-value方式
		JavaPairRDD<String, Integer> pairs = lines
				.mapToPair(new PairFunction<String, String, Integer>() {

					public Tuple2<String, Integer> call(String line)
							throws Exception {
						String[] splitedLine = line.split(" ");
						return new Tuple2<String, Integer>(splitedLine[0],
								Integer.valueOf(splitedLine[1]));
					}
				});

		JavaPairRDD<String, Iterable<Integer>> groupedPairs = pairs
				.groupByKey();

		JavaPairRDD<String, Iterable<Integer>> top5 = groupedPairs
				.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<String, Iterable<Integer>> call(
							Tuple2<String, Iterable<Integer>> groupedData)
							throws Exception {
						Integer top5[] = new Integer[5];// 保存top5的数据本身
						String groupedKey = groupedData._1;// 获取分组的组名
						// 获取每组的内容集合
						Iterator<Integer> groupedValue = groupedData._2
								.iterator();

						while (groupedValue.hasNext()) {// 查看是否有下一个元素，如果有，则继续进行循环
							Integer value = groupedValue.next();// 获取当前循环的元素本身的内容
							// 具体实现分组内部的TopN
							for (int i = 0; i < 5; i++) {
								if (top5[i] == null) {
									top5[i] = value;
									break;
								} else if (value > top5[i]) {
									for (int j = 4; j > i; j--) {
										top5[j] = top5[j - 1];
									}
									top5[i] = value;
									break;
								}
							}
						}
						return new Tuple2<String, Iterable<Integer>>(
								groupedKey, Arrays.asList(top5));
					}
				});

		JavaPairRDD<String, Iterable<Integer>> top5Sorted = top5.sortByKey();

		top5Sorted
				.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {

					private static final long serialVersionUID = 1L;

					public void call(Tuple2<String, Iterable<Integer>> topped)
							throws Exception {
						// 获取Group key
						System.out.println("Group Key:" + topped._1);
						Iterator<Integer> topValue = topped._2.iterator();

						while (topValue.hasNext()) {
							Integer value = topValue.next();
							System.out.println(value);
						}
						System.out.println("*************************");
					}
				});
	}
}
