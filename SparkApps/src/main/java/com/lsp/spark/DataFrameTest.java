package com.lsp.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;


public class DataFrameTest {

    public static void main(String[] args) {

        SparkConf conf=new SparkConf().setMaster("local").setAppName("test");

        JavaSparkContext sc= new JavaSparkContext(conf);
        
        SQLContext sqlContext=new SQLContext(sc);
        
        /*
         * {"name":"Michael","score":92}
         * {"name":"Andy", "score":89}
         * {"name":"Justin", "score":95}
         */
        DataFrame peopleScoreDF=sqlContext.read().json("D:\\spark-1.6.0\\examples\\src\\main\\resources\\peopleScore.json");
        peopleScoreDF.registerTempTable("peopleScore");
        
        DataFrame execScoresDF=sqlContext.sql("select name,score from peopleScore where score>90");
        
        // 在DataFrame的基础上转化为RDD，通过Map操作计算出分数大于90的所有人的姓名
        List<String> execNameList=execScoresDF.javaRDD().map(new Function<Row, String>() {

			public String call(Row row) throws Exception {
				return row.getAs("name");
			}
		}).collect();
        
        // 拼出JSON
        List<String> peopInfos=new ArrayList<String>();
        peopInfos.add("{\"name\":\"Michael\",\"age\":20}");
        peopInfos.add("{\"name\":\"Andy\",\"age\":17}");
        peopInfos.add("{\"name\":\"Justin\",\"age\":19}");
        
        // 通过JSON的RDD来构造DataFrame
        JavaRDD<String> peopInfoRDD=sc.parallelize(peopInfos);
        DataFrame peopInfoDF=sqlContext.read().json(peopInfoRDD);
        
        // 注册为临时表
        peopInfoDF.registerTempTable("peopInfos");
        
        StringBuffer sb=new StringBuffer("select name,age from peopInfos where name in (");
        for(int i=0;i<execNameList.size();i++){
        	sb.append("'"+execNameList.get(i)+"'");
        	if(i<execNameList.size()-1){
        		sb.append(",");
        	}
        }
        sb.append(")");
        
        DataFrame execNameAgeDF=sqlContext.sql(sb.toString());
        
        JavaPairRDD<String, Tuple2<Long, Long>> 
        resultRDD=execScoresDF.javaRDD().mapToPair(new PairFunction<Row, String, Long>() {
        	public Tuple2<String, Long> call(Row row) throws Exception {
				return new Tuple2<String, Long>((String)row.getAs("name"),(Long)row.getAs("score"));
			}
		}).join(execNameAgeDF.javaRDD().mapToPair(new PairFunction<Row, String, Long>() {
			public Tuple2<String, Long> call(Row row) throws Exception {
				return new Tuple2<String, Long>((String)row.getAs("name"),(Long)row.getAs("age"));
			}
		}));
        
        JavaRDD<Row> rowRDD=resultRDD.map(new Function<Tuple2<String,Tuple2<Long,Long>>, Row>() {

			public Row call(Tuple2<String, Tuple2<Long, Long>> tuple)
					throws Exception {
				return RowFactory.create(tuple._1,tuple._2._1,tuple._2._2);
			}
		});
        
        // 结果rowRDD
        rowRDD.foreach(new VoidFunction<Row>() {
			
			public void call(Row row) throws Exception {
				System.out.println(row.get(0)+"\t"+row.get(1)+"\t"+
						row.get(2));
			}
		});
    }
}
