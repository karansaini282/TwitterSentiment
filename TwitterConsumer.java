import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.kafka.*;
import org.apache.spark.sql.*;
import kafka.serializer.StringDecoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.Set;
import java.util.Properties;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import java.sql.Timestamp;
import java.util.Date;

import scala.Tuple2;

public class TwitterConsumer implements Runnable{
    private static int sentSum;
    private static int sentCount;
    private static Map<String,Integer> dict;

    public TwitterConsumer()
    {
    	try
    	{
	    	sentSum=0;
	    	sentCount=0;
	        dict=new HashMap<String,Integer>();
	        
	    	File dir = new File(".");
	    	File fin = new File(dir.getCanonicalPath() + File.separator + "src/good.txt");
	    	FileInputStream fis = new FileInputStream(fin);
	    	BufferedReader br = new BufferedReader(new InputStreamReader(fis));
	     
	    	String line = null;
	    	while ((line = br.readLine()) != null) {
	    		dict.put(line,1);
	    	}
	     
	    	br.close();
	
	    	File dir2 = new File(".");
	    	File fin2 = new File(dir2.getCanonicalPath() + File.separator + "src/bad.txt");
	    	FileInputStream fis2 = new FileInputStream(fin2);
	    	BufferedReader br2 = new BufferedReader(new InputStreamReader(fis2));
	    	
	    	String line2 = null;
	    	while ((line2 = br2.readLine()) != null) {
	    		dict.put(line2,-1);
	    	}
	     
	    	br2.close();
    	}
    	catch(Exception e)
    	{
    		e.printStackTrace();
    	}
    }
    
    public static final Function<String,Integer> mapFunction=new Function<String,Integer>()
	{
		@Override
		public Integer call(String tweet)
		{
			System.out.println("Tweet:"+tweet);
			String[] words=tweet.split(" ");
			sentSum=0;			
        	for (String word:words)
        	{
        		if(dict.containsKey(word.toLowerCase()))
        		{
        		    sentCount++;
        			sentSum+=dict.get(word.toLowerCase());        			
        			System.out.println("word: " + word);        			        			
        		}
        	}
        	System.out.println("sentSum: " + sentSum);
			return sentSum;
		}
	};
	
    public static final Function<Tuple2<String,Integer>,Row> rowFunction=new Function<Tuple2<String,Integer>,Row>()
	{
		@Override
		public Row call(Tuple2<String,Integer> a)
		{
	        Date date = new Date();	        
			return RowFactory.create("topicName",a._2.toString(),new Timestamp(date.getTime()).toString());
		}
	};
	
    public static final Function2<Integer,Integer,Integer> reduceFunction=new Function2<Integer,Integer,Integer>()
	{
		@Override
		public Integer call(Integer a,Integer b)
		{			
			return a+b;
		}
	};
    
    public void run(){
        try{
        	  
        SparkConf conf = new SparkConf()
                .setAppName("kafka-consumer")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));
        
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        Set<String> topics = Collections.singleton("brandData");

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "admin");
        SQLContext sqlContext = new SQLContext(sc);
     // The schema is encoded in a string
        String schemaString = "topic sentiment timestamp";

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
          StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
          fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);
        
        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc,
                String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
                    
        directKafkaStream.mapValues(mapFunction).reduceByKey(reduceFunction).map(rowFunction).foreachRDD(rdd -> {
            Dataset<Row> df = sqlContext.createDataFrame(rdd,schema).na().drop();                    
            df.select("topic","sentiment","timestamp").write().mode("append").jdbc("jdbc:mysql://localhost:3306/db1?autoReconnect=true&useSSL=false", "db1.twitterSentiment", connectionProperties);
        	rdd.foreach(record -> {

            });            
        });            

        ssc.start();
        ssc.awaitTermination();
        }
        catch(Exception e)
        {
        	e.printStackTrace();
        }
    }
    
    public static void main(String[] args) 
    {
    	try
	    {
	        TwitterConsumer obj = new TwitterConsumer();
	        Thread t=new Thread(obj);
	        t.start();
	        t.join();
    	}
    	catch(Exception e)
    	{
    		e.printStackTrace();
    	}
    	
    }
}