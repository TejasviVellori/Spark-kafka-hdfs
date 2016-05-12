package sparkstreamingkafka;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;



public class SparkKafka {
	 private static final Pattern SPACE = Pattern.compile(" ");

	  public static void main(String[] args) throws Exception {
	   

	  

	    String brokers = "52.90.173.221:9092,54.208.27.4:9093";
	    String topics = "stringdata";

	    // Create context with a 2 seconds batch interval
	    SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount").setMaster("local[2]").set("spark.driver.allowMultipleContexts", "true");
	    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

	    Set<String> topicsSet = new HashSet(Arrays.asList(topics.split(",")));
	    Map<String, String> kafkaParams = new HashMap();
	    kafkaParams.put("metadata.broker.list", brokers);
	    kafkaParams.put("zookeeper.connect", "52.90.173.221:2181");
	  
	    // Create direct kafka stream with brokers and topics
	    JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc,String.class,String.class,StringDecoder.class,StringDecoder.class,kafkaParams,topicsSet);

	    // Get the lines, split them into words, count the words and print
	    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
	      
	      public String call(Tuple2<String, String> tuple2) {
	        return tuple2._2();
	      }
	    });
	    lines.print();
	    JavaDStream<String> words = lines.flatMap(
	  		  new FlatMapFunction<String, String>() {
	  		 public Iterable<String> call(String x) {
	  		      return Arrays.asList(x.split(" "));
	  		    }
	  		  });    
	    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
	      new PairFunction<String, String, Integer>() {
	        
	        public Tuple2<String, Integer> call(String s) {
	          return new Tuple2(s, 1);
	        }
	      }).reduceByKey(
	        new Function2<Integer, Integer, Integer>() {
	        
	        public Integer call(Integer i1, Integer i2) {
	          return i1 + i2;
	        }
	      });
	    wordCounts.print();
	    wordCounts.dstream().saveAsTextFiles("hdfs://master:9000/sparkkafka/wc", "txt");
	    // Start the computation
	    jssc.start();
	    jssc.awaitTermination();
	    jssc.stop();
	  }
}
