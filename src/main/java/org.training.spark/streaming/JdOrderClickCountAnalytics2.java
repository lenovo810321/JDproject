package org.training.spark.streaming;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.training.spark.util.JavaRedisClient;
import org.training.spark.util.KafkaRedisConfig;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.util.*;

/**
 * Created by 张宝玉 on 2018/7/5.
 */
public class JdOrderClickCountAnalytics2 {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("OrderClickCountAnalytics");
        if (args.length == 0) {
            conf.setMaster("local[1]");
        }

        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // Kafka configurations
        String[] topics = KafkaRedisConfig.KAFKA_ORDER2_TOPIC.split("\\,");
        System.out.println("Topics: " + Arrays.toString(topics));

        String brokers = KafkaRedisConfig.KAFKA_ADDR;
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("serializer.class", "kafka.serializer.StringEncoder");

        final String clickHashKey = "buy+";

        // Create a direct stream
        JavaPairInputDStream<String, String> kafkaStream = KafkaUtils.createDirectStream(ssc,
                String.class, String.class,
                StringDecoder.class, StringDecoder.class,
                kafkaParams,
                new HashSet<String>(Arrays.asList(topics)));

        JavaDStream events = kafkaStream.map(new Function<Tuple2<String, String>, JSONObject>() {
            @Override
            public JSONObject call(Tuple2<String, String> line) throws Exception {
                System.out.println("line:" + line._2());
                JSONObject data = JSON.parseObject(line._2());
                return data;
            }
        });

        // Compute user click times
        JavaPairDStream<String, Double> orderSum = events.mapToPair(
                new PairFunction<JSONObject, String, Double>() {
                    @Override
                    public Tuple2<String, Double> call(JSONObject x) {
                        return new Tuple2<>(x.getString("age"), x.getDouble("sum"));
                    }
                }).reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double i1, Double i2) {
                return i1 + i2;
            }
        });
        orderSum.foreachRDD(new VoidFunction<JavaPairRDD<String, Double>>() {
            @Override
            public void call(JavaPairRDD<String, Double> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Double>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Double>> partitionOfRecords) throws Exception {
                        Jedis jedis = JavaRedisClient.get().getResource();
                        while(partitionOfRecords.hasNext()) {
                            try {
                                Tuple2<String, Double> pair = partitionOfRecords.next();
                                String age = pair._1();
                                double sum = pair._2();
                                jedis.hincrByFloat(clickHashKey, age, sum);
                                System.out.println("Update age " + age + " to " + sum);
                            } catch(Exception e) {
                                System.out.println("error:" + e);
                            }
                        }
                        jedis.close();
                    }
                });
            }
        });

        ssc.start();
        ssc.awaitTermination();
    }
}
