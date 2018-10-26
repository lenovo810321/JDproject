package org.training.spark.streaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
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
public class JdUserClickCountAnalytics {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("UserClickCountAnalytics");
        if (args.length == 0) {
            conf.setMaster("local[2]");
        }

        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // Kafka configurations
        String[] topics = KafkaRedisConfig.KAFKA_USER_TOPIC.split("\\,");
        System.out.println("Topics: " + Arrays.toString(topics));

        String brokers = KafkaRedisConfig.KAFKA_ADDR;
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("serializer.class", "kafka.serializer.StringEncoder");

        final String clickHashKey = "click+";

        // Create a direct stream
        JavaPairInputDStream<String, String> kafkaStream = KafkaUtils.createDirectStream(ssc,
                String.class, String.class,
                StringDecoder.class, StringDecoder.class,
                kafkaParams,
                new HashSet<String>(Arrays.asList(topics)));

        JavaDStream events = kafkaStream.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> line) throws Exception {
                System.out.println("line:" + line._2());
                String data = line._2();
                return data;
            }
        });

        // Compute user click times
        JavaPairDStream<String, Long> pageClicks = events.mapToPair(x -> {
            String pid = "0";
            String[] list = x.toString().split(",");
            if (list.length == 2) {
                pid = list[1];
            }
            return new Tuple2<>(pid, 1L);
        }).reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long i1, Long i2) {
                return i1 + i2;
            }
        });
        pageClicks.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
            @Override
            public void call(JavaPairRDD<String, Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> partitionOfRecords) throws Exception {
                        Jedis jedis = JavaRedisClient.get().getResource();
                        while(partitionOfRecords.hasNext()) {
                            try {
                                Tuple2<String, Long> pair = partitionOfRecords.next();
                                String pid = pair._1 ();
                                long clickCount = pair._2();
                                jedis.hincrBy(clickHashKey, pid, clickCount);
                                System.out.println("Update pid " + pid + " to " + clickCount);
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
