package org.training.spark.streaming;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.training.spark.util.JavaDBDao;
import org.training.spark.util.JavaRedisClient;
import org.training.spark.util.KafkaRedisConfig;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.util.*;

/**
 * Created by 张宝玉 on 2018/7/6.
 */
public class JavaJdOrderSumAnalytics {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("UserClickCountAnalytics");
        if (args.length == 0) {
            conf.setMaster("local[*]");
        }

        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(2));

        // Kafka configurations
        String[] topics = KafkaRedisConfig.KAFKA_ORDER3_TOPIC.split("\\,");
        System.out.println("Topics: " + Arrays.toString(topics));

        String brokers = KafkaRedisConfig.KAFKA_ADDR;
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("serializer.class", "kafka.serializer.StringEncoder");

        final String clickHashKey = "uses::total";

        // Create a direct stream
        JavaPairInputDStream<String, String> kafkaStream = KafkaUtils.createDirectStream(ssc,
                String.class, String.class,
                StringDecoder.class, StringDecoder.class,
                kafkaParams,
                new HashSet<String>(Arrays.asList(topics)));

        JavaDStream<JSONObject> events = kafkaStream.map(s -> JSON.parseObject(s._2()));

        JavaPairDStream<Integer, Double> orders = events.mapToPair(s -> {
            Integer uid = s.getInteger("uid");
            Double price = s.getDouble("price");
            Integer qty = s.getInteger("qty");
            Double discount = s.getDouble("discount");
            return new Tuple2<Integer, Double>(uid, price * qty - discount);
        });

        //从mysql表中读取用户信息userMap(uid,age)
        Map<Integer, Integer> userMap = JavaDBDao.getUserMap();
        Broadcast<Map<Integer, Integer>> broadcastMap = ssc.sparkContext().broadcast(userMap);

        JavaPairDStream<Integer, Double> ageOrder = orders.mapToPair(s -> {
            int age = broadcastMap.value().getOrDefault(s._1(), 0);
            return new Tuple2<Integer, Double>(age, s._2());
        }).reduceByKey((x,y)->x+y);

        ageOrder.foreachRDD(rdd -> {
            rdd.foreachPartition(partitionRdd -> {
                Jedis jedis = JavaRedisClient.get().getResource();
                while (partitionRdd.hasNext()) {
                    try {
                        Tuple2<Integer, Double> pair = partitionRdd.next();
                        Integer age = pair._1();
                        Double total = pair._2();
                        jedis.hincrByFloat(clickHashKey, "buy:"+age, total);
                        System.out.println("Update uid " + "buy:"+age+":"+total);
                    } catch (Exception e) {
                        System.out.println("error:" + e);
                    }
                }
                jedis.close();
            });
        });
        ssc.start();
        ssc.awaitTermination();
    }
}