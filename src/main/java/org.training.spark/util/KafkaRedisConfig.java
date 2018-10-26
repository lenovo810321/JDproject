package org.training.spark.util;

/**
 * Created by 张宝玉 on 2018/7/5.
 */
public class KafkaRedisConfig {
    public static String REDIS_SERVER = "localhost";
    public static int REDIS_PORT = 6379;

    public static String KAFKA_SERVER = "master";
    public static String KAFKA_ADDR = KAFKA_SERVER + ":9092";

    public static String KAFKA_USER_TOPIC = "t_click";
    public static String KAFKA_ORDER_TOPIC = "t_order";
    public static String KAFKA_ORDER2_TOPIC = "t_order2";
    public static String KAFKA_ORDER3_TOPIC = "t_order3";

    public static String ZOOKEEPER_SERVER = "master:2181";
    public static String ZOOKEEPER_PATH = "/offsets";
}
