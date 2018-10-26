package org.training.hadoop.kafka;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.training.spark.util.HiveClientUtils;
import org.training.spark.util.KafkaRedisConfig;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by 张宝玉 on 2018/7/6.
 */
public class JavaJDKafkaOrderEventProducer {
    private static Connection conn;
    private static PreparedStatement ps;
    private static ResultSet rs;

    public static void main(String[] args) throws Exception {
        Connection hiveConn = HiveClientUtils.getConnnection();

        String topic = KafkaRedisConfig.KAFKA_ORDER3_TOPIC;
        String brokers = KafkaRedisConfig.KAFKA_ADDR;
        Map<String, String> props = new HashMap<>();
        props.put("bootstrap.servers", brokers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer producer = new KafkaProducer(props);
        String sql = "select uid,price,qty,discount from t_order";

        try {
            ps = HiveClientUtils.prepare(hiveConn, sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                String uid = rs.getString("uid");
                Double price = rs.getDouble("price");
                Integer qty = rs.getInt("qty");
                Double discount = rs.getDouble("discount");

                JSONObject event = new JSONObject();
                event.put("uid", uid);
                event.put("price", price);
                event.put("qty", qty);
                event.put("discount", discount);
                producer.send(new ProducerRecord(topic, event.toString()));
                System.out.println(uid + "::" + price + "::" + qty+"::"+discount);
                Thread.sleep(10);
            }

        } catch (SQLException e) {

            e.printStackTrace();

        }
    }
}
