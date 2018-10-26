package org.training.hadoop.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.codehaus.jettison.json.JSONObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * Created by 张宝玉 on 2018/7/5.
 */
public class OrderProducer2 {
    private static String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    //    private static String PRESTO_DRIVER = "com.facebook.presto.jdbc.PrestoDriver";
    static final String TOPIC_CLICK = "t_order2";

    public static void main(String[] args) throws Exception {

        //Kafka master节点属性
        Properties props = new Properties();
        props.put("bootstrap.servers", "master:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);

        //新建生产者
        final Producer<String, String> producer = new KafkaProducer<String, String>(
                props, new StringSerializer(), new StringSerializer());
        try {//确认Hive驱动存在
            Class.forName(HIVE_DRIVER);
//            Class.forName(PRESTO_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        //连接Hive
        Connection conn = DriverManager.getConnection(
                "jdbc:hive2://master:10000/jd2", "zkpk", "zkpk");
//                "jdbc:presto://c7402:8080/hive/jd","vagrant", null);
        //查询t_click表
        Statement stmt = conn.createStatement();
        String sql = "select u.age, (o.price*o.qty-o.discount) sum, o.buy_time from t_order as o join t_user as u on o.uid=u.uid order by o.buy_time";
        ResultSet res = stmt.executeQuery(sql);

        while (res.next()) {
            //生成消息
            Integer age = res.getInt("age");
            Double sum = res.getDouble("sum");
            String buy_time = res.getString("buy_time");

            System.out.println("Update user " + age + " to " + sum);
            JSONObject event = new JSONObject();
            event.put("age", age);
            event.put("buy_time", buy_time);
            event.put("sum", sum);
            //发送消息
            producer.send(new ProducerRecord(TOPIC_CLICK,event.toString()));
            System.out.println(age + "::" + buy_time + "::" + sum);
            //延时10毫秒
            Thread.sleep(1000);
        }
    }
}
