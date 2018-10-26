package org.training.hadoop.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * Created by 张宝玉 on 2018/7/5.
 */
public class ClickProducer {
    private static String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    //    private static String PRESTO_DRIVER = "com.facebook.presto.jdbc.PrestoDriver";
    static final String TOPIC_CLICK = "t_click";

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
        try {
            //确认Hive驱动存在
            Class.forName(HIVE_DRIVER);
//            Class.forName(PRESTO_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        //连接Hive
        Connection conn = DriverManager.getConnection(
                "jdbc:hive2://master:10000/jd2", "zkpk", "zkpk");
//                "jdbc:presto://c7402:8080/hive/jd","zkpk", null);
        //查询t_click表
        Statement stmt = conn.createStatement();
        long startTime = System.currentTimeMillis();
        String sql = "select uid, click_time, pid from t_click order by click_time";
        ResultSet res = stmt.executeQuery(sql);
        int count = 0;
        while (res.next()) {
            count++;
            //生成消息
            int uid = res.getInt("uid");
            Date click_time=new Date(res.getTimestamp("click_time").getTime());
            SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String timestr = fmt.format(click_time);
            int pid = res.getInt("pid");

            System.out.println("Update page " + pid + " clicked by " + uid);
            //发送消息
            producer.send(new ProducerRecord<String, String>(TOPIC_CLICK,
                    ""+uid, timestr + "," + pid
            ));
            //延时10毫秒
            Thread.sleep(10);
        }
        long stopTime = System.currentTimeMillis();
        System.out.println("click time: " + (stopTime - startTime) + ", count : " + count);
    }
}
