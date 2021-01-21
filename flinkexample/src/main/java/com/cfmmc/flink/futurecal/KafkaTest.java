package com.cfmmc.flink.futurecal;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class KafkaTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "13.76.185.14:9092");
        properties.setProperty("group.id", "test-consumer-group-new");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("first_topic", new SimpleStringSchema(), properties);
        //从最早开始消费
        consumer.setStartFromEarliest();
        DataStream<String> rawOrderJson = env
                .addSource(consumer);
        rawOrderJson.print();

        // 反序列化
        DataStream<Order> orderStream = rawOrderJson.map(rec ->  JSONObject.parseObject(rec, Order.class))
                .map(rec -> {
                    rec.setTimestampSecond(rec.getTimestamp()/1000);
                    rec.setOrderPay(rec.getPrice() * rec.getNumber());
                    return rec;
                });

        DataStream<String> orderString = orderStream.map(rec -> "Step2: " + rec.toString());
//        orderString.print();

        DataStream<FuturePrice> futurePriceStream = orderStream.keyBy(
                new KeySelector<Order, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> getKey(Order value) throws Exception {
                        return Tuple2.of(value.getFutureVariety(), value.getTimestampSecond());
                    }
                })
                .timeWindow(Time.of(10, TimeUnit.SECONDS))
                .reduce((Order t1, Order t2) -> {
                    Order result = new Order();
                    result.setFutureVariety(t1.getFutureVariety());
                    result.setTimestampSecond(t1.getTimestampSecond());
                    result.setOrderPay(t1.getOrderPay() + t2.getOrderPay());
                    result.setNumber(t1.getNumber() + t2.getNumber());
                    return result;
                })
                .map(order -> {
                    FuturePrice p = new FuturePrice();
                    p.setFutureVariety(order.getFutureVariety());
                    p.setTimestampSecond(order.getTimestampSecond());
                    p.setPrice(order.getOrderPay()/ order.getNumber());
                    return p;
                });

        DataStream<String> futurePriceOutput = futurePriceStream.map(rec -> JSONObject.toJSONString(rec));
        futurePriceOutput.print();

        StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path("D:/kafkastreamfile"),
                        new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();
        futurePriceOutput.addSink(sink);

        env.execute();






//        // set up the streaming execution environment
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        //默认情况下，检查点被禁用。要启用检查点，请在StreamExecutionEnvironment上调用enableCheckpointing(n)方法，
//        // 其中n是以毫秒为单位的检查点间隔。每隔5000 ms进行启动一个检查点,则下一个检查点将在上一个检查点完成后5秒钟内启动
//        env.enableCheckpointing(5000);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "13.76.185.14:9092");//kafka的节点的IP或者hostName，多个使用逗号分隔
//        properties.setProperty("zookeeper.connect", "13.76.185.14:2181");//zookeeper的节点的IP或者hostName，多个使用逗号进行分隔
//        properties.setProperty("group.id", "test-consumer-group-new");//flink consumer flink的消费者的group.id
//        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>("first_topic", new SimpleStringSchema(),
//                properties);//test0是kafka中开启的topic
////        myConsumer.setStartFromEarliest();
////        myConsumer.assignTimestampsAndWatermarks(new CustomWatermarkEmitter());
//        DataStream<String> text = env.addSource(myConsumer);//将kafka生产者发来的数据进行处理，本例子我进任何处理
//        text.print();//直接将从生产者接收到的数据在控制台上进行打印
////        text.writeAsCsv("/Users/wenlong/WorkSpaces/DayDayUP/FlinkKafkaLab/received.txt", FileSystem.WriteMode.OVERWRITE);
//
//        // 反序列化
//        DataStream<Order> orderStream = text.map(rec -> (Order)JSON.parse(rec))
//                .map(rec -> {
//                    rec.setTimestampSecond(rec.getTimestamp()/1000);
//                    rec.setOrderPay(rec.getPrice() * rec.getNumber());
//                    return rec;
//                });
//
//        DataStream<String> output = orderStream.map(rec -> rec.toString());
////        output.print();
//////         StreamingFileSink<String> sink = StreamingFileSink
//////                .forRowFormat(new Path("/Users/wenlong/WorkSpaces/DayDayUP/FlinkKafkaLab/result.txt"),
//////                        new SimpleStringEncoder<String>("UTF-8"))
//////                .withRollingPolicy(
//////                        DefaultRollingPolicy.builder()
//////                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
//////                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
//////                                .withMaxPartSize(1024 * 1024 * 1024)
//////                                .build())
//////                .build();
////        orderStream.print();
////
////        DataStream<FuturePrice> futurePrice = orderStream.keyBy(
////
////                new KeySelector<Order, Tuple2<String, Long>>() {
////
////                    @Override
////                    public Tuple2<String, Long> getKey(Order value) throws Exception {
////                        return Tuple2.of(value.getFutureVariety(), value.getTimestampSecond());
////                    }
////                }).reduce((Order t1, Order t2) -> {
////                      Order result = new Order();
////                      result.setFutureVariety(t1.getFutureVariety());
////                      result.setTimestampSecond(t1.getTimestampSecond());
////                      result.setOrderPay(t1.getOrderPay() + t2.getOrderPay());
////                      result.setNumber(t1.getNumber() + t2.getNumber());
////                      return result;
////                }).map(order -> {
////            FuturePrice p = new FuturePrice();
////            p.setFutureVariety(order.getFutureVariety());
////            p.setTimestampSecond(order.getTimestampSecond());
////            p.setPrice(order.getOrderPay()/ order.getNumber());
////            return p;
////        });
////
////
////        futurePrice.print();//直接将从生产者接收到的数据在控制台上进行打印
//
//        // execute program
//        env.execute("Flink Streaming Java API Skeleton");

    }


}
