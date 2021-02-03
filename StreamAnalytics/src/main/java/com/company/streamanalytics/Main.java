package com.company.streamanalytics;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.json.JSONObject;

import java.util.Iterator;

public class Main {

    public static void main(String[] args) {

        final String processedImageDir = "/home/harsh/analysed-data/";

        try {
            SparkSession session = SparkSession
                    .builder()
                    .appName("KafkaDemo")
                    .master("local[*]")
                    .getOrCreate();

            session.sparkContext().setLogLevel("ERROR");

            Dataset<Row> df = session
                    .readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("auto.offset.reset", "latest")
                    .option("max.partition.fetch.bytes", 2097152)
                    .option("subscribe", "distributed-video1")
                    .load();


            Dataset<String> words = df.selectExpr("CAST(value AS STRING)").as(Encoders.STRING());

            KeyValueGroupedDataset<String, String> kv = words.groupByKey(new MapFunction<String, String>() {
                @Override
                public String call(String s) throws Exception {
                    JSONObject object = new JSONObject(s);
                    return object.getString("cameraId");
                }
            }, Encoders.STRING());

            Dataset<String> processed = kv.mapGroupsWithState(new MapGroupsWithStateFunction<String, String, String, String>() {
                @Override
                public String call(String s, Iterator<String> iterator, GroupState<String> groupState) throws Exception {

                    String existing = null;
                    //check previous state
                    if (groupState.exists()) {
                        existing = groupState.get();
                    }
                    //detect motion
                    String processed = MotionDetector.detectMotion(s,iterator,processedImageDir,existing);

                    //update last processed
                    if(processed != null){
                        groupState.update(processed);
                    }
                    return processed;
                }
            }, Encoders.STRING(), Encoders.STRING());

            StreamingQuery query = processed.writeStream()
                    .outputMode("update")
                    .format("console")
                    .start();

            query.awaitTermination();
        }
        catch(Exception e) {
            System.out.println("ERROR: " + e);
        }

    }
}
