package com.adv.flinkjobs;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WordCountsSocket {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //source
//        DataStreamSource<String> dataStreamSource = env.readTextFile("/home/lz/workspace/flink-jobs/wordCount/src/main/resources/wordCount.txt");
        DataStream<String> dataStreamSource = env.socketTextStream("127.0.0.1", 9000, "\n");


        //transform
        DataStream<WordWithCount> windowCounts = dataStreamSource.flatMap(
                        new FlatMapFunction<String, WordWithCount>() {
                            public void flatMap(String s, Collector<WordWithCount> out) {
                                if ("".equals(s)) {
                                    return;
                                }
                                for (String word : s.split("\\s")) {
                                    if (!"".equals(word)) {
                                        out.collect(new WordWithCount(word, 1));
                                    }
                                }
                            }
                        })
                .keyBy("word")
//                .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1)))
                .reduce(
                        new ReduceFunction<WordWithCount>() {
                            public WordWithCount reduce(WordWithCount a, WordWithCount b) {
                                return new WordWithCount(a.word, a.count + b.count);
                            }
                        }
                );

        //sink
        windowCounts.print();

        env.execute();
    }

    // Data type for words with count
    public static class WordWithCount {

        public String word;
        public long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}