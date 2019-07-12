package Project2.metrics;

import Project2.deserializer.Tuple2Serializer;
import Project2.entity.RedisBean;
import Project2.flink.FlinkConfig;
import Project2.flink.RedisConfig;
import com.codahale.metrics.SlidingWindowReservoir;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.*;

public class Query3Metrics {



    private static double wa = 0.3, wb = 0.7, approveIncr = 1.1;

    public static void doQuery(DataStream<Tuple15<Long, String, Long, Long, String, Long, Long, Boolean, Long, String, Long, String, String, Long, String>> dataStream, Properties configProperties) {

        // ---- for metric computation only ----
        DataStream<Tuple16<Long, String, Long, Long, String, Long, Long, Boolean, Long, String, Long, String, String, Long, String, Long>> metricSterams = dataStream.process(new ProcessFunction<Tuple15<Long, String, Long, Long, String, Long, Long, Boolean, Long, String, Long, String, String, Long, String>, Tuple16<Long, String, Long, Long, String, Long, Long, Boolean, Long, String, Long, String, String, Long, String, Long>>() {

            private transient Meter meter;

            @Override
            public void open(Configuration parameters) throws Exception {
                com.codahale.metrics.Meter dropwizard = new com.codahale.metrics.Meter();
                this.meter = getRuntimeContext().getMetricGroup().addGroup("Query3").meter("throughput_in", new DropwizardMeterWrapper(dropwizard));
            }

            @Override
            public void processElement(Tuple15<Long, String, Long, Long, String, Long, Long, Boolean, Long, String, Long, String, String, Long, String> t, Context context, Collector<Tuple16<Long, String, Long, Long, String, Long, Long, Boolean, Long, String, Long, String, String, Long, String, Long>> collector) throws Exception {
                this.meter.markEvent();
                collector.collect(new Tuple16<>(t.f0, t.f1, t.f2, t.f3, t.f4, t.f5, t.f6, t.f7, t.f8, t.f9, t.f10, t.f11, t.f12, t.f13, t.f14, System.nanoTime()));

            }
        });


        //Direct Stream: commentid, commentType, editorSelection, recommendations, userId, depth, inReplyTo
        DataStream<Tuple8<Long, String, Boolean, Long, Long, Long, Long, Long>> directCommentsTs = metricSterams.project(3, 4, 7, 10, 13, 6, 8, 15);

        DataStream<Tuple7<Long, String, Boolean, Long, Long, Long, Long>> directComments = directCommentsTs.process(new ProcessFunction<Tuple8<Long,String,Boolean,Long,Long,Long,Long,Long>, Tuple7<Long,String,Boolean,Long,Long,Long,Long>>() {
            private transient Meter meter;
            private transient DropwizardHistogramWrapper histogram;

            @Override
            public void open(Configuration parameters) throws Exception {
                com.codahale.metrics.Meter dropwizard = new com.codahale.metrics.Meter();
                this.meter = getRuntimeContext().getMetricGroup().addGroup("Query3").meter("thoughput_pre_window", new DropwizardMeterWrapper(dropwizard));
                com.codahale.metrics.Histogram dropwizardHistogram =
                        new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500));

                this.histogram = getRuntimeContext()
                        .getMetricGroup()
                        .addGroup("Query3")
                        .histogram("histogram", new DropwizardHistogramWrapper(dropwizardHistogram));

            }

            @Override
            public void processElement(Tuple8<Long, String, Boolean, Long, Long, Long, Long, Long> t, Context context, Collector<Tuple7<Long, String, Boolean, Long, Long, Long, Long>> collector) throws Exception {
                this.meter.markEvent();
                Long processingTime = System.nanoTime() - t.f7;
                this.histogram.update(processingTime);
                collector.collect(new Tuple7<>(t.f0, t.f1, t.f2, t.f3, t.f4, t.f5, t.f6));
            }
        });

        //commentId, recommendation, userId
        DataStream<Tuple3<Long, Double, Long>> comments_1 = directComments
                .filter(t -> t.f5.equals(1L))
                .map(new MapFunction<Tuple7<Long, String, Boolean, Long, Long, Long, Long>, Tuple3<Long, Double, Long>>() {
                    @Override
                    public Tuple3<Long, Double, Long> map(Tuple7<Long, String, Boolean, Long, Long, Long, Long> tuple) throws Exception {
                        if (tuple.f2)
                            return new Tuple3<>(tuple.f0, tuple.f3 * approveIncr, tuple.f4);
                        else
                            return new Tuple3<>(tuple.f0, tuple.f3.doubleValue(), tuple.f4);
                    }
                })
                .keyBy(2)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .apply(new WindowFunction<Tuple3<Long, Double, Long>, Tuple3<Long, Double, Long>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple t, TimeWindow
                            timeWindow, Iterable<Tuple3<Long, Double, Long>> iterable, Collector<Tuple3<Long, Double, Long>> collector) throws
                            Exception {
                        Jedis jedis = new Jedis(RedisConfig.REDIS_ADDR, Integer.parseInt(RedisConfig.REDIS_PORT));
                        ObjectMapper mapper = new ObjectMapper();
                        iterable.forEach(tuple -> {
                            try {
                                jedis.set(tuple.f0.toString(), mapper.writeValueAsString(new RedisBean(tuple.f2, tuple.f1, 0L)));
                                jedis.expire(tuple.f0.toString(), RedisConfig.expirationTime);
                            } catch (JsonProcessingException e) {
                                e.printStackTrace();
                            }
                            collector.collect(tuple);
                        });
                        jedis.close();
                    }
                });

        //OUTPUT: Tuple3 commentId #likes userId


        DataStream<Tuple3<Long, Long, Long>> comments_2 = directComments
                .filter(t -> t.f5.equals(2L))
                //commentID, 0.0, userId, inReplyTo
                .map(new MapFunction<Tuple7<Long, String, Boolean, Long, Long, Long, Long>, Tuple4<Long, Double, Long, Long>>() {
                    @Override
                    public Tuple4<Long, Double, Long, Long> map(Tuple7<Long, String, Boolean, Long, Long, Long, Long> tuple) throws Exception {
                        return new Tuple4<>(tuple.f0, 0.0, tuple.f4, tuple.f6);
                    }
                })
                .keyBy(2)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .apply(new WindowFunction<Tuple4<Long, Double, Long, Long>, Tuple3<Long, Long, Long>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple t, TimeWindow
                            timeWindow, Iterable<Tuple4<Long, Double, Long, Long>> iterable, Collector<Tuple3<Long, Long, Long>> collector) throws
                            Exception {
                        Jedis jedis = new Jedis(RedisConfig.REDIS_ADDR, Integer.parseInt(RedisConfig.REDIS_PORT));
                        ObjectMapper mapper = new ObjectMapper();
                        iterable.forEach(tuple -> {
                            try {
                                jedis.set(tuple.f0.toString(), mapper.writeValueAsString(new RedisBean(tuple.f2, tuple.f1, tuple.f3)));
                                jedis.expire(tuple.f0.toString(), RedisConfig.expirationTime);
                            } catch (JsonProcessingException e) {
                                e.printStackTrace();
                            }
                            collector.collect(new Tuple3<>(tuple.f0, tuple.f2, tuple.f3));
                        });
                        jedis.close();
                    }
                });

        //OUTPUT comment2: Tuple3 commentId userId inReplyTo


        //Indirect stream: InreplyTo commentType
        DataStream<Tuple2<Long, String>> indirectComments = dataStream.project(8, 4);

        indirectComments = indirectComments.filter(t -> t.f1.equals("userReply"));

        DataStream<Tuple2<Long, Long>> indirectSum = indirectComments
                .map(new MapFunction<Tuple2<Long, String>, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> map(Tuple2<Long, String> tuple2) throws Exception {
                        return new Tuple2<>(tuple2.f0, 1L);
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .sum(1);

        //OUTPUT: Tuple2: inReplyTo(commentId) #indirectComments

        DataStream<Tuple5<Long, Double, Double, Long, Long>> comments_2_indirect = indirectSum
                .connect(comments_2)
                .map(new CoMapFunction<Tuple2<Long, Long>, Tuple3<Long, Long, Long>, Tuple3<Long, Long, Long>>() {

                    //inReplyTo #indirectComments
                    //third level comments after key by
                    @Override
                    public Tuple3<Long, Long, Long> map1(Tuple2<Long, Long> tuple) throws Exception {
                        return new Tuple3<>(tuple.f0, tuple.f1, 0L);
                    }

                    //commentId userId inReplyTo
                    @Override
                    public Tuple3<Long, Long, Long> map2(Tuple3<Long, Long, Long> tuple) throws Exception {
                        return tuple;
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new AggregateFunction<Tuple3<Long, Long, Long>, Tuple5<Long, Double, Double, Long, Long>, Tuple5<Long, Double, Double, Long, Long>>() {
                    @Override
                    public Tuple5<Long, Double, Double, Long, Long> createAccumulator() {
                        //commentid indirectCount #likes userid inReplyTo
                        return new Tuple5<>(0L, 0.0, 0.0, 0L, 0L);
                    }

                    @Override
                    public Tuple5<Long, Double, Double, Long, Long> add(Tuple3<Long, Long, Long> tuple, Tuple5<Long, Double, Double, Long, Long> acc) {
                        acc.f0 = tuple.f0;
                        if (tuple.f2 == 0L) {
                            acc.f1 = tuple.f1.doubleValue();
                        } else {
                            acc.f3 = tuple.f1;
                            acc.f4 = tuple.f2;
                        }
                        return acc;
                    }

                    @Override
                    public Tuple5<Long, Double, Double, Long, Long> getResult(Tuple5<Long, Double, Double, Long, Long> res) {
                        //commentid indirectCount #likes userid inReplyTo
                        if (res.f3.equals(0L)) {
                            ObjectMapper mapper = new ObjectMapper();
                            Jedis jedis = new Jedis(RedisConfig.REDIS_ADDR, Integer.parseInt(RedisConfig.REDIS_PORT));
                            String response = jedis.get(String.valueOf(res.f0));
                            jedis.close();
                            if (response != null) {
                                RedisBean redisBean = null;
                                try {
                                    redisBean = mapper.readValue(response, RedisBean.class);
                                    if (redisBean.getInReplyto() == 0L)
                                        return null;
                                    res.f3 = redisBean.getUserId();
                                    res.f4 = redisBean.getInReplyto();
                                    //System.err.println(redisBean);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            } else {
                                return null;
                            }
                        }
                        return res;
                    }

                    @Override
                    public Tuple5<Long, Double, Double, Long, Long> merge(Tuple5<Long, Double, Double, Long, Long> acc1, Tuple5<Long, Double, Double, Long, Long> acc2) {
                        return new Tuple5<>(acc1.f0,
                                acc1.f1 == 0 ? acc2.f1 : acc1.f1,
                                acc1.f2 == 0.0 ? acc2.f2 : acc1.f2,
                                acc1.f3 == 0 ? acc2.f3 : acc1.f3,
                                acc1.f4 == 0 ? acc2.f4 : acc1.f4);
                    }
                });

        DataStream<Tuple4<Long, Double, Double, Long>> comments_1_indirect = indirectSum
                .connect(comments_1)
                .map(new CoMapFunction<Tuple2<Long, Long>, Tuple3<Long, Double, Long>, Tuple4<Long, Double, Long, Long>>() {
                    //inReplyTo #indirectComments
                    @Override
                    public Tuple4<Long, Double, Long, Long> map1(Tuple2<Long, Long> tuple2) throws Exception {
                        return new Tuple4<>(tuple2.f0, 0.0, tuple2.f1, 0L);
                    }

                    //commentId #likes userId
                    @Override
                    public Tuple4<Long, Double, Long, Long> map2(Tuple3<Long, Double, Long> tuple3) throws Exception {
                        return new Tuple4<>(tuple3.f0, tuple3.f1, 0L, tuple3.f2);
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new AggregateFunction<Tuple4<Long, Double, Long, Long>, Tuple4<Long, Double, Double, Long>, Tuple4<Long, Double, Double, Long>>() {
                    @Override
                    public Tuple4<Long, Double, Double, Long> createAccumulator() {
                        return new Tuple4<>(0L, 0.0, 0.0, 0L);
                    }

                    @Override
                    public Tuple4<Long, Double, Double, Long> add(Tuple4<Long, Double, Long, Long> tuple, Tuple4<Long, Double, Double, Long> acc) {
                        acc.f0 = tuple.f0;
                        if (tuple.f3 == 0L) {
                            acc.f2 = tuple.f2.doubleValue();
                        } else {
                            acc.f1 = tuple.f1;
                            acc.f3 = tuple.f3;
                        }
                        return acc;
                    }

                    @Override
                    public Tuple4<Long, Double, Double, Long> getResult(Tuple4<Long, Double, Double, Long> res) {
                        if (res.f3 == 0) {
                            ObjectMapper mapper = new ObjectMapper();
                            Jedis jedis = new Jedis(RedisConfig.REDIS_ADDR, Integer.parseInt(RedisConfig.REDIS_PORT));
                            String response = jedis.get(String.valueOf(res.f0));
                            jedis.close();
                            if (response != null) {
                                RedisBean redisBean = null;
                                try {
                                    redisBean = mapper.readValue(response, RedisBean.class);
                                    if (redisBean.getInReplyto() != 0L)
                                        return null;
                                    res.f3 = redisBean.getUserId();
                                    //res.f1 = redisBean.getRecommendations();
                                    res.f1 = 0.0;
                                    //System.err.println(redisBean);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            } else {
                                return null;
                            }
                        }

                        return res;
                    }

                    @Override
                    public Tuple4<Long, Double, Double, Long> merge(Tuple4<Long, Double, Double, Long> acc1, Tuple4<Long, Double, Double, Long> acc2) {
                        return new Tuple4<>(acc1.f0,
                                acc1.f1 == 0 ? acc2.f1 : acc1.f1,
                                acc1.f2 == 0.0 ? acc2.f2 : acc1.f2,
                                acc1.f3 == 0 ? acc2.f3 : acc1.f3);
                    }
                });


        DataStream<Tuple4<Long, Double, Double, Long>> comment_1_final = comments_1_indirect
                .connect(comments_2_indirect)
                .map(new CoMapFunction<Tuple4<Long, Double, Double, Long>, Tuple5<Long, Double, Double, Long, Long>, Tuple4<Long, Double, Double, Long>>() {
                    //commentId #likes IndirectCounts userId
                    @Override
                    public Tuple4<Long, Double, Double, Long> map1(Tuple4<Long, Double, Double, Long> tuple) throws Exception {
                        return tuple;
                    }

                    //commentid indirectCount #likes userid inReplyTo
                    @Override
                    public Tuple4<Long, Double, Double, Long> map2(Tuple5<Long, Double, Double, Long, Long> tuple) throws Exception {
                        return new Tuple4<>(tuple.f4, -1.0, tuple.f1, tuple.f3);
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new AggregateFunction<Tuple4<Long, Double, Double, Long>, Tuple4<Long, Double, Double, Long>, Tuple4<Long, Double, Double, Long>>() {
                    @Override
                    public Tuple4<Long, Double, Double, Long> createAccumulator() {
                        //commentid #likes indirectCounts userId
                        return new Tuple4<>(0L, 0.0, 0.0, 0L);
                    }

                    @Override
                    public Tuple4<Long, Double, Double, Long> add(Tuple4<Long, Double, Double, Long> tuple, Tuple4<Long, Double, Double, Long> acc) {
                        acc.f0 = tuple.f0;
                        if (tuple.f1 != -1.0) {
                            acc.f1 = tuple.f1;
                        }
                        acc.f3 = tuple.f3;
                        acc.f2 += tuple.f2;
                        return acc;
                    }

                    @Override
                    public Tuple4<Long, Double, Double, Long> getResult(Tuple4<Long, Double, Double, Long> res) {
                        return res;
                    }

                    @Override
                    public Tuple4<Long, Double, Double, Long> merge(Tuple4<Long, Double, Double, Long> acc1, Tuple4<Long, Double, Double, Long> acc2) {
                        return new Tuple4<>(acc1.f0,
                                acc1.f1 == 0 ? acc2.f1 : acc1.f1,
                                acc1.f2 == 0.0 ? acc2.f2 : acc1.f2,
                                acc1.f3 == 0 ? acc2.f3 : acc1.f3);
                    }
                });

        DataStream<Tuple3<Long, Long, Double>> aggregateStreams = comment_1_final
                .connect(comments_2_indirect)
                .map(new CoMapFunction<Tuple4<Long, Double, Double, Long>, Tuple5<Long, Double, Double, Long, Long>, Tuple3<Long, Double, Double>>() {

                    //commentId #likes IndirectCounts userId
                    @Override
                    public Tuple3<Long, Double, Double> map1(Tuple4<Long, Double, Double, Long> tuple) throws Exception {
                        return new Tuple3<>(tuple.f3, tuple.f1, tuple.f2);
                    }

                    //commentid indirectCount #likes userid inReplyTo
                    @Override
                    public Tuple3<Long, Double, Double> map2(Tuple5<Long, Double, Double, Long, Long> tuple) throws Exception {
                        return new Tuple3<>(tuple.f3, tuple.f2, tuple.f1);
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new AggregateFunction<Tuple3<Long, Double, Double>, Tuple3<Long, Double, Double>, Tuple3<Long, Long, Double>>() {
                    private Random random = new Random(FlinkConfig.SEED);

                    @Override
                    public Tuple3<Long, Double, Double> createAccumulator() {
                        //userid #like indirectCounts
                        return new Tuple3<>(0L, 0.0, 0.0);
                    }

                    @Override
                    public Tuple3<Long, Double, Double> add(Tuple3<Long, Double, Double> tuple, Tuple3<Long, Double, Double> acc) {
                        acc.f0 = tuple.f0;
                        acc.f1 += tuple.f1;
                        acc.f2 += tuple.f2;
                        return acc;
                    }

                    @Override
                    public Tuple3<Long, Long, Double> getResult(Tuple3<Long, Double, Double> tuple) {

                        return new Tuple3<>((long) random.nextInt(FlinkConfig.PARALLELISM), tuple.f0, tuple.f1 * wa + tuple.f2 * wb);
                    }

                    @Override
                    public Tuple3<Long, Double, Double> merge(Tuple3<Long, Double, Double> acc1, Tuple3<Long, Double, Double> acc2) {
                        acc1.f0 = acc2.f0;
                        acc1.f1 += acc2.f1;
                        acc1.f2 += acc2.f2;
                        return acc1;
                    }
                });

        aggregateStreams
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new AggregateFunction<Tuple3<Long, Long, Double>, List<Tuple2<Long, Double>>, List<Tuple2<Long, Double>>>() {
                    @Override
                    public List<Tuple2<Long, Double>> createAccumulator() {
                        return new ArrayList<>();
                    }

                    @Override
                    public List<Tuple2<Long, Double>> add(Tuple3<Long, Long, Double> tuple3, List<Tuple2<Long, Double>> list) {
                        list.add(new Tuple2<>(tuple3.f1, tuple3.f2));
                        return list;
                    }

                    @Override
                    public List<Tuple2<Long, Double>> getResult(List<Tuple2<Long, Double>> list) {
                        list.sort(Comparator.comparing(o -> -o.f1));
                        return new ArrayList<>(list.subList(0, Math.min(10, list.size())));
                    }

                    @Override
                    public List<Tuple2<Long, Double>> merge(List<Tuple2<Long, Double>> list1, List<Tuple2<Long, Double>> list2) {
                        list1.forEach(t -> list2.add(t));
                        return list2;
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
                .process(new ProcessAllWindowFunction<List<Tuple2<Long, Double>>, Tuple2<Long, List<Tuple2<Long, Double>>>, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<List<Tuple2<Long, Double>>> iterable, Collector<Tuple2<Long, List<Tuple2<Long, Double>>>> collector) throws Exception {
                        ArrayList<Tuple2<Long, Double>> res = new ArrayList<>();
                        iterable.forEach(t -> t.forEach(tu -> res.add(new Tuple2<>(tu.f0, tu.f1))));
                        res.sort(Comparator.comparing(o -> -o.f1));
                        collector.collect(new Tuple2<>(context.window().getStart(), new ArrayList<>(res.subList(0, Math.min(10, res.size())))));
                    }
                })
                .addSink(new FlinkKafkaProducer011<Tuple2<Long, List<Tuple2<Long, Double>>>>("query3_24_hours1", new Tuple2Serializer<Long, Double>(), configProperties));


    }

}
