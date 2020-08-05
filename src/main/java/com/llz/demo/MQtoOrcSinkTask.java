package com.llz.demo;

import orc.OrcTaskParam;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.realb.flink.orc.OrcWriters;
import uk.co.realb.flink.orc.encoder.EncoderOrcWriterFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class MQtoOrcSinkTask {

    protected static final Logger log = LoggerFactory.getLogger(MQtoOrcSinkTask.class);

    public static final String LOG_PRE = "MQtoOrcSinkTask_";

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        String paramJson = params.get("json");

        if (StringUtils.isBlank(paramJson)) {
            log.error(LOG_PRE + "参数异常，任务停止");
            return;
        }
        log.info(LOG_PRE + "param:" + paramJson);
        OrcTaskParam param = GsonUtil.fromJson(paramJson, OrcTaskParam.class);
        //下面54行有bug目前必须要有dt
        String[] names = param.getFiledNames();
        List<String> fs = new ArrayList(Arrays.asList(names));
        if (!fs.contains("dt")) {
            fs.add("dt");
            names = (String[]) fs.toArray();
        }
        final String[] fields = names;
        System.setProperty("HADOOP_USER_NAME", param.getUserName());

        //String basePath = "hdfs://127.0.0.1:9000/user/pyx/table2";
        //String basePath = "hdfs://172.25.223.53:8020/user/dwetl/adanosdata/stra_dtpipeline_result/20200306";
        //String basePath = "hdfs://172.25.66.63:9000/user/dwetl/stra_dtpipeline_result";
        //String[] fields = {"pin", "result_id", "result_value", "result_time", "ext_json", "dt"};

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(9999, Time.of(10, TimeUnit.SECONDS)));
        env.getConfig().setGlobalJobParameters(params);
        //enable checkpoint to guarantee exactly once
        env.enableCheckpointing(2 * 60 * 1000L);
        //env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", param.getServerUrl());
        properties.setProperty("group.id", param.getGroup());
        properties.setProperty("client.id", param.getGroup());
        FlinkKafkaConsumer011<String> kafkaConsumer011 = new FlinkKafkaConsumer011<>(param.getTopic(), new SimpleStringSchema(), properties);
        kafkaConsumer011.setStartFromLatest();
        SingleOutputStreamOperator<Map<String, String>> stream = env.addSource(kafkaConsumer011)
                .name("source")
                .flatMap(new FlatMapFunction<String, Map<String, String>>() {
                    @Override
                    public void flatMap(String value, Collector<Map<String, String>> out) throws Exception {
                        // 对json有要求，必须是平级，可以有json串实现第二级
                        try {
                            //log.info(value);
                            Map<String, String> map = GsonUtil.fromJson2StrMap(value);
                            map.put("dt", DateUtil.format(new Date(), DateUtil.PARSE_PATTERNS_YYYYMMDD));
                            out.collect(map);
                        } catch (Exception e) {
                            log.error(LOG_PRE + "MAP error", e);
                        }
                    }
                })
                .name("flatMap");

        // 暂时全是 String 类型
        TypeInformation[] typeInformations = new TypeInformation[fields.length];
        Arrays.fill(typeInformations, Types.STRING);
        OrcSchema orcSchema = new OrcSchema(fields, typeInformations);

        OrcRecordEncoder encoder = new OrcRecordEncoder();

        Properties conf = new Properties();
        conf.setProperty("orc.compress", "SNAPPY");
        //conf.setProperty("orc.bloom.filter.columns", "x");

        EncoderOrcWriterFactory<OrcRecord> writer = OrcWriters.withCustomEncoder(
                encoder, TypeDescription.fromString(orcSchema.toDescString()), conf);

        StreamingFileSink<OrcRecord> sink = StreamingFileSink
                .forBulkFormat(new Path(param.getBasePath()), writer)
                .withBucketAssigner(new OrcRecordBucketAssigner())
                .build();

        stream.map((MapFunction<Map<String, String>, OrcRecord>) value -> {
            Object[] row = new Object[fields.length];
            for (int i = 0; i < row.length; i++) {
                String v = value.get(fields[i]);
                row[i] = v;
            }
            return OrcRecord.builder().orcSchema(orcSchema).row(Row.of(row)).build();
        })
                .addSink(sink)
                .name("sinkOrc");
        // execute program
        env.execute(LOG_PRE + param.getType() + "_" + param.getTopic());
    }
}
