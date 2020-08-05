package orc;

import com.jdd.flink.util.GsonUtil;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
public class OrcTaskParam {

    // 消费类型，kafka或jmq4
    private String type;
    // kafka或jmq4地址
    private String serverUrl;
    // 消息组
    private String group;
    // 消息主题
    private String topic;
    // 字段名
    private String[] filedNames;
    // 字段类型
    private String[] filedTypes;
    // hadoop用户名
    private String userName;
    // orc路径
    private String basePath;

    // 扩展字段
    private Integer parallelism;
    private String mainClassName;
    private String savepointPath;

    public static void main(String[] args) {
        OrcTaskParam build = OrcTaskParam.builder()
                .type("kafka")
                .serverUrl("172.25.66.89:9092,172.25.66.93:9092")
                .group("jrflink.flink2es")
                .topic("stra_dtpipeline_result")
                .filedNames(new String[]{"pin", "result_id", "result_value", "result_time", "ext_json", "dt"})
                .filedTypes(new String[]{"string", "string", "string", "string", "string", "string"})
                .userName("pyx")
                .basePath("hdfs://127.0.0.1:9000/user/pyx/table4")
                .build();
        System.out.println(GsonUtil.toJson(build));

        OrcTaskParam build1 = OrcTaskParam.builder()
                .type("kafka")
                .serverUrl("nameserver.jmq.jd.local:80")
                .group("jrflink.flink2es")
                .topic("stra_dtpipeline_result")
                .filedNames(new String[]{"pin", "result_id", "result_value", "result_time", "ext_json", "dt"})
                .filedTypes(new String[]{"string", "string", "string", "string", "string", "string"})
                .userName("dwetl")
                .basePath("hdfs://172.25.223.53:8020/user/dwetl/extdb/stra_dtpipeline_result")
                .build();
        System.out.println(GsonUtil.toJson(build1));
    }
}
