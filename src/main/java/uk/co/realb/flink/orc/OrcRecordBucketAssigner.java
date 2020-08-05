package uk.co.realb.flink.orc;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.shaded.hadoop2.org.apache.commons.httpclient.util.DateUtil;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

import java.util.Date;

/**
 * 分区设置
 */
public class OrcRecordBucketAssigner implements BucketAssigner {
    @Override
    public Object getBucketId(Object o, Context context) {
        return "dt=" + DateUtil.formatDate(new Date(), "YYYYMMDD_HH");
    }

    @Override
    public SimpleVersionedSerializer getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}
