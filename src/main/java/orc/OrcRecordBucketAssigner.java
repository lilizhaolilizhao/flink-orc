package orc;

import com.jdd.flink.util.DateUtil;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

import java.util.Date;

public class OrcRecordBucketAssigner implements BucketAssigner<OrcRecord, String> {
    private static final long serialVersionUID = 987325769970523327L;

    @Override
    public String getBucketId(OrcRecord orcRecord, Context context) {
        return "dt=" + DateUtil.format(new Date(), DateUtil.PARSE_PATTERNS_YYYYMMDD);
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}
