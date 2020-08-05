package orc;

import org.apache.flink.types.Row;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import uk.co.realb.flink.orc.encoder.OrcRowEncoder;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Timestamp;

public class OrcRecordEncoder extends OrcRowEncoder<OrcRecord> implements Serializable {
    @Override
    public void encodeAndAdd(OrcRecord datum, VectorizedRowBatch batch) {
        int index = nextIndex(batch);

        OrcSchema orcSchema = datum.getOrcSchema();
        Row row = datum.getRow();

        //fill up the orc value
        for (int i = 0; i < orcSchema.getFieldCount(); i++) {

            ColumnVector cv = batch.cols[i];
            Object colVal = row.getField(i);

            if (colVal != null) {
                String type = orcSchema.getOrcFieldType(i);
                switch (type) {
                    case OrcSchema.OrcType.TYPE_BOOLEAN:
                    case OrcSchema.OrcType.TYPE_BIGINT:
                    case OrcSchema.OrcType.TYPE_INT:
                    case OrcSchema.OrcType.TYPE_TINYINT:
                        LongColumnVector longCv = (LongColumnVector) cv;
                        longCv.vector[index] = Long.parseLong(colVal.toString());
                        break;
                    case OrcSchema.OrcType.TYPE_DOUBLE:
                    case OrcSchema.OrcType.TYPE_FLOAT:
                        DoubleColumnVector doubleCv = (DoubleColumnVector) cv;
                        doubleCv.vector[index] = Double.parseDouble(colVal.toString());
                        break;
                    case OrcSchema.OrcType.TYPE_STRING:
                        BytesColumnVector bytesCv = (BytesColumnVector) cv;
                        bytesCv.setVal(index, colVal.toString().getBytes(Charset.forName("utf-8")));
                        break;
                    case OrcSchema.OrcType.TYPE_TIMESTAMP:
                        TimestampColumnVector timestampCv = (TimestampColumnVector) cv;
                        Timestamp timestamp = (Timestamp) colVal;
                        timestampCv.time[index] = timestamp.getTime();
                        timestampCv.nanos[index] = timestamp.getNanos();
                        break;
                    case OrcSchema.OrcType.TYPE_DECIMAL:
                        DecimalColumnVector decimalCv = (DecimalColumnVector) cv;
                        BigDecimal bigDecimal = (BigDecimal) colVal;
                        HiveDecimal hiveDecimal = HiveDecimal.create(bigDecimal);
                        HiveDecimalWritable hiveDecimalWritable = new HiveDecimalWritable(hiveDecimal);
                        decimalCv.precision = Short.valueOf(bigDecimal.precision() + "");
                        decimalCv.scale = Short.valueOf(bigDecimal.scale() + "");
                        decimalCv.vector[index] = hiveDecimalWritable;
                        break;
                    default:
                        throw new RuntimeException("the " + type + " type not support to orc now");
                }
            } else {
                cv.noNulls = false;
                cv.isNull[index] = true;
            }
        }

        incrementBatchSize(batch);
    }

}
