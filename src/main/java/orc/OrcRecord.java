package orc;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.flink.types.Row;

@Getter
@Setter
@Builder
public class OrcRecord {

    private OrcSchema orcSchema;

    private Row row;

}
