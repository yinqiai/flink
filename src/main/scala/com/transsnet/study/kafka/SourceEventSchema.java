package com.transsnet.study.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class SourceEventSchema implements DeserializationSchema<SourceEvent> {

    @Override
    public SourceEvent deserialize(byte[] bytes) throws IOException {
        return SourceEvent.fromString(new String(bytes));
    }

    @Override
    public boolean isEndOfStream(SourceEvent sourceEvent) {
        return false;
    }

    @Override
    public TypeInformation<SourceEvent> getProducedType() {
        return TypeInformation.of(SourceEvent.class);
    }
}
