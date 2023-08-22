package com.taosdata.jdbc.tmq;

public class OffsetAndMetadata {
    private final long offset;
    private final String metadata;


    public OffsetAndMetadata(long offset) {
        this(offset, "");
    }

    public OffsetAndMetadata(long offset, String metadata) {
        if (offset < 0)
            throw new IllegalArgumentException("Invalid negative offset");

        this.offset = offset;

        this.metadata = "";
    }

    public long offset() {
        return offset;
    }

    public String metadata() {
        return metadata;
    }
}
