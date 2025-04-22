package com.taosdata.jdbc.ws.entity;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class BinQueryNewResp extends Response {

    public BinQueryNewResp(QueryResp queryResp, FetchBlockNewResp fetchBlockNewResp) {
        this.queryResp = queryResp;
        this.fetchBlockNewResp = fetchBlockNewResp;
    }

    QueryResp queryResp;
    FetchBlockNewResp fetchBlockNewResp;



    public QueryResp getQueryResp() {
        return queryResp;
    }

    public void setQueryResp(QueryResp queryResp) {
        this.queryResp = queryResp;
    }

    public FetchBlockNewResp getFetchBlockNewResp() {
        return fetchBlockNewResp;
    }

    public void setFetchBlockNewResp(FetchBlockNewResp fetchBlockNewResp) {
        this.fetchBlockNewResp = fetchBlockNewResp;
    }
}
