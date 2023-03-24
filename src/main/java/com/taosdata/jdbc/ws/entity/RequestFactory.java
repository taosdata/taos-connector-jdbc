package com.taosdata.jdbc.ws.entity;

/**
 * generate id for request
 */
public class RequestFactory {

    private RequestFactory() {
    }

    public static Request generateQuery(String sql, long reqId) {
        QueryReq queryReq = new QueryReq();
        queryReq.setReqId(reqId);
        queryReq.setSql(sql);
        return new Request(Action.QUERY.getAction(), queryReq);
    }

    public static Request generateFetch(long id, long reqId) {
        FetchReq fetchReq = new FetchReq();
        fetchReq.setReqId(reqId);
        fetchReq.setId(id);
        return new Request(Action.FETCH.getAction(), fetchReq);
    }

    public static Request generateFetchBlock(long id) {
        FetchReq fetchReq = new FetchReq();
        fetchReq.setReqId(id);
        fetchReq.setId(id);
        return new Request(Action.FETCH_BLOCK.getAction(), fetchReq);
    }
}
