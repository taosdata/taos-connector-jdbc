package com.taosdata.jdbc.utils;

import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.ws.Transport;
import com.taosdata.jdbc.ws.entity.Action;
import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.entity.VersionReq;
import com.taosdata.jdbc.ws.entity.VersionResp;

import java.sql.SQLException;

import static com.taosdata.jdbc.TSDBConstants.MIN_SUPPORT_VERSION;

public class VersionUtil {
    private VersionUtil(){}
    public static void checkVersion(String version, Transport transport) throws SQLException {
        if (version != null) {
            if (Utils.compareVersions(version, MIN_SUPPORT_VERSION) < 0){
                transport.close();
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_VERSION_INCOPMATIABLE, "minimal supported version is " + MIN_SUPPORT_VERSION + ", but got " + version);
            }
        } else {
            VersionReq versionReq = new VersionReq();
            versionReq.setReqId(ReqId.getReqID());
            VersionResp versionResp = (VersionResp) transport.send(new Request(Action.VERSION.getAction(), versionReq));
            if (Utils.compareVersions(versionResp.getVersion(), MIN_SUPPORT_VERSION) < 0){
                transport.close();
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_VERSION_INCOPMATIABLE, "minimal supported version is " + MIN_SUPPORT_VERSION + ", but got " + versionResp.getVersion());
            }
        }
    }
}
