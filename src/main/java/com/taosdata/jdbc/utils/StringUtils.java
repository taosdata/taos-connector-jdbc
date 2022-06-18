package com.taosdata.jdbc.utils;

import com.taosdata.jdbc.TSDBDriver;

import java.util.Properties;
import java.util.StringTokenizer;

public class StringUtils {
    public static boolean isEmpty(final CharSequence cs) {
        return cs == null || cs.length() == 0;
    }

    public static Properties parseUrl(String url, Properties defaults) {
        Properties urlProps = (defaults != null) ? defaults : new Properties();
        if (StringUtils.isEmpty(url)) {
            return urlProps;
        }

        // parse properties in url
        int beginningOfSlashes = url.indexOf("//");
        int index = url.indexOf("?");
        if (index != -1) {
            String paramString = url.substring(index + 1);
            url = url.substring(0, index);
            StringTokenizer queryParams = new StringTokenizer(paramString, "&");
            while (queryParams.hasMoreElements()) {
                String parameterValuePair = queryParams.nextToken();
                int indexOfEqual = parameterValuePair.indexOf("=");
                String parameter = null;
                String value = null;
                if (indexOfEqual != -1) {
                    parameter = parameterValuePair.substring(0, indexOfEqual);
                    if (indexOfEqual + 1 < parameterValuePair.length()) {
                        value = parameterValuePair.substring(indexOfEqual + 1);
                    }
                }
                if (value != null && value.length() > 0 && parameter.length() > 0) {
                    urlProps.setProperty(parameter, value);
                }
            }
        }

        // parse Product Name
        String dbProductName = url.substring(0, beginningOfSlashes);
        dbProductName = dbProductName.substring(dbProductName.indexOf(":") + 1);
        dbProductName = dbProductName.substring(0, dbProductName.indexOf(":"));
        urlProps.setProperty(TSDBDriver.PROPERTY_KEY_PRODUCT_NAME, dbProductName);
        // parse dbname
        url = url.substring(beginningOfSlashes + 2);
        int indexOfSlash = url.indexOf("/");
        if (indexOfSlash != -1) {
            if (indexOfSlash + 1 < url.length()) {
                urlProps.setProperty(TSDBDriver.PROPERTY_KEY_DBNAME, url.substring(indexOfSlash + 1));
            }
            url = url.substring(0, indexOfSlash);
        }
        // parse port
        int indexOfColon = url.indexOf(":");
        if (indexOfColon != -1) {
            if (indexOfColon + 1 < url.length()) {
                urlProps.setProperty(TSDBDriver.PROPERTY_KEY_PORT, url.substring(indexOfColon + 1));
            }
            url = url.substring(0, indexOfColon);
        }
        // parse host
        if (url.length() > 0 && url.trim().length() > 0) {
            urlProps.setProperty(TSDBDriver.PROPERTY_KEY_HOST, url);
        }
        return urlProps;
    }
}
