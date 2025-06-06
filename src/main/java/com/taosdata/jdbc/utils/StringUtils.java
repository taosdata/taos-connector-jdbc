package com.taosdata.jdbc.utils;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;

import java.sql.SQLException;
import java.util.Properties;
import java.util.StringTokenizer;

public class StringUtils {

    public static boolean isEmpty(final CharSequence cs) {
        return cs == null || cs.length() == 0;
    }

    /**
     * check string every char is numeric or false
     * so string is negative number or include decimal point，will return false
     * @param str
     * @return
     */
    public static boolean isNumeric(String str) {
        if (isEmpty(str)) {
            return false;
        }

        for (int i = str.length(); --i >= 0; ) {
            if (!Character.isDigit(str.charAt(i))) {
                return false;
            }
        }

        return true;
    }

    public static Properties parseHostPort(String hostPortDb, boolean isNative) throws SQLException {
        Properties properties = new Properties();

        // parse host and port
        String host = hostPortDb;
        String port = null;

        // ipv6 address
        if (hostPortDb.startsWith("[")) {
            int endBracket = hostPortDb.indexOf(']');
            if (endBracket != -1) {
                // extract host
                if (isNative){
                    host = hostPortDb.substring(1, endBracket);
                } else {
                    host = hostPortDb.substring(0, endBracket + 1);
                }

                // if have port
                if (endBracket + 1 < hostPortDb.length() &&
                        hostPortDb.charAt(endBracket + 1) == ':') {
                    port = hostPortDb.substring(endBracket + 2);
                }
            } else {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "Invalid IPv6 address: " + hostPortDb);
            }
        }
        // ipv4 address or hostname
        else {
            // find last ':'
            int lastColon = hostPortDb.lastIndexOf(':');

            if (lastColon != -1) {
                host = hostPortDb.substring(0, lastColon);
                port = hostPortDb.substring(lastColon + 1);
            }
        }

        // handle Scope Identifier like fe80::1%eth0
        if (host.contains("%")) {
            host = host.replace("%", "%25");
        }

        // set property
        if (!host.trim().isEmpty()) {
            properties.setProperty(TSDBDriver.PROPERTY_KEY_HOST, host);
        }
        if (port != null && !port.trim().isEmpty()) {
            properties.setProperty(TSDBDriver.PROPERTY_KEY_PORT, port);
        }

        return properties;
    }

    public static Properties parseUrl(String url, Properties defaults, boolean isNative) throws SQLException {
        Properties urlProps = (defaults != null) ? defaults : new Properties();
        if (StringUtils.isEmpty(url)) {
            return urlProps;
        }

        // parse parameters
        int paramStart = url.indexOf("?");
        if (paramStart != -1) {
            String paramString = url.substring(paramStart + 1);
            url = url.substring(0, paramStart);
            StringTokenizer queryParams = new StringTokenizer(paramString, "&");
            while (queryParams.hasMoreElements()) {
                String pair = queryParams.nextToken();
                int eqIdx = pair.indexOf("=");
                if (eqIdx != -1) {
                    String key = pair.substring(0, eqIdx);
                    String value = eqIdx + 1 < pair.length() ? pair.substring(eqIdx + 1) : "";
                    if (!key.isEmpty() && !value.isEmpty()) {
                        urlProps.setProperty(key, value);
                    }
                }
            }
        }

        // parse product name
        int slashesStart = url.indexOf("//");
        String dbProductName = url.substring(0, slashesStart);
        dbProductName = dbProductName.substring(dbProductName.indexOf(":") + 1);
        dbProductName = dbProductName.substring(0, dbProductName.indexOf(":"));
        urlProps.setProperty(TSDBDriver.PROPERTY_KEY_PRODUCT_NAME, dbProductName);

        // extract host,port and dbname
        String hostPortDb = url.substring(slashesStart + 2);

        // parse dbname
        int dbStart = hostPortDb.indexOf("/");
        if (dbStart != -1) {
            if (dbStart + 1 < hostPortDb.length()) {
                urlProps.setProperty(TSDBDriver.PROPERTY_KEY_DBNAME,
                        hostPortDb.substring(dbStart + 1).toLowerCase());
            }
            hostPortDb = hostPortDb.substring(0, dbStart);
        }

        // parse host and port
        Properties hostPortProps = parseHostPort(hostPortDb, isNative);
        urlProps.putAll(hostPortProps);

        return urlProps;
    }


    public static byte[] hexToBytes(String hex)
    {
        int byteLen = hex.length() / 2;
        byte[] bytes = new byte[byteLen];

        for (int i = 0; i < hex.length() / 2; i++) {
            int i2 = 2 * i;
            if (i2 + 1 > hex.length())
                throw new IllegalArgumentException("Hex string has odd length");

            int nib1 = hexToInt(hex.charAt(i2));
            int nib0 = hexToInt(hex.charAt(i2 + 1));
            byte b = (byte) ((nib1 << 4) + (byte) nib0);
            bytes[i] = b;
        }
        return bytes;
    }
    private static int hexToInt(char hex)
    {
        int nib = Character.digit(hex, 16);
        if (nib < 0)
            throw new IllegalArgumentException("Invalid hex digit: '" + hex + "'");
        return nib;
    }

    public static String bytesToHex(byte[] bytes)
    {
        return toHex(bytes);
    }

    /**
     * Converts a byte array to a hexadecimal string.
     *
     * @param bytes a byte array
     * @return a string of hexadecimal digits
     */
    public static String toHex(byte[] bytes)
    {
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < bytes.length; i++) {
            byte b = bytes[i];
            buf.append(toHexDigit((b >> 4) & 0x0F));
            buf.append(toHexDigit(b & 0x0F));
        }
        return buf.toString();
    }

    private static char toHexDigit(int n)
    {
        if (n < 0 || n > 15)
            throw new IllegalArgumentException("Nibble value out of range: " + n);
        if (n <= 9)
            return (char) ('0' + n);
        return (char) ('A' + (n - 10));
    }

    public static String getBasicUrl(String url){
        if (url == null || url.trim().length() == 0) {
            return "";
        }
        int firstIndex = url.indexOf("?");
        if (firstIndex > 0) {
            return url.substring(0, firstIndex);
        }
        return url;
    }
}
