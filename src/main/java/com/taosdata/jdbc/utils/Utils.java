package com.taosdata.jdbc.utils;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.rs.ConnectionParam;
import com.taosdata.jdbc.ws.entity.ConnectReq;
import com.taosdata.jdbc.ws.tmq.WSConsumer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Utils {
    private static final Logger log = LoggerFactory.getLogger(Utils.class);
    private static final ForkJoinPool forkJoinPool = new ForkJoinPool();
    private static final Pattern ptn = Pattern.compile(".*?'");

    private static EventLoopGroup eventLoopGroup = null;
    public static String escapeSingleQuota(String origin) {
        Matcher m = ptn.matcher(origin);
        StringBuilder sb = new StringBuilder();
        int end = 0;
        while (m.find()) {
            end = m.end();
            String seg = origin.substring(m.start(), end);
            int len = seg.length();
            if (len == 1) {
                if ('\'' == seg.charAt(0)) {
                    sb.append("\\'");
                } else {
                    sb.append(seg);
                }
            } else { // len > 1
                sb.append(seg, 0, seg.length() - 2);
                char lastcSec = seg.charAt(seg.length() - 2);
                if (lastcSec == '\\') {
                    sb.append("\\'");
                } else {
                    sb.append(lastcSec);
                    sb.append("\\'");
                }
            }
        }

        if (end < origin.length()) {
            sb.append(origin.substring(end));
        }
        return sb.toString();
    }

    public static String getNativeSql(String rawSql, Object[] parameters) {
        if (parameters == null || !rawSql.contains("?"))
            return rawSql;
        // toLowerCase
        String preparedSql = rawSql.trim().toLowerCase();
        String[] clause = new String[]{"tags\\s*\\([\\s\\S]*?\\)", "where[\\s\\S]*"};
        Map<Integer, Integer> placeholderPositions = new HashMap<>();
        RangeSet<Integer> clauseRangeSet = TreeRangeSet.create();
        findPlaceholderPosition(preparedSql, placeholderPositions);
        // find tags and where clause's position
        findClauseRangeSet(preparedSql, clause, clauseRangeSet);
        // find values clause's position
        findValuesClauseRangeSet(preparedSql, clauseRangeSet);

        return transformSql(rawSql, parameters, placeholderPositions, clauseRangeSet);
    }

    private static void findValuesClauseRangeSet(String preparedSql, RangeSet<Integer> clauseRangeSet) {
        Matcher matcher = Pattern.compile("(values||,)\\s*(\\([^)]*\\))").matcher(preparedSql);
        while (matcher.find()) {
            int start = matcher.start(2);
            int end = matcher.end(2);
            clauseRangeSet.add(Range.closedOpen(start, end));
        }
    }

    private static void findClauseRangeSet(String preparedSql, String[] regexArr, RangeSet<Integer> clauseRangeSet) {
        clauseRangeSet.clear();
        for (String regex : regexArr) {
            Matcher matcher = Pattern.compile(regex).matcher(preparedSql);
            while (matcher.find()) {
                int start = matcher.start();
                int end = matcher.end();
                clauseRangeSet.add(Range.closedOpen(start, end));
            }
        }
    }

    private static void findPlaceholderPosition(String preparedSql, Map<Integer, Integer> placeholderPosition) {
        placeholderPosition.clear();
        Matcher matcher = Pattern.compile("\\?").matcher(preparedSql);
        int index = 0;
        while (matcher.find()) {
            int pos = matcher.start();
            placeholderPosition.put(index, pos);
            index++;
        }
    }

    /***
     *
     * @param rawSql
     * @param paramArr
     * @param placeholderPosition
     * @param clauseRangeSet
     * @return
     */
    private static String transformSql(String rawSql, Object[] paramArr, Map<Integer, Integer> placeholderPosition, RangeSet<Integer> clauseRangeSet) {
        String[] sqlArr = rawSql.split("\\?");

        return IntStream.range(0, sqlArr.length).mapToObj(index -> {
            if (index == paramArr.length)
                return sqlArr[index];

            Object para = paramArr[index];
            String paraStr;
            if (para != null) {
                if (para instanceof byte[]) {
                    paraStr = new String((byte[]) para, StandardCharsets.UTF_8);
                } else {
                    paraStr = para.toString();
                }
                // if para is timestamp or String or byte[] need to translate ' character
                if (para instanceof Timestamp || para instanceof String || para instanceof byte[]) {
                    paraStr = Utils.escapeSingleQuota(paraStr);

                    Integer pos = placeholderPosition.get(index);
                    boolean contains = clauseRangeSet.contains(pos);
                    if (contains) {
                        paraStr = "'" + paraStr + "'";
                    }
                }
            } else {
                paraStr = "NULL";
            }
            return sqlArr[index] + paraStr;
        }).collect(Collectors.joining());
    }

    public static ClassLoader getClassLoader() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null)
            return Utils.class.getClassLoader();
        else
            return cl;
    }

    public static Class<?> parseClassType(String key) {
        ClassLoader contextClassLoader = Utils.getClassLoader();
        try {
            return Class.forName(key, true, contextClassLoader);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Instantiate the class
     */
    public static <T> T newInstance(Class<T> c) {
        if (c == null)
            throw new RuntimeException("class cannot be null");
        try {
            Constructor<T> declaredConstructor = c.getDeclaredConstructor();
            declaredConstructor.setAccessible(true);
            return declaredConstructor.newInstance();
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Could not find a public no-argument constructor for " + c.getName(), e);
        } catch (ReflectiveOperationException | RuntimeException e) {
            throw new RuntimeException("Could not instantiate class " + c.getName(), e);
        }
    }

    public static boolean isValidIP(String ip) {
        try {
            InetAddress address = InetAddress.getByName(ip);
            return true;
        } catch (UnknownHostException e) {
            return false;
        }
    }

    public static ForkJoinPool getForkJoinPool() {
        return forkJoinPool;
    }
    public static int getSqlRows(Connection connection, String sql) throws SQLException {
        sql = "select count(*) from " + sql;
        try(Statement statement = connection.createStatement()){
            statement.execute(sql);
            try (ResultSet rs = statement.getResultSet()){
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    public static void initEventLoopGroup() {
        // Initialize the EventLoopGroup
        // This is just a placeholder, you need to implement the actual initialization logic
        // For example, you might want to use NioEventLoopGroup or another implementation
        // based on your requirements.
        // Example:
        //
        eventLoopGroup = new NioEventLoopGroup(0, new DefaultThreadFactory("netty-eventloop", true));
        //ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }
    public static EventLoopGroup getEventLoopGroup() {
        return eventLoopGroup;
    }

    public static void releaseByteBuf(ByteBuf byteBuf){
        if (log.isTraceEnabled()){
            String stackTrace = Arrays.stream(Thread.currentThread().getStackTrace())
                    .limit(5)
                    .map(StackTraceElement::toString)
                    .collect(Collectors.joining("\n\t"));

            log.trace("ByteBuf release called, addr: {}, refCnt: {}, Caller Stack Trace:{}}",
                    Integer.toHexString(System.identityHashCode(byteBuf)),
                    byteBuf.refCnt(),
                    stackTrace
            );
        }

        if (byteBuf.refCnt() > 0){
            ReferenceCountUtil.safeRelease(byteBuf);
        } else {
            log.error("ByteBuf {} already released, refCnt: {}",
                    Integer.toHexString(System.identityHashCode(byteBuf)),
                    byteBuf.refCnt());
        }
    }

    public static void retainByteBuf(ByteBuf byteBuf){
        if (log.isTraceEnabled()){
            String stackTrace = Arrays.stream(Thread.currentThread().getStackTrace())
                    .limit(5)
                    .map(StackTraceElement::toString)
                    .collect(Collectors.joining("\n\t"));

            log.trace("ByteBuf retain called, addr: {}, refCnt: {}, Caller Stack Trace:{}}",
                    Integer.toHexString(System.identityHashCode(byteBuf)),
                    byteBuf.refCnt(),
                    stackTrace
            );
        }
        if (byteBuf.refCnt() > 0){
            byteBuf.retain();
        } else {
            log.error("ByteBuf already released, addr: {}, refCnt: {}",
                    Integer.toHexString(System.identityHashCode(byteBuf)),
                    byteBuf.refCnt());
        }
    }
}
