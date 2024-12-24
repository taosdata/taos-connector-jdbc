package com.taosdata.jdbc.utils;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DateTimeUtils {

    private static final ZoneId stdZoneId = ZoneId.of("UTC");
    private static final Pattern ptn = Pattern.compile(".*?'");
    private static final DateTimeFormatter milliSecFormatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss.SSS").toFormatter();
    private static final DateTimeFormatter microSecFormatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").toFormatter();
    private static final DateTimeFormatter nanoSecFormatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS").toFormatter();

    public static Time parseTime(String timestampStr, ZoneId zoneId) throws DateTimeParseException {
        LocalDateTime dateTime = parseLocalDateTime(timestampStr, zoneId);
        return dateTime != null ? Time.valueOf(dateTime.toLocalTime()) : null;
    }

    public static Date parseDate(String timestampStr, ZoneId zoneId) {
        LocalDateTime dateTime = parseLocalDateTime(timestampStr, zoneId);
        return dateTime != null ? Date.valueOf(dateTime.toLocalDate()) : null;
    }

    public static Timestamp parseTimestamp(String timeStampStr, ZoneId zoneId) {
        LocalDateTime dateTime = parseLocalDateTime(timeStampStr, zoneId);
        return dateTime != null ? Timestamp.valueOf(dateTime) : null;
    }

    private static LocalDateTime parseLocalDateTime(String timeStampStr, ZoneId zoneId) {
        try {
            return parseMilliSecTimestamp(timeStampStr, zoneId);
        } catch (DateTimeParseException e) {
            try {
                return parseMicroSecTimestamp(timeStampStr, zoneId);
            } catch (DateTimeParseException ee) {
                return parseNanoSecTimestamp(timeStampStr, zoneId);
            }
        }
    }

    private static LocalDateTime parseMilliSecTimestamp(String timeStampStr, ZoneId zoneId) throws DateTimeParseException {
        if (zoneId == null){
            return LocalDateTime.parse(timeStampStr, milliSecFormatter);
        } else {
            LocalDateTime dateTime = LocalDateTime.parse(timeStampStr, milliSecFormatter);
            ZonedDateTime zonedDateTime = dateTime.atZone(zoneId);
            return zonedDateTime.toLocalDateTime();
        }
    }

    private static LocalDateTime parseMicroSecTimestamp(String timeStampStr, ZoneId zoneId) throws DateTimeParseException {
        if (zoneId == null) {
            return LocalDateTime.parse(timeStampStr, microSecFormatter);
        } else {
            LocalDateTime dateTime = LocalDateTime.parse(timeStampStr, microSecFormatter);
            ZonedDateTime zonedDateTime = dateTime.atZone(zoneId);
            return zonedDateTime.toLocalDateTime();
        }
    }

    private static LocalDateTime parseNanoSecTimestamp(String timeStampStr, ZoneId zoneId) throws DateTimeParseException {
        if (zoneId == null) {
            return LocalDateTime.parse(timeStampStr, nanoSecFormatter);
        } else {
            LocalDateTime dateTime = LocalDateTime.parse(timeStampStr, nanoSecFormatter);
            ZonedDateTime zonedDateTime = dateTime.atZone(zoneId);
            return zonedDateTime.toLocalDateTime();
        }
    }

    public static Timestamp toUTC(Timestamp timestamp, ZoneId zoneId) {
        if (zoneId == null) {
            return timestamp;
        }

        Instant instant = timestamp.toInstant();
        ZonedDateTime zonedDateTime = instant.atZone(zoneId);
        ZonedDateTime utcDateTime = zonedDateTime.withZoneSameInstant(stdZoneId);
        return Timestamp.valueOf(utcDateTime.toLocalDateTime());
    }
    public static Timestamp toUTC(Timestamp timestamp, Calendar cal) {
        if (cal == null) {
            return timestamp;
        }

        TimeZone calTimeZone = cal.getTimeZone();
        ZoneId zoneId = calTimeZone.toZoneId();

        return toUTC(timestamp, zoneId);
    }
}
