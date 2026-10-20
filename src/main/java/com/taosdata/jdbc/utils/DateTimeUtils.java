/*
 * Copyright (c) 2024 TAOS Data, Inc.
 *
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.taosdata.jdbc.utils;

import com.taosdata.jdbc.enums.TimestampPrecision;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.Calendar;

public class DateTimeUtils {
    private DateTimeUtils() {
    }

    private static final ZoneId systemZoneId = ZoneId.systemDefault();
    private static final DateTimeFormatter milliSecFormatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss.SSS").toFormatter();
    private static final DateTimeFormatter microSecFormatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").toFormatter();
    private static final DateTimeFormatter nanoSecFormatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS").toFormatter();
    private static final DateTimeFormatter timestampFormatter = new DateTimeFormatterBuilder()
            .appendPattern("uuuu-MM-dd HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
            .toFormatter();

    public static Time parseTime(String timestampStr, ZoneId zoneId) throws DateTimeParseException {
        LocalDateTime dateTime = parseLocalDateTime(timestampStr);
        return dateTime != null ? Time.valueOf(dateTime.toLocalTime()) : null;
    }

    public static Date parseDate(String timestampStr, ZoneId zoneId) {
        LocalDateTime dateTime = parseLocalDateTime(timestampStr);
        return dateTime != null ? Date.valueOf(dateTime.toLocalDate()) : null;
    }

    /**
     * Parses a zone-less timestamp string. When zoneId is given, the wall-clock
     * fields are interpreted in that zone; otherwise in the JVM default zone.
     */
    public static Timestamp parseTimestamp(String timeStampStr, ZoneId zoneId) {
        LocalDateTime dateTime = parseLocalDateTime(timeStampStr);
        if (dateTime == null) {
            return null;
        }
        return zoneId == null
                ? Timestamp.valueOf(dateTime)
                : Timestamp.from(dateTime.atZone(zoneId).toInstant());
    }

    private static LocalDateTime parseLocalDateTime(String timeStampStr) {
        try {
            return LocalDateTime.parse(timeStampStr, milliSecFormatter);
        } catch (DateTimeParseException e) {
            try {
                return LocalDateTime.parse(timeStampStr, microSecFormatter);
            } catch (DateTimeParseException ee) {
                return LocalDateTime.parse(timeStampStr, nanoSecFormatter);
            }
        }
    }

    /**
     * TIMESTAMP columns store absolute instants, so the zone is intentionally
     * not applied to the value; it only affects display and zone-less literal
     * parsing.
     */
    public static Instant toInstant(Timestamp timestamp, ZoneId zoneId) {
        return timestamp == null ? null : timestamp.toInstant();
    }
    public static Instant toInstant(Timestamp timestamp, Calendar cal) {
        return timestamp == null ? null : timestamp.toInstant();
    }

    public static long toLong(Instant instant, int precision) {
        long v;
        if (precision == TimestampPrecision.MS) {
            v = instant.toEpochMilli();
        } else if (precision == TimestampPrecision.US) {
            v = instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
        } else {
            v = instant.getEpochSecond() * 1_000_000_000L + instant.getNano();
        }
        return v;
    }

    public static Long toLong(Timestamp timestamp,  ZoneId zoneId, int precision) {
        if (timestamp == null) {
            return null;
        }
        Instant instant = toInstant(timestamp, zoneId);
        return toLong(instant, precision);
    }

    public static Long toLong(LocalDateTime localDateTime,  ZoneId zoneId, int precision) {
        if (localDateTime == null) {
            return null;
        }
        ZonedDateTime zonedDateTime = localDateTime.atZone(zoneId);
        return toLong(zonedDateTime.toInstant(), precision);
    }

    public static long toLong(ZonedDateTime zonedDateTime, int precision) {
        return toLong(zonedDateTime.toInstant(), precision);
    }

    public static long toLong(OffsetDateTime offsetDateTime, int precision) {
        return toLong(offsetDateTime.toInstant(), precision);
    }



    public static Instant parseTimestampColumnData(long value, int timestampPrecision) {
        if (TimestampPrecision.MS == timestampPrecision)
            return Instant.ofEpochMilli(value);

        if (TimestampPrecision.US == timestampPrecision) {
            long epochSec = value / 1000_000L;
            long nanoAdjustment = value % 1000_000L * 1000L;
            return Instant.ofEpochSecond(epochSec, nanoAdjustment);
        }
        if (TimestampPrecision.NS == timestampPrecision) {
            long epochSec = value / 1000_000_000L;
            long nanoAdjustment = value % 1000_000_000L;
            return Instant.ofEpochSecond(epochSec, nanoAdjustment);
        }
        return null;
    }

    /**
     * Returns a Timestamp holding exactly the given instant; the zone is
     * intentionally not applied to the value. Use formatTimestamp for
     * zone-aware display.
     */
    public static Timestamp getTimestamp(Instant instant, ZoneId zoneId) {
        return Timestamp.from(instant);
    }

    /**
     * Renders the instant as a wall-clock string in the given zone, or in the
     * JVM default zone when null.
     */
    public static String formatTimestamp(Instant instant, ZoneId zoneId) {
        if (zoneId == null) {
            return Timestamp.from(instant).toString();
        }
        return timestampFormatter.format(LocalDateTime.ofInstant(instant, zoneId));
    }

    public static LocalDateTime getLocalDateTime(Instant instant, ZoneId zoneId) {
        if (zoneId == null){
            ZonedDateTime zonedDateTime = instant.atZone(systemZoneId);
            return zonedDateTime.toLocalDateTime();
        }

        ZonedDateTime zonedDateTime = instant.atZone(zoneId);
        return zonedDateTime.toLocalDateTime();
    }
    public static OffsetDateTime getOffsetDateTime(Instant instant, ZoneId zoneId) {
        if (zoneId == null){
            ZonedDateTime zonedDateTime = instant.atZone(systemZoneId);
            return zonedDateTime.toOffsetDateTime();
        }

        ZonedDateTime zonedDateTime = instant.atZone(zoneId);
        return zonedDateTime.toOffsetDateTime();
    }

    public static ZonedDateTime getZonedDateTime(Instant instant, ZoneId zoneId) {
        if (zoneId == null){
            return instant.atZone(systemZoneId);
        }

        return instant.atZone(zoneId);
    }

    public static Date getDate(Instant instant, ZoneId zoneId) {
        return new Date(instant.toEpochMilli());
    }
    public static Time getTime(Instant instant, ZoneId zoneId) {
        return new Time(instant.toEpochMilli());
    }
}
