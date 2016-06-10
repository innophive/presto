package com.facebook.presto.type;
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.facebook.presto.Session;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.SqlTimestampWithTimeZone;
import com.facebook.presto.spi.type.TimeZoneKey;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.util.DateTimeZoneIndex.getDateTimeZone;
import static org.joda.time.DateTimeZone.UTC;

public class TestDate
    extends AbstractTestFunctions
{
    private static final TimeZoneKey TIME_ZONE_KEY = getTimeZoneKey("Europe/Berlin");
    private static final DateTimeZone DATE_TIME_ZONE = getDateTimeZone(TIME_ZONE_KEY);

    @BeforeClass
    public void setUp()
    {
        Session session = testSessionBuilder()
                .setTimeZoneKey(TIME_ZONE_KEY)
                .build();
        functionAssertions = new FunctionAssertions(session);
    }

    @Test
    public void testLiteral()
            throws Exception
    {
        long millis = new DateTime(2001, 1, 22, 0, 0, UTC).getMillis();
        assertFunction("DATE '2001-1-22'", DATE, new SqlDate((int) TimeUnit.MILLISECONDS.toDays(millis)));
    }

    @Test
    public void testEqual()
            throws Exception
    {
        assertFunction("DATE '2001-1-22' = DATE '2001-1-22'", BOOLEAN, true);
        assertFunction("DATE '2001-1-22' = DATE '2001-1-22'", BOOLEAN, true);

        assertFunction("DATE '2001-1-22' = DATE '2001-1-23'", BOOLEAN, false);
        assertFunction("DATE '2001-1-22' = DATE '2001-1-11'", BOOLEAN, false);
    }

    @Test
    public void testNotEqual()
            throws Exception
    {
        assertFunction("DATE '2001-1-22' <> DATE '2001-1-23'", BOOLEAN, true);
        assertFunction("DATE '2001-1-22' <> DATE '2001-1-11'", BOOLEAN, true);

        assertFunction("DATE '2001-1-22' <> DATE '2001-1-22'", BOOLEAN, false);
    }

    @Test
    public void testLessThan()
            throws Exception
    {
        assertFunction("DATE '2001-1-22' < DATE '2001-1-23'", BOOLEAN, true);

        assertFunction("DATE '2001-1-22' < DATE '2001-1-22'", BOOLEAN, false);
        assertFunction("DATE '2001-1-22' < DATE '2001-1-20'", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEqual()
            throws Exception
    {
        assertFunction("DATE '2001-1-22' <= DATE '2001-1-22'", BOOLEAN, true);
        assertFunction("DATE '2001-1-22' <= DATE '2001-1-23'", BOOLEAN, true);

        assertFunction("DATE '2001-1-22' <= DATE '2001-1-20'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThan()
            throws Exception
    {
        assertFunction("DATE '2001-1-22' > DATE '2001-1-11'", BOOLEAN, true);

        assertFunction("DATE '2001-1-22' > DATE '2001-1-22'", BOOLEAN, false);
        assertFunction("DATE '2001-1-22' > DATE '2001-1-23'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEqual()
            throws Exception
    {
        assertFunction("DATE '2001-1-22' >= DATE '2001-1-22'", BOOLEAN, true);
        assertFunction("DATE '2001-1-22' >= DATE '2001-1-11'", BOOLEAN, true);

        assertFunction("DATE '2001-1-22' >= DATE '2001-1-23'", BOOLEAN, false);
    }

    @Test
    public void testBetween()
            throws Exception
    {
        assertFunction("DATE '2001-1-22' between DATE '2001-1-11' and DATE '2001-1-23'", BOOLEAN, true);
        assertFunction("DATE '2001-1-22' between DATE '2001-1-11' and DATE '2001-1-22'", BOOLEAN, true);
        assertFunction("DATE '2001-1-22' between DATE '2001-1-22' and DATE '2001-1-23'", BOOLEAN, true);
        assertFunction("DATE '2001-1-22' between DATE '2001-1-22' and DATE '2001-1-22'", BOOLEAN, true);

        assertFunction("DATE '2001-1-22' between DATE '2001-1-11' and DATE '2001-1-12'", BOOLEAN, false);
        assertFunction("DATE '2001-1-22' between DATE '2001-1-23' and DATE '2001-1-24'", BOOLEAN, false);
        assertFunction("DATE '2001-1-22' between DATE '2001-1-23' and DATE '2001-1-11'", BOOLEAN, false);
    }

    @Test
    public void testCastToTimestamp()
            throws Exception
    {
        assertFunction("cast(DATE '2001-1-22' as timestamp)",
                TIMESTAMP,
                new SqlTimestamp(new DateTime(2001, 1, 22, 0, 0, 0, 0, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY));
    }

    @Test
    public void testCastToTimestampWithTimeZone()
            throws Exception
    {
        assertFunction("cast(DATE '2001-1-22' as timestamp with time zone)",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 22, 0, 0, 0, 0, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY));
    }

    @Test
    public void testCastToSlice()
            throws Exception
    {
        assertFunction("cast(DATE '2001-1-22' as varchar(11))", createVarcharType(11), "2001-01-22");
        assertFunction("cast(DATE '2001-1-22' as varchar(10))", createVarcharType(10), "2001-01-22");
        assertFunction("cast(DATE '2001-1-22' as varchar(9))", createVarcharType(9), "2001-01-2");
    }

    @Test
    public void testCastFromSlice()
            throws Exception
    {
        assertFunction("cast('2001-1-22' as date) = Date '2001-1-22'", BOOLEAN, true);
        assertFunction("cast('\n\t 2001-1-22' as date) = Date '2001-1-22'", BOOLEAN, true);
        assertFunction("cast('2001-1-22 \t\n' as date) = Date '2001-1-22'", BOOLEAN, true);
        assertFunction("cast('\n\t 2001-1-22 \t\n' as date) = Date '2001-1-22'", BOOLEAN, true);
    }

    @Test
    public void testGreatest()
            throws Exception
    {
        int days = (int) TimeUnit.MILLISECONDS.toDays(new DateTime(2013, 3, 30, 0, 0, UTC).getMillis());
        assertFunction("greatest(DATE '2013-03-30', DATE '2012-05-23')", DATE, new SqlDate(days));
        assertFunction("greatest(DATE '2013-03-30', DATE '2012-05-23', DATE '2012-06-01')", DATE, new SqlDate(days));
    }

    @Test
    public void testLeast()
            throws Exception
    {
        int days = (int) TimeUnit.MILLISECONDS.toDays(new DateTime(2012, 5, 23, 0, 0, UTC).getMillis());
        assertFunction("least(DATE '2013-03-30', DATE '2012-05-23')", DATE, new SqlDate(days));
        assertFunction("least(DATE '2013-03-30', DATE '2012-05-23', DATE '2012-06-01')", DATE, new SqlDate(days));
    }

    @Test
    public void dateCanBeImplicitlyCastedFromVarchar()
            throws Exception
    {
        assertFunction("'1999-01-01' < DATE '2000-01-01'", BOOLEAN, true);
        assertFunction("DATE '1999-01-01' < '2000-01-01'", BOOLEAN, true);

        assertFunction("'2000-01-01' = DATE '2000-01-01'", BOOLEAN, true);
        assertFunction("DATE '2000-01-01' = '2000-01-02'", BOOLEAN, false);
    }

    @Test
    public void dateIsNotImplicitlyCastToVarchar()
            throws Exception
    {
        assertFunction("'999-01-01' < DATE '2000-01-01'", BOOLEAN, true);
        assertFunction("DATE '999-01-01' < '2000-01-01'", BOOLEAN, true);
    }

    @Test
    public void dateIsImplicitlyCastedForCompareOperators()
    {
        assertEvaluates("'2000-01-01' < DATE '2000-01-01'", BOOLEAN);
        assertEvaluates("'2000-01-01' = DATE '2000-01-01'", BOOLEAN);
        assertEvaluates("'2000-01-01' > DATE '2000-01-01'", BOOLEAN);
        assertEvaluates("'2000-01-01' <> DATE '2000-01-01'", BOOLEAN);
        assertEvaluates("'2000-01-01' <= DATE '2000-01-01'", BOOLEAN);
        assertEvaluates("'2000-01-01' >= DATE '2000-01-01'", BOOLEAN);
    }

    @Test
    public void dateIsImplicitlyCastedForMostSpecificFunction()
    {
        // Test if there is a possibility of solving ambiguity by selecting
        // most specific function it's done for date format varchargit .
        assertFunction("day('2001-01-10')", BIGINT, 10L);
    }
}
