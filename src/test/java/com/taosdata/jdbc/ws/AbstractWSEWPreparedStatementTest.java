package com.taosdata.jdbc.ws;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AbstractWSEWPreparedStatementTest {

    @Test
    public void writeQueueSkew_isDetectedWhenSingleQueueIsHotAndOthersAreBelowBatchSize() {
        AbstractWSEWPreparedStatement.QueueSkewStats stats =
                AbstractWSEWPreparedStatement.analyzeWriteQueueSkew(new int[]{12, 1, 1, 1}, 4);

        assertTrue(stats.skewed);
        assertEquals(0, stats.maxQueueIndex);
        assertEquals(12, stats.maxQueueSize);
        assertEquals(1.0, stats.otherAverageQueueSize, 0.001);
    }

    @Test
    public void writeQueueSkew_isIgnoredWhenOtherQueuesAreAlsoBacklogged() {
        AbstractWSEWPreparedStatement.QueueSkewStats stats =
                AbstractWSEWPreparedStatement.analyzeWriteQueueSkew(new int[]{40, 12, 12, 12}, 4);

        assertFalse(stats.skewed);
        assertEquals(12.0, stats.otherAverageQueueSize, 0.001);
    }

    @Test
    public void writeQueueSkew_requiresHotQueueToReachOneBatchWhenOtherQueuesAreEmpty() {
        AbstractWSEWPreparedStatement.QueueSkewStats belowBatch =
                AbstractWSEWPreparedStatement.analyzeWriteQueueSkew(new int[]{3, 0, 0, 0}, 4);
        AbstractWSEWPreparedStatement.QueueSkewStats atBatch =
                AbstractWSEWPreparedStatement.analyzeWriteQueueSkew(new int[]{4, 0, 0, 0}, 4);

        assertFalse(belowBatch.skewed);
        assertTrue(atBatch.skewed);
    }
}
