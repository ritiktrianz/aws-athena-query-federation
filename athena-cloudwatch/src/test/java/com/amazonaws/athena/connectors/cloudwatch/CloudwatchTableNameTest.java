/*-
 * #%L
 * athena-cloudwatch
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.cloudwatch;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class CloudwatchTableNameTest {
    private static final String LOG_GROUP = "TestGroup";
    private static final String LOG_STREAM = "TestStream";
    private static final String DIFFERENT_LOG_GROUP = "DifferentGroup";
    private static final String DIFFERENT_LOG_STREAM = "DifferentStream";

    @Test
    public void testConstructorAndGetters() {
        CloudwatchTableName tableName = new CloudwatchTableName(LOG_GROUP, LOG_STREAM);
        assertEquals(LOG_GROUP, tableName.getLogGroupName());
        assertEquals(LOG_STREAM, tableName.getLogStreamName());
    }

    @Test
    public void testToTableName() {
        CloudwatchTableName tableName = new CloudwatchTableName(LOG_GROUP, LOG_STREAM);
        TableName result = tableName.toTableName();
        assertEquals(LOG_GROUP, result.getSchemaName());
        assertEquals(LOG_STREAM, result.getTableName());
    }

    @Test
    public void testToString() {
        CloudwatchTableName tableName = new CloudwatchTableName(LOG_GROUP, LOG_STREAM);
        String expected = "CloudwatchTableName{" +
                "logGroupName='" + LOG_GROUP + '\'' +
                ", logStreamName='" + LOG_STREAM + '\'' +
                '}';
        assertEquals(expected, tableName.toString());
    }

    @Test
    public void testEqualsAndHashCode() {
        CloudwatchTableName tableName1 = new CloudwatchTableName(LOG_GROUP, LOG_STREAM);
        CloudwatchTableName tableName2 = new CloudwatchTableName(LOG_GROUP, LOG_STREAM);
        CloudwatchTableName tableName3 = new CloudwatchTableName(DIFFERENT_LOG_GROUP, LOG_STREAM);
        CloudwatchTableName tableName4 = new CloudwatchTableName(LOG_GROUP, DIFFERENT_LOG_STREAM);

        assertEquals(tableName1, tableName1); // Same instance
        assertEquals(tableName1, tableName2); // Equal instances
        assertNotEquals(tableName1, tableName3); // Different log group
        assertNotEquals(tableName1, tableName4); // Different log stream
        assertNotEquals(null, tableName1); // Null comparison
        assertNotEquals(new Object(), tableName1); // Different type

        // Test hashCode
        assertEquals(tableName1.hashCode(), tableName2.hashCode()); // Equal instances have same hash
        assertNotEquals(tableName1.hashCode(), tableName3.hashCode()); // Different log group
        assertNotEquals(tableName1.hashCode(), tableName4.hashCode()); // Different log stream
    }
}
