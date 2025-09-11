/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019-2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.jdbc.connection;

import org.junit.Assert;
import org.junit.Test;

public class DatabaseConnectionInfoTest
{
    private static final String DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";

    @Test
    public void testEquals()
    {
        DatabaseConnectionInfo info1 = new DatabaseConnectionInfo(DRIVER_CLASS_NAME, 3306);
        DatabaseConnectionInfo info2 = new DatabaseConnectionInfo(DRIVER_CLASS_NAME, 3306);
        DatabaseConnectionInfo info3 = new DatabaseConnectionInfo("org.postgresql.Driver", 5432);
        DatabaseConnectionInfo info4 = new DatabaseConnectionInfo(DRIVER_CLASS_NAME, 3307);
        
        // Test equality
        Assert.assertEquals(info1, info2);
        Assert.assertEquals(info2, info1);

        // Test inequality with different driver
        Assert.assertNotEquals(info1, info3);
        
        // Test inequality with different port
        Assert.assertNotEquals(info1, info4);
        
        Assert.assertNotEquals(null, info1);

    }

    @Test
    public void testEqualsWithDifferentPorts()
    {
        DatabaseConnectionInfo mysql3306 = new DatabaseConnectionInfo(DRIVER_CLASS_NAME, 3306);
        DatabaseConnectionInfo mysql3307 = new DatabaseConnectionInfo(DRIVER_CLASS_NAME, 3307);
        
        Assert.assertNotEquals(mysql3306, mysql3307);
        Assert.assertNotEquals(mysql3306.hashCode(), mysql3307.hashCode());
    }

}
