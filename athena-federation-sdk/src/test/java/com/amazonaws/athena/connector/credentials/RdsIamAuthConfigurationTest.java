/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connector.credentials;

import org.junit.Test;
import software.amazon.awssdk.regions.Region;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class RdsIamAuthConfigurationTest
{
    private static final RdsIamAuthConfiguration CONFIG = new RdsIamAuthConfiguration(
            "mydb.us-east-1.rds.amazonaws.com",
            5432,
            "athena_iam",
            Region.US_EAST_1,
            "arn:aws:iam::123456789012:role/CrossAccountRole");

    @Test
    public void getHostname_WhenConstructed_ReturnsHostname()
    {
        assertEquals("mydb.us-east-1.rds.amazonaws.com", CONFIG.getHostname());
    }

    @Test
    public void getPort_WhenConstructed_ReturnsPort()
    {
        assertEquals(5432, CONFIG.getPort());
    }

    @Test
    public void getUsername_WhenConstructed_ReturnsUsername()
    {
        assertEquals("athena_iam", CONFIG.getUsername());
    }

    @Test
    public void getRegion_WhenConstructed_ReturnsRegion()
    {
        assertEquals(Region.US_EAST_1, CONFIG.getRegion());
    }

    @Test
    public void getCrossAccountRoleArn_WhenConstructed_ReturnsRoleArn()
    {
        assertEquals("arn:aws:iam::123456789012:role/CrossAccountRole", CONFIG.getCrossAccountRoleArn());
    }

    @Test
    public void equals_WhenSameValues_ReturnsTrue()
    {
        RdsIamAuthConfiguration other = new RdsIamAuthConfiguration(
                "mydb.us-east-1.rds.amazonaws.com",
                5432,
                "athena_iam",
                Region.US_EAST_1,
                "arn:aws:iam::123456789012:role/CrossAccountRole");
        
        assertEquals(CONFIG, other);
        assertEquals(CONFIG.hashCode(), other.hashCode());
    }

    @Test
    public void equals_WhenDifferentValues_ReturnsFalse()
    {
        RdsIamAuthConfiguration other = new RdsIamAuthConfiguration(
                "other.us-east-1.rds.amazonaws.com",
                5432,
                "athena_iam",
                Region.US_EAST_1,
                null);
        
        assertNotEquals(CONFIG, other);
        assertNotEquals(CONFIG.hashCode(), other.hashCode());
    }

    @Test
    public void equals_WhenComparedToNull_ReturnsFalse()
    {
        assertNotEquals(null, CONFIG);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_WhenHostnameBlank_ThrowsException()
    {
        new RdsIamAuthConfiguration("", 5432, "athena_iam", Region.US_EAST_1, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_WhenPortNotPositive_ThrowsException()
    {
        new RdsIamAuthConfiguration("mydb.us-east-1.rds.amazonaws.com", 0, "athena_iam", Region.US_EAST_1, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_WhenUsernameBlank_ThrowsException()
    {
        new RdsIamAuthConfiguration("mydb.us-east-1.rds.amazonaws.com", 5432, "", Region.US_EAST_1, null);
    }

    @Test(expected = NullPointerException.class)
    public void constructor_WhenRegionNull_ThrowsException()
    {
        new RdsIamAuthConfiguration("mydb.us-east-1.rds.amazonaws.com", 5432, "athena_iam", null, null);
    }
}
