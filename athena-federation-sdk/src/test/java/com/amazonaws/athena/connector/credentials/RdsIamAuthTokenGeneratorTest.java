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
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.RdsClientBuilder;
import software.amazon.awssdk.services.rds.RdsUtilities;
import software.amazon.awssdk.services.rds.model.GenerateAuthenticationTokenRequest;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RdsIamAuthTokenGeneratorTest
{
    @Mock
    private AwsCredentialsProvider awsCredentialsProvider;

    @Test
    public void generateAuthToken_WhenInputValid_ReturnsGeneratedToken()
    {
        RdsClient rdsClient = mock(RdsClient.class);
        RdsUtilities rdsUtilities = mock(RdsUtilities.class);
        RdsClientBuilder rdsClientBuilder = mock(RdsClientBuilder.class);

        when(rdsClientBuilder.region(any(Region.class))).thenReturn(rdsClientBuilder);
        when(rdsClientBuilder.credentialsProvider(any(AwsCredentialsProvider.class))).thenReturn(rdsClientBuilder);
        when(rdsClientBuilder.build()).thenReturn(rdsClient);
        when(rdsClient.utilities()).thenReturn(rdsUtilities);
        when(rdsUtilities.generateAuthenticationToken(any(GenerateAuthenticationTokenRequest.class))).thenReturn("signed-token");

        try (MockedStatic<RdsClient> rdsClientStatic = mockStatic(RdsClient.class)) {
            rdsClientStatic.when(RdsClient::builder).thenReturn(rdsClientBuilder);

            String token = new RdsIamAuthTokenGenerator().generateAuthToken(
                    "mydb.us-east-1.rds.amazonaws.com",
                    5432,
                    "athena_iam",
                    Region.US_EAST_1,
                    awsCredentialsProvider);

            assertEquals("signed-token", token);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void generateAuthToken_WhenHostnameBlank_ThrowsException()
    {
        new RdsIamAuthTokenGenerator().generateAuthToken("", 5432, "athena_iam", Region.US_EAST_1, awsCredentialsProvider);
    }

    @Test(expected = IllegalArgumentException.class)
    public void generateAuthToken_WhenPortNotPositive_ThrowsException()
    {
        new RdsIamAuthTokenGenerator().generateAuthToken("host", 0, "athena_iam", Region.US_EAST_1, awsCredentialsProvider);
    }

    @Test(expected = IllegalArgumentException.class)
    public void generateAuthToken_WhenUsernameBlank_ThrowsException()
    {
        new RdsIamAuthTokenGenerator().generateAuthToken("host", 5432, "", Region.US_EAST_1, awsCredentialsProvider);
    }

    @Test(expected = NullPointerException.class)
    public void generateAuthToken_WhenRegionNull_ThrowsException()
    {
        new RdsIamAuthTokenGenerator().generateAuthToken("host", 5432, "athena_iam", null, awsCredentialsProvider);
    }

    @Test(expected = NullPointerException.class)
    public void generateAuthToken_WhenCredentialsProviderNull_ThrowsException()
    {
        new RdsIamAuthTokenGenerator().generateAuthToken("host", 5432, "athena_iam", Region.US_EAST_1, null);
    }
}
