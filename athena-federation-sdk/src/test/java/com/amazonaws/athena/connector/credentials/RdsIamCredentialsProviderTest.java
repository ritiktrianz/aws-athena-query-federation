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

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RdsIamCredentialsProviderTest
{
    private static final RdsIamAuthConfiguration IAM_CONFIG = new RdsIamAuthConfiguration(
            "mydb.cluster-abc123.us-east-1.rds.amazonaws.com",
            5432,
            "athena_iam",
            Region.US_EAST_1,
            null);

    @Mock
    private RdsIamAuthTokenGenerator tokenGenerator;

    @Mock
    private AwsCredentialsProvider awsCredentialsProvider;

    @Test
    public void getCredential_WhenTokenGenerated_ReturnsUserAndToken()
    {
        when(tokenGenerator.generateAuthToken(
                eq(IAM_CONFIG.getHostname()),
                eq(IAM_CONFIG.getPort()),
                eq(IAM_CONFIG.getUsername()),
                eq(IAM_CONFIG.getRegion()),
                eq(awsCredentialsProvider)))
                .thenReturn("signed-token");

        RdsIamCredentialsProvider provider = new RdsIamCredentialsProvider(IAM_CONFIG, tokenGenerator, awsCredentialsProvider);

        DefaultCredentials credentials = (DefaultCredentials) provider.getCredential();

        assertEquals("athena_iam", credentials.getUser());
        assertEquals("signed-token", credentials.getPassword());
    }

    @Test
    public void getCredential_WhenCalledTwiceBeforeExpiry_ReusesCachedToken()
    {
        when(tokenGenerator.generateAuthToken(any(), any(Integer.class), any(), any(), any()))
                .thenReturn("signed-token");

        RdsIamCredentialsProvider provider = new RdsIamCredentialsProvider(IAM_CONFIG, tokenGenerator, awsCredentialsProvider);

        DefaultCredentials firstCredential = (DefaultCredentials) provider.getCredential();
        DefaultCredentials secondCredential = (DefaultCredentials) provider.getCredential();

        assertEquals("signed-token", firstCredential.getPassword());
        assertEquals("signed-token", secondCredential.getPassword());

        verify(tokenGenerator, times(1)).generateAuthToken(
                eq(IAM_CONFIG.getHostname()),
                eq(IAM_CONFIG.getPort()),
                eq(IAM_CONFIG.getUsername()),
                eq(IAM_CONFIG.getRegion()),
                eq(awsCredentialsProvider));
    }

    @Test
    public void getCredentialMap_WhenCredentialAvailable_ReturnsCredentialProperties()
    {
        when(tokenGenerator.generateAuthToken(any(), any(Integer.class), any(), any(), any()))
                .thenReturn("signed-token");

        RdsIamCredentialsProvider provider = new RdsIamCredentialsProvider(IAM_CONFIG, tokenGenerator, awsCredentialsProvider);

        assertEquals(provider.getCredential().getProperties(), provider.getCredentialMap());
    }

    @Test(expected = AthenaConnectorException.class)
    public void getCredential_WhenTokenGenerationFails_ThrowsAthenaConnectorException()
    {
        when(tokenGenerator.generateAuthToken(any(), any(Integer.class), any(), any(), any()))
                .thenThrow(new RuntimeException("token generation failed"));

        RdsIamCredentialsProvider provider = new RdsIamCredentialsProvider(IAM_CONFIG, tokenGenerator, awsCredentialsProvider);
        provider.getCredential();
    }

    @Test
    public void getCredential_WhenCachedTokenCleared_RegeneratesToken() throws Exception
    {
        when(tokenGenerator.generateAuthToken(any(), any(Integer.class), any(), any(), any()))
                .thenReturn("token-one", "token-two");

        RdsIamCredentialsProvider provider = new RdsIamCredentialsProvider(IAM_CONFIG, tokenGenerator, awsCredentialsProvider);

        assertEquals("token-one", ((DefaultCredentials) provider.getCredential()).getPassword());

        java.lang.reflect.Field cachedTokenField = RdsIamCredentialsProvider.class.getDeclaredField("cachedToken");
        cachedTokenField.setAccessible(true);
        cachedTokenField.set(provider, null);

        assertEquals("token-two", ((DefaultCredentials) provider.getCredential()).getPassword());

        verify(tokenGenerator, times(2)).generateAuthToken(
                eq(IAM_CONFIG.getHostname()),
                eq(IAM_CONFIG.getPort()),
                eq(IAM_CONFIG.getUsername()),
                eq(IAM_CONFIG.getRegion()),
                eq(awsCredentialsProvider));
    }

    @Test
    public void getCredential_WhenCrossAccountRoleArnPresent_UsesCrossAccountCredentialsProvider()
    {
        RdsIamAuthConfiguration crossAccountConfig = new RdsIamAuthConfiguration(
                IAM_CONFIG.getHostname(),
                IAM_CONFIG.getPort(),
                IAM_CONFIG.getUsername(),
                IAM_CONFIG.getRegion(),
                "arn:aws:iam::123456789012:role/CrossAccountRole");

        AwsCredentialsProvider crossAccountProvider = mock(AwsCredentialsProvider.class);
        when(tokenGenerator.generateAuthToken(
                eq(crossAccountConfig.getHostname()),
                eq(crossAccountConfig.getPort()),
                eq(crossAccountConfig.getUsername()),
                eq(crossAccountConfig.getRegion()),
                eq(crossAccountProvider)))
                .thenReturn("signed-token");

        RdsIamCredentialsProvider provider = new RdsIamCredentialsProvider(
                crossAccountConfig, tokenGenerator, crossAccountProvider);

        DefaultCredentials credentials = (DefaultCredentials) provider.getCredential();

        assertEquals("athena_iam", credentials.getUser());
        assertEquals("signed-token", credentials.getPassword());

        verify(tokenGenerator).generateAuthToken(
                eq(crossAccountConfig.getHostname()),
                eq(crossAccountConfig.getPort()),
                eq(crossAccountConfig.getUsername()),
                eq(crossAccountConfig.getRegion()),
                eq(crossAccountProvider));
    }

    @Test
    public void constructor_WhenCrossAccountRoleArnPresent_InvokesCrossAccountCredentialsProvider()
    {
        RdsIamAuthConfiguration crossAccountConfig = new RdsIamAuthConfiguration(
                IAM_CONFIG.getHostname(),
                IAM_CONFIG.getPort(),
                IAM_CONFIG.getUsername(),
                IAM_CONFIG.getRegion(),
                "arn:aws:iam::123456789012:role/CrossAccountRole");

        AwsCredentialsProvider crossAccountProvider = mock(AwsCredentialsProvider.class);

        try (MockedStatic<CrossAccountCredentialsProvider> crossAccountStatic = mockStatic(CrossAccountCredentialsProvider.class)) {
            crossAccountStatic.when(() -> CrossAccountCredentialsProvider.getCrossAccountCredentialsIfPresent(
                            eq(Map.of(CrossAccountCredentialsProvider.CROSS_ACCOUNT_ROLE_ARN_CONFIG,
                                    "arn:aws:iam::123456789012:role/CrossAccountRole")),
                            eq("RdsIamCredentialsProvider")))
                    .thenReturn(crossAccountProvider);

            new RdsIamCredentialsProvider(crossAccountConfig);

            crossAccountStatic.verify(() -> CrossAccountCredentialsProvider.getCrossAccountCredentialsIfPresent(
                    eq(Map.of(CrossAccountCredentialsProvider.CROSS_ACCOUNT_ROLE_ARN_CONFIG,
                            "arn:aws:iam::123456789012:role/CrossAccountRole")),
                    eq("RdsIamCredentialsProvider")));
        }
    }
}
