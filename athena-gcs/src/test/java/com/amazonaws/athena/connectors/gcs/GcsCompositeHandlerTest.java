/*-
 * #%L
 * athena-gcs
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.gcs;

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.StorageException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateEncodingException;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@TestInstance(PER_CLASS)
public class GcsCompositeHandlerTest extends GenericGcsTest {

    private SecretsManagerClient secretsManager;
    private ServiceAccountCredentials serviceAccountCredentials;
    private GoogleCredentials credentials;

    @BeforeAll
    public void init() {
        super.initCommonMockedStatic();
        secretsManager = Mockito.mock(SecretsManagerClient.class);
        mockedSecretManagerBuilder.when(SecretsManagerClient::create).thenReturn(secretsManager);
        serviceAccountCredentials = Mockito.mock(ServiceAccountCredentials.class);
        mockedServiceAccountCredentials.when(() -> ServiceAccountCredentials.fromStream(Mockito.any())).thenReturn(serviceAccountCredentials);
        credentials = Mockito.mock(GoogleCredentials.class);
        mockedGoogleCredentials.when(() -> GoogleCredentials.fromStream(Mockito.any())).thenReturn(credentials);
        S3Client mockedAmazonS3 = Mockito.mock(S3Client.class);
        when(S3Client.create()).thenReturn(mockedAmazonS3);
    }

    @AfterAll
    public void cleanUp() {
        super.closeMockedObjects();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGcsCompositeHandler() throws IOException, CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException
    {
        GetSecretValueResponse getSecretValueResponse = GetSecretValueResponse.builder()
                .versionStages(com.google.common.collect.ImmutableList.of("v1"))
                .secretString("{\"gcs_credential_keys\": \"test\"}")
                .build();
        when(secretsManager.getSecretValue(Mockito.isA(GetSecretValueRequest.class))).thenReturn(getSecretValueResponse);
        when(ServiceAccountCredentials.fromStream(Mockito.any())).thenReturn(serviceAccountCredentials);
        when(credentials.createScoped((Collection<String>) any())).thenReturn(credentials);
        GcsCompositeHandler gcsCompositeHandler = new GcsCompositeHandler();
        assertInstanceOf(GcsCompositeHandler.class, gcsCompositeHandler);
        // The composite handler should classify storage errors before they reach Athena.
        Exception classified = gcsCompositeHandler.handleException(new StorageException(403, "Access denied"));
        assertInstanceOf(AthenaConnectorException.class, classified);
    }

    @Test
    public void handleGcsException_storageForbidden_returnsAccessDenied()
    {
        assertClassified(new StorageException(403, "Access denied"), FederationSourceErrorCode.ACCESS_DENIED_EXCEPTION);
    }

    @Test
    public void handleGcsException_storageNotFound_returnsEntityNotFound()
    {
        assertClassified(new StorageException(404, "The specified bucket does not exist."), FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION);
    }

    @Test
    public void handleGcsException_storageUnauthorized_returnsInvalidCredentials()
    {
        assertClassified(new StorageException(401, "Invalid credentials"), FederationSourceErrorCode.INVALID_CREDENTIALS_EXCEPTION);
    }

    @Test
    public void handleGcsException_storageTooManyRequests_returnsThrottling()
    {
        assertClassified(new StorageException(429, "Rate limit exceeded"), FederationSourceErrorCode.THROTTLING_EXCEPTION);
    }

    @Test
    public void handleGcsException_storageServerError_returnsInternalService()
    {
        assertClassified(new StorageException(503, "Service unavailable"), FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION);
    }

    @Test
    public void handleGcsException_storageWithoutHttpStatus_returnsInternalService()
    {
        // A status of 0 means no HTTP response was received, for example a network failure.
        assertClassified(new StorageException(0, "Connection reset"), FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION);
    }

    @Test
    public void handleGcsException_glueEntityNotFound_returnsEntityNotFound()
    {
        assertClassified(EntityNotFoundException.builder().message("Table not found").build(),
                FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION);
    }

    @Test
    public void handleGcsException_glueForbidden_returnsAccessDenied()
    {
        GlueException glueException = (GlueException) GlueException.builder().statusCode(403).message("Not authorized").build();
        assertClassified(glueException, FederationSourceErrorCode.ACCESS_DENIED_EXCEPTION);
    }

    @Test
    public void handleGcsException_storageExceptionWrappedInRuntimeException_returnsClassifiedError()
    {
        Exception wrapped = new RuntimeException("Failed to list objects", new StorageException(404, "Not found"));
        assertClassified(wrapped, FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION);
    }

    @Test
    public void handleGcsException_alreadyClassified_returnsSameInstance()
    {
        AthenaConnectorException existing = new AthenaConnectorException("already classified",
                ErrorDetails.builder()
                        .errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString())
                        .build());
        assertSame(existing, GcsCompositeHandler.handleGcsException(existing));
    }

    @Test
    public void handleGcsException_causeCycle_returnsOriginalWithoutLooping()
    {
        RuntimeException a = new RuntimeException("a");
        RuntimeException b = new RuntimeException("b", a);
        a.initCause(b);
        assertSame(a, GcsCompositeHandler.handleGcsException(a));
    }

    @Test
    public void handleGcsException_unrelatedException_returnsSameInstance()
    {
        Exception unrelated = new IllegalStateException("something else");
        assertSame(unrelated, GcsCompositeHandler.handleGcsException(unrelated));
    }

    private void assertClassified(Exception input, FederationSourceErrorCode expectedCode)
    {
        Exception result = GcsCompositeHandler.handleGcsException(input);
        AthenaConnectorException ex = assertInstanceOf(AthenaConnectorException.class, result);
        assertEquals(expectedCode.toString(), ex.getErrorDetails().errorCode());
    }
}
