/*-
 * #%L
 * athena-example
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;
import com.google.cloud.storage.StorageException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.glue.model.GlueException;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateEncodingException;

import static com.amazonaws.athena.connectors.gcs.GcsUtil.installCaCertificate;
import static com.amazonaws.athena.connectors.gcs.GcsUtil.installGoogleCredentialsJsonFile;
import static com.amazonaws.athena.connectors.gcs.GcsUtil.setupNativeEnvironmentVariables;

/**
 * Boilerplate composite handler that allows us to use a single Lambda function for both
 * Metadata and Data.
 */
@SuppressWarnings("unused")
public class GcsCompositeHandler
        extends CompositeHandler
{
    private static final BufferAllocator allocator = new RootAllocator();
    private static final int MAX_CAUSE_DEPTH = 20;
    /**
     * The default constructor that initializes metadata and record handlers for GCS
     */
    public GcsCompositeHandler() throws IOException, CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException
    {
        super(new GcsMetadataHandler(allocator, new GcsEnvironmentProperties().createEnvironment()), new GcsRecordHandler(allocator, new GcsEnvironmentProperties().createEnvironment()));
        installCaCertificate();
        installGoogleCredentialsJsonFile(new GcsEnvironmentProperties().createEnvironment());
        setupNativeEnvironmentVariables();
    }

    @Override
    public Exception handleException(Exception e)
    {
        return handleGcsException(super.handleException(e));
    }

    /**
     * Transforms Cloud Storage and Glue exceptions into an AthenaConnectorException with the matching
     * FederationSourceErrorCode. The HTTP status of the failed call is mapped with mapHTTPErrorCode.
     * Returns other exceptions unchanged.
     *
     * @param e The exception to handle
     * @return The transformed exception or the original exception
     */
    public static Exception handleGcsException(Exception e)
    {
        if (e instanceof AthenaConnectorException) {
            return e;
        }
        // Depth limit guards against cause cycles (A -> B -> A), which would otherwise loop forever.
        int depth = 0;
        for (Throwable current = e; current != null && depth < MAX_CAUSE_DEPTH; current = current.getCause(), depth++) {
            FederationSourceErrorCode errorCode = classify(current);
            if (errorCode != null) {
                String message = current.getMessage() != null ? current.getMessage() : current.getClass().getSimpleName();
                return new AthenaConnectorException(message, e, ErrorDetails.builder()
                        .errorCode(errorCode.toString())
                        .errorMessage(message)
                        .build());
            }
        }
        return e;
    }

    private static FederationSourceErrorCode classify(Throwable t)
    {
        if (t instanceof EntityNotFoundException) {
            return FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION;
        }
        if (t instanceof GlueException) {
            return mapStatusCode(((GlueException) t).statusCode());
        }
        if (t instanceof StorageException) {
            return mapStatusCode(((StorageException) t).getCode());
        }
        return null;
    }

    /**
     * A status of 0 means no HTTP response was received (for example a network failure), so it is
     * reported as an internal service error rather than as invalid input.
     */
    private static FederationSourceErrorCode mapStatusCode(int statusCode)
    {
        if (statusCode <= 0) {
            return FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION;
        }
        return mapHTTPErrorCode(statusCode);
    }
}
