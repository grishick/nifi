/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 package org.apache.nifi.processors.aws.sqs;

 import com.amazonaws.ClientConfiguration;
 import com.amazonaws.auth.AWSCredentials;
 import com.amazonaws.auth.AWSCredentialsProvider;
 import com.amazonaws.regions.Region;
 import com.amazonaws.regions.Regions;
 import com.amazonaws.services.s3.AmazonS3Client;
 import com.amazonaws.services.sqs.AmazonSQS;
 import com.amazonaws.services.sqs.AmazonSQSClient;
 import com.amazonaws.services.sqs.model.SendMessageRequest;
 import com.amazonaws.services.sqs.model.MessageAttributeValue;
 import com.amazonaws.services.sqs.model.SendMessageResult;
 import org.apache.nifi.annotation.behavior.InputRequirement;
 import org.apache.nifi.annotation.documentation.SeeAlso;
 import org.apache.nifi.annotation.documentation.Tags;
 import org.apache.nifi.components.PropertyDescriptor;
 import org.apache.nifi.expression.ExpressionLanguageScope;
 import org.apache.nifi.flowfile.FlowFile;
 import org.apache.nifi.processor.ProcessContext;
 import org.apache.nifi.processor.ProcessSession;
 import org.apache.nifi.processor.Relationship;
 import org.apache.nifi.processor.exception.ProcessException;
 import org.apache.nifi.processor.util.StandardValidators;
 import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;
 import com.amazon.sqs.javamessaging.AmazonSQSExtendedClient;
 import com.amazon.sqs.javamessaging.ExtendedClientConfiguration;
 import org.apache.nifi.processors.aws.AwsClientDetails;
 import org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors;
 import org.apache.nifi.processors.aws.v2.RegionUtil;
 import org.apache.nifi.ssl.SSLContextService;
 import java.io.ByteArrayOutputStream;
 import java.util.Arrays;
 import java.util.Collections;
 import java.util.LinkedHashSet;
 import java.util.List;
 import java.util.Map;
 import java.util.Set;
 import java.util.concurrent.TimeUnit;
 
 @SeeAlso({ PutSQS.class })
 @InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
 @Tags({"Amazon", "AWS", "SQS", "Queue", "Put", "Publish"})
 @CapabilityDescription("Publishes a message to an Amazon Simple Queuing Service Queue with support for large payloads")
 public class PutSQSExtended extends AbstractAWSCredentialsProviderProcessor<AmazonSQSClient> {
     public static final PropertyDescriptor QUEUE_URL = new PropertyDescriptor.Builder()
             .name("Queue URL")
             .description("The URL of the queue to act upon")
             .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
             .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
             .required(true)
             .build();
 
     public static final PropertyDescriptor DELAY = new PropertyDescriptor.Builder()
             .name("Delay")
             .displayName("Delay")
             .description("The amount of time to delay the message before it becomes available to consumers")
             .required(true)
             .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
             .defaultValue("0 secs")
             .build();
 
     public static final PropertyDescriptor BUCKET = new PropertyDescriptor.Builder()
             .name("Bucket")
             .expressionLanguageSupported(ExpressionLanguageScope.NONE)
             .required(true)
             .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
             .build();
 
 
     private volatile List<PropertyDescriptor> userDefinedProperties = Collections.emptyList();
     public static final Relationship REL_SUCCESS = new Relationship.Builder()
             .name("success")
             .description("FlowFiles are routed to success relationship")
             .build();
 
     public static final Relationship REL_FAILURE = new Relationship.Builder()
             .name("failure")
             .description("FlowFiles are routed to failure relationship")
             .build();
 
     private static final Set<Relationship> relationships = Collections.unmodifiableSet(
             new LinkedHashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE))
     );
 
     public Set<Relationship> getRelationships() {
         return relationships;
     }
 
     public static final PropertyDescriptor CREDENTIALS_FILE = CredentialPropertyDescriptors.CREDENTIALS_FILE;
 
     public static final PropertyDescriptor ACCESS_KEY = CredentialPropertyDescriptors.ACCESS_KEY;
 
     public static final PropertyDescriptor SECRET_KEY = CredentialPropertyDescriptors.SECRET_KEY;
 
     public static final PropertyDescriptor PROXY_HOST = new PropertyDescriptor.Builder()
             .name("Proxy Host")
             .description("Proxy host name or IP")
             .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
             .required(false)
             .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
             .build();
 
     public static final PropertyDescriptor PROXY_HOST_PORT = new PropertyDescriptor.Builder()
             .name("Proxy Host Port")
             .description("Proxy host port")
             .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
             .required(false)
             .addValidator(StandardValidators.PORT_VALIDATOR)
             .build();
 
     public static final PropertyDescriptor PROXY_USERNAME = new PropertyDescriptor.Builder()
             .name("proxy-user-name")
             .displayName("Proxy Username")
             .description("Proxy username")
             .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
             .required(false)
             .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
             .build();
 
     public static final PropertyDescriptor PROXY_PASSWORD = new PropertyDescriptor.Builder()
             .name("proxy-user-password")
             .displayName("Proxy Password")
             .description("Proxy password")
             .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
             .required(false)
             .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
             .sensitive(true)
             .build();
 
     public static final PropertyDescriptor REGION = new PropertyDescriptor.Builder()
             .name("Region")
             .required(true)
             .allowableValues(RegionUtil.getAvailableRegions())
             .defaultValue(RegionUtil.createAllowableValue(software.amazon.awssdk.regions.Region.US_WEST_2).getValue())
             .build();
 
     public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
             .name("Communications Timeout")
             .required(true)
             .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
             .defaultValue("30 secs")
             .build();
 
     public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
             .name("SSL Context Service")
             .description("Specifies an optional SSL Context Service that, if provided, will be used to create connections")
             .required(false)
             .identifiesControllerService(SSLContextService.class)
             .build();
 
     public static final PropertyDescriptor ENDPOINT_OVERRIDE = new PropertyDescriptor.Builder()
             .name("Endpoint Override URL")
             .description("Endpoint URL to use instead of the AWS default including scheme, host, port, and path. " +
                     "The AWS libraries select an endpoint URL based on the AWS region, but this property overrides " +
                     "the selected endpoint URL, allowing use with other S3-compatible endpoints.")
             .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
             .required(false)
             .addValidator(StandardValidators.URL_VALIDATOR)
             .build();
 
 
     public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
             Arrays.asList(QUEUE_URL, ACCESS_KEY, SECRET_KEY, CREDENTIALS_FILE, AWS_CREDENTIALS_PROVIDER_SERVICE,
                     REGION, DELAY, TIMEOUT, ENDPOINT_OVERRIDE, PROXY_HOST, PROXY_HOST_PORT, PROXY_USERNAME,
                     PROXY_PASSWORD, BUCKET));
 
     @Override
     protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
         return properties;
     }
 
     @Override
     protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
         return new PropertyDescriptor.Builder()
                 .name(propertyDescriptorName)
                 .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                 .required(false)
                 .dynamic(true)
                 .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                 .build();
     }
 
     private AmazonS3Client s3 = null;
     @Override
     protected AmazonSQSClient createClient(ProcessContext context, AWSCredentialsProvider credentialsProvider, ClientConfiguration config) {
         if (s3 == null) {
             s3 = new AmazonS3Client(credentialsProvider, config);
         }
         AmazonSQSClient simpleClient = new AmazonSQSClient(credentialsProvider, config);
         return simpleClient;
     }
 
     @Override
     public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
         FlowFile flowFile = session.get();
         if (flowFile == null) {
             return;
         }
         final long startNanos = System.nanoTime();
         final AmazonSQSClient sqs = getSQSClient(context, flowFile.getAttributes());
         final String bucket = context.getProperty(BUCKET).getValue();
         ExtendedClientConfiguration extendedClientConfig = new ExtendedClientConfiguration()
                 .withLargePayloadSupportEnabled(s3, bucket);
         final AmazonSQS sqsExtended = new AmazonSQSExtendedClient(sqs, extendedClientConfig);
         String queueUrl = context.getProperty(QUEUE_URL).evaluateAttributeExpressions(flowFile).getValue();
 
         final ByteArrayOutputStream baos = new ByteArrayOutputStream();
         session.exportTo(flowFile, baos);
         final String flowFileContent = baos.toString();
 
         final SendMessageRequest messageRequest = new SendMessageRequest(queueUrl, flowFileContent);
         messageRequest.setDelaySeconds(context.getProperty(DELAY).asTimePeriod(TimeUnit.SECONDS).intValue());
         for (final PropertyDescriptor descriptor : userDefinedProperties) {
             final String value = context.getProperty(descriptor).evaluateAttributeExpressions(flowFile).getValue();
             if (value != null) {
                 messageRequest.addMessageAttributesEntry(descriptor.getName(), new MessageAttributeValue().withDataType("String").withStringValue(value));
             }
         }
         SendMessageResult result = sqsExtended.sendMessage(messageRequest);
         // check result for errors
         if (result.getSdkHttpMetadata().getHttpStatusCode() != 200) {
             getLogger().error("Error sending message to SQS: " + result.getSdkHttpMetadata().getHttpStatusCode() + " - " + result.getSdkHttpMetadata().getHttpHeaders().toString());
             session.transfer(flowFile, REL_FAILURE);
             return;
         }
         getLogger().info("Successfully published message to Amazon SQS for {}", new Object[]{flowFile});
         session.transfer(flowFile, REL_SUCCESS);
         final long transmissionMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
         session.getProvenanceReporter().send(flowFile, queueUrl, transmissionMillis);
     }
 
     @Override
     protected AmazonSQSClient createClient(ProcessContext context, AWSCredentials credentials, ClientConfiguration config) {
         if (s3 == null) {
             s3 = new AmazonS3Client(credentials, config);
         }
         AmazonSQSClient simpleClient = new AmazonSQSClient(credentials, config);
         return simpleClient;
     }
 
     protected AmazonSQSClient getSQSClient(final ProcessContext context, final Map<String, String> attributes) {
         final AwsClientDetails clientDetails = getAwsClientDetails(context, attributes);
         return getClient(context, clientDetails);
     }
 
     private AwsClientDetails getAwsClientDetails(final ProcessContext context, final Map<String, String> attributes) {
         return new AwsClientDetails(Region.getRegion(Regions.fromName((context.getProperty(REGION).getValue()))));
     }
 }