package org.zalando.opentracing.sqs;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import org.elasticmq.rest.sqs.SQSRestServer;
import org.elasticmq.rest.sqs.SQSRestServerBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IntegrationTest {
    private static SQSRestServer server;

    @BeforeClass
    public static void setUp() {
        server = SQSRestServerBuilder.start();
    }

    @AfterClass
    public static void tearDown() {
        server.stopAndWait();
    }

    @Test
    public void testTracedMessage() {
        final AmazonSQS sqs = AmazonSQSClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("a", "b")))
            .withEndpointConfiguration(new EndpointConfiguration("http://localhost:9324", "elasticmq"))
            .build();

        final String queueUrl = sqs.createQueue("meep").getQueueUrl();

        final MockTracer tracer = new MockTracer();
        final SQSTracing tracing = new SQSTracing(tracer);

        final Span span = tracing.createActiveQueueingSpan(queueUrl, "test-span");
        final MockSpan.MockContext sourceContext = (MockSpan.MockContext) span.context();

        final Map<String, MessageAttributeValue> attributes = new HashMap<>();
        attributes.put("meep", new MessageAttributeValue().withDataType("String").withStringValue("moop"));

        final SendMessageRequest sendRequest = tracing.injectInto(
            new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageBody("hello world!")
                .withMessageAttributes(attributes));

        final SendMessageResult sendResponse = sqs.sendMessage(sendRequest);

        final ReceiveMessageResult response = sqs.receiveMessage(new ReceiveMessageRequest()
            .withQueueUrl(queueUrl)
            .withMessageAttributeNames("All"));

        assertEquals("exactly one message received", 1, response.getMessages().size());
        final Map<String, SpanContext> contexts = tracing.extractFrom(response);
        final Message msg = response.getMessages().get(0);

        assertEquals("message body is correct", "hello world!", msg.getBody());
        assertEquals("pre-existing attribute is correct",
            "moop", msg.getMessageAttributes().get("meep").getStringValue());
        assertEquals("only one span context is returned", 1, contexts.size());

        assertTrue("message id is consistent", contexts.keySet().contains(msg.getMessageId()));
        final MockSpan.MockContext spanContext = (MockSpan.MockContext) contexts.values().iterator().next();
        assertEquals("received span id should be equal to sent one", spanContext.spanId(), sourceContext.spanId());
        assertEquals("received trace id should be equal to sent one", spanContext.traceId(), sourceContext.traceId());
    }
}
