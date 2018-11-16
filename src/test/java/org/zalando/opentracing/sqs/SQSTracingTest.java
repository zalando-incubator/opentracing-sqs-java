package org.zalando.opentracing.sqs;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.util.ThreadLocalScopeManager;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SQSTracingTest {
    private final MockTracer mockTracer = new MockTracer(new ThreadLocalScopeManager(), MockTracer.Propagator.TEXT_MAP);
    private final String defaultContext = "span_ctx";

    @Test
    public void producerMustLeaveRequestUnchangedIfNoSpanContextIsActive() {
        final SendMessageRequest request = request();
        final SendMessageRequest tracedRequest = new SQSTracing(mockTracer).injectInto(request);

        assertEquals("returned request should be the same as the original one", request, tracedRequest);
        assertEquals("request should have size one", 1, request.getMessageAttributes().size());
    }

    @Test
    public void producerMustLeaveRequestUnchangedForNullSpanContext() {
        mockTracer.buildSpan("meep").startActive(true).span()
            .setBaggageItem("baggage-key", "baggage-value");

        final SendMessageRequest request = request();
        final SendMessageRequest tracedRequest = new SQSTracing(mockTracer).injectInto(request, null);

        assertEquals("returned request should be the same as the original one", request, tracedRequest);
        assertEquals("request should have size one", 1, request.getMessageAttributes().size());
    }

    @Test
    public void producerMustAddSpanContextToSingleMessage() throws IOException {
        mockTracer.buildSpan("meep").startActive(true).span()
            .setBaggageItem("baggage-key", "baggage-value");

        final String customName = "span_context";

        final SendMessageRequest request = request();
        final SendMessageRequest tracedRequest = new SQSTracing(mockTracer, customName).injectInto(request);

        assertEquals("returned request should be the same as the (modified) original one", request,
            tracedRequest);

        assertEquals("message must have one additional attribute", 2,
            request.getMessageAttributes().size());
        asList(customName, "preexisting").forEach(key ->
            assertTrue("message attributes contain " + key, request.getMessageAttributes().containsKey(key))
        );

        final MessageAttributeValue attribute = request.getMessageAttributes().get(customName);
        assertEquals("attribute data type must be string", "String", attribute.getDataType());

        final String context = attribute.getStringValue();
        final JsonNode root = new ObjectMapper().readTree(context);

        assertTrue("context must contain span id", root.has("spanid"));
        assertTrue("context must contain trace id", root.has("traceid"));
        assertTrue("context must contain baggage", root.has("baggage-baggage-key"));
        assertEquals("baggage value must be correct", "baggage-value",
            root.get("baggage-baggage-key").asText());
    }

    @Test
    public void producerMustAddExplicitSpanContextToSingleMessage() throws IOException {
        mockTracer.buildSpan("meep").startActive(true).span()
            .setBaggageItem("baggage-key", "baggage-value");
        final Span inactive = mockTracer.buildSpan("bleep").start()
            .setBaggageItem("baggage-key", "bleep-value");
        final long spanId = ((MockSpan.MockContext) inactive.context()).spanId();
        final long traceId = ((MockSpan.MockContext) inactive.context()).traceId();

        final SendMessageRequest request = request();
        final SendMessageRequest tracedRequest = new SQSTracing(mockTracer).injectInto(request, inactive.context());

        assertEquals("returned request should be the same as the (modified) original one", request,
            tracedRequest);

        assertEquals("message must have one additional attribute", 2,
            request.getMessageAttributes().size());
        asList(defaultContext, "preexisting").forEach(key ->
            assertTrue("message attributes contain " + key, request.getMessageAttributes().containsKey(key))
        );

        final MessageAttributeValue attribute = request.getMessageAttributes().get(defaultContext);
        assertEquals("attribute data type must be string", "String", attribute.getDataType());

        final String context = attribute.getStringValue();
        final JsonNode root = new ObjectMapper().readTree(context);
        assertTrue("context must contain span id", root.has("spanid"));
        assertEquals("span id must be correct", String.valueOf(spanId), root.get("spanid").textValue());
        assertTrue("context must contain trace id", root.has("traceid"));
        assertEquals("trace id must be correct", String.valueOf(traceId), root.get("traceid").textValue());
        assertTrue("context must contain baggage", root.has("baggage-baggage-key"));
        assertEquals("baggage value must be correct", "bleep-value",
            root.get("baggage-baggage-key").asText());
    }

    @Test
    public void producerMustLeaveBatchEntryUnchangedForNullSpanContext() {
        mockTracer.buildSpan("meep").startActive(true).span()
            .setBaggageItem("baggage-key", "baggage-value");

        final SendMessageBatchRequestEntry entry = entry();
        final SendMessageBatchRequestEntry tracedEntry = new SQSTracing(mockTracer).injectInto(entry, null);

        assertEquals("returned entry should be the same as the original one", entry, tracedEntry);
        assertEquals("entry " + entry.getId() + " should have size one", 1,
            entry.getMessageAttributes().size());
    }

    @Test
    public void producerMustAddSpanContextToBatchEntry() throws IOException {
        mockTracer.buildSpan("meep").startActive(true).span()
            .setBaggageItem("baggage-key", "baggage-value");

        final SendMessageBatchRequestEntry request = entry();
        final SendMessageBatchRequestEntry tracedRequest = new SQSTracing(mockTracer)
            .injectInto(request, mockTracer.activeSpan().context());

        assertEquals("returned request should be the same as the (modified) original one", request,
            tracedRequest);

        assertEquals("message must have one additional attribute", 2,
            request.getMessageAttributes().size());
        asList(defaultContext, "preexisting").forEach(key ->
            assertTrue("message attributes contain " + key, request.getMessageAttributes().containsKey(key))
        );

        final MessageAttributeValue attribute = request.getMessageAttributes().get(defaultContext);
        assertEquals("attribute data type must be string", "String", attribute.getDataType());

        final String context = attribute.getStringValue();
        final JsonNode root = new ObjectMapper().readTree(context);
        assertTrue("context must contain span id", root.has("spanid"));
        assertTrue("context must contain trace id", root.has("traceid"));
        assertTrue("context must contain baggage", root.has("baggage-baggage-key"));
        assertEquals("baggage value must be correct", "baggage-value",
            root.get("baggage-baggage-key").asText());
    }

    @Test
    public void producerMustAddExplicitSpanContextToBatchEntry() throws IOException {
        mockTracer.buildSpan("meep").startActive(true).span()
            .setBaggageItem("baggage-key", "baggage-value");
        final Span inactive = mockTracer.buildSpan("bleep").start()
            .setBaggageItem("baggage-key", "bleep-value");
        final long spanId = ((MockSpan.MockContext) inactive.context()).spanId();
        final long traceId = ((MockSpan.MockContext) inactive.context()).traceId();

        final SendMessageBatchRequestEntry request = entry();
        final SendMessageBatchRequestEntry tracedRequest = new SQSTracing(mockTracer)
            .injectInto(request, inactive.context());

        assertEquals("returned request should be the same as the (modified) original one", request,
            tracedRequest);

        assertEquals("message must have one additional attribute", 2,
            request.getMessageAttributes().size());
        asList(defaultContext, "preexisting").forEach(key ->
            assertTrue("message attributes contain " + key, request.getMessageAttributes().containsKey(key))
        );

        final MessageAttributeValue attribute = request.getMessageAttributes().get(defaultContext);
        assertEquals("attribute data type must be string", "String", attribute.getDataType());

        final String context = attribute.getStringValue();
        final JsonNode root = new ObjectMapper().readTree(context);
        assertTrue("context must contain span id", root.has("spanid"));
        assertEquals("span id must be correct", String.valueOf(spanId), root.get("spanid").textValue());
        assertTrue("context must contain trace id", root.has("traceid"));
        assertEquals("trace id must be correct", String.valueOf(traceId), root.get("traceid").textValue());
        assertTrue("context must contain baggage", root.has("baggage-baggage-key"));
        assertEquals("baggage value must be correct", "bleep-value",
            root.get("baggage-baggage-key").asText());
    }

    @Test
    public void producerMustLeaveBatchRequestUnchangedForNullSpanContext() {
        mockTracer.buildSpan("meep").startActive(true).span()
            .setBaggageItem("baggage-key", "baggage-value");

        final SendMessageBatchRequest request = batch();
        final SendMessageBatchRequest tracedRequest = new SQSTracing(mockTracer).injectInto(request, null);

        assertEquals("returned request should be the same as the original one", request, tracedRequest);

        request.getEntries().forEach(entry ->
            assertEquals("entry " + entry.getId() + " should have size one", 1,
                entry.getMessageAttributes().size())
        );
    }

    @Test
    public void producerMustLeaveBatchRequestUnchangedIfNoSpanContextIsActive() {
        final SendMessageBatchRequest request = batch();
        final SendMessageBatchRequest tracedRequest = new SQSTracing(mockTracer).injectInto(request);

        assertEquals("returned request should be the same as the original one", request, tracedRequest);

        request.getEntries().forEach(entry ->
            assertEquals("entry " + entry.getId() + " should have size one", 1,
                entry.getMessageAttributes().size())
        );
    }

    @Test
    public void producerMustAddSpanContextToWholeMessageBatch() {
        mockTracer.buildSpan("meep").startActive(true).span()
            .setBaggageItem("baggage-key", "baggage-value");

        final SendMessageBatchRequest request = batch();
        final SendMessageBatchRequest tracedRequest = new SQSTracing(mockTracer).injectInto(request);

        assertEquals("returned request should be the same as the (modified) original one", request,
            tracedRequest);

        request.getEntries().forEach(entry -> {
            assertEquals("message must have one additional attribute", 2,
                entry.getMessageAttributes().size());
            asList(defaultContext, "preexisting").forEach(key ->
                assertTrue("message attributes contain " + key, entry.getMessageAttributes().containsKey(key))
            );

            final MessageAttributeValue attribute = entry.getMessageAttributes().get(defaultContext);
            assertEquals("attribute data type must be string", "String", attribute.getDataType());

            final String context = attribute.getStringValue();
            JsonNode root = JsonNodeFactory.instance.objectNode();
            try {
                root = new ObjectMapper().readTree(context);
            } catch (IOException e) { /* ignored in test */ }

            assertTrue("context must contain span id", root.has("spanid"));
            assertTrue("context must contain trace id", root.has("traceid"));
            assertTrue("context must contain baggage", root.has("baggage-baggage-key"));
            assertEquals("baggage value must be correct", "baggage-value",
                root.get("baggage-baggage-key").asText());
        });
    }

    @Test
    public void producerMustAddExplicitSpanContextToWholeMessageBatch() {
        mockTracer.buildSpan("meep").startActive(true).span()
            .setBaggageItem("baggage-key", "baggage-value");
        final Span inactive = mockTracer.buildSpan("bleep").start()
            .setBaggageItem("baggage-key", "bleep-value");
        final long spanId = ((MockSpan.MockContext) inactive.context()).spanId();
        final long traceId = ((MockSpan.MockContext) inactive.context()).traceId();

        final SendMessageBatchRequest request = batch();
        final SendMessageBatchRequest tracedRequest = new SQSTracing(mockTracer)
            .injectInto(request, inactive.context());

        assertEquals("returned request should be the same as the (modified) original one", request,
            tracedRequest);

        request.getEntries().forEach(entry -> {
            assertEquals("message must have one additional attribute", 2,
                entry.getMessageAttributes().size());
            asList(defaultContext, "preexisting").forEach(key ->
                assertTrue("message attributes contain " + key, entry.getMessageAttributes().containsKey(key))
            );

            final MessageAttributeValue attribute = entry.getMessageAttributes().get(defaultContext);
            assertEquals("attribute data type must be string", "String", attribute.getDataType());

            final String context = attribute.getStringValue();
            JsonNode root = JsonNodeFactory.instance.objectNode();
            try {
                root = new ObjectMapper().readTree(context);
            } catch (IOException e) { /* ignored in test */ }

            assertTrue("context must contain span id", root.has("spanid"));
            assertEquals("span id must be correct", String.valueOf(spanId), root.get("spanid").textValue());
            assertTrue("context must contain trace id", root.has("traceid"));
            assertEquals("trace id must be correct", String.valueOf(traceId), root.get("traceid").textValue());
            assertTrue("context must contain baggage", root.has("baggage-baggage-key"));
            assertEquals("baggage value must be correct", "bleep-value",
                root.get("baggage-baggage-key").asText());
        });
    }

    @Test
    public void consumerCanRetrieveSpanContextFromMessages() {
        final ReceiveMessageResult result = new ReceiveMessageResult().withMessages(
            new Message()
                .withMessageId("id-1")
                .addMessageAttributesEntry(defaultContext,
                    new MessageAttributeValue().withStringValue("{\"spanid\":\"3\",\"traceid\":\"5\"}")),
            new Message()
                .withMessageId("id-2") // no span context in this one
        );

        final Map<String, SpanContext> spanContexts = new SQSTracing(mockTracer).extractFrom(result);

        assertEquals("must return exactly one span context entry", 1, spanContexts.size());
        assertFalse("span context of second message must not be present", spanContexts.containsKey("id-2"));

        assertTrue("span context of first message must be present", spanContexts.containsKey("id-1"));

        final MockSpan.MockContext spanContext = (MockSpan.MockContext) spanContexts.get("id-1");
        assertEquals("span id must be correct", 3, spanContext.spanId());
        assertEquals("trace id must be correct", 5, spanContext.traceId());
    }

    private static SendMessageBatchRequest batch() {
        return new SendMessageBatchRequest()
            .withEntries(
                new SendMessageBatchRequestEntry()
                    .addMessageAttributesEntry("preexisting", new MessageAttributeValue().withStringValue("entry-1"))
                    .withId("id-1")
                    .withMessageBody("body-1"),
                new SendMessageBatchRequestEntry()
                    .addMessageAttributesEntry("preexisting", new MessageAttributeValue().withStringValue("entry-2"))
                    .withId("id-2")
                    .withMessageBody("body-2")
            )
            .withQueueUrl("queue-url");
    }

    private static SendMessageBatchRequestEntry entry() {
        return new SendMessageBatchRequestEntry()
            .addMessageAttributesEntry("preexisting", new MessageAttributeValue().withStringValue("entry"))
            .withId("id")
            .withMessageBody("body");
    }

    private static SendMessageRequest request() {
        return new SendMessageRequest()
            .addMessageAttributesEntry("preexisting", new MessageAttributeValue().withStringValue("entry"))
            .withMessageBody("body")
            .withQueueUrl("queue-url");
    }
}
