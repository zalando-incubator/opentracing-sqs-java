package de.zalando.opentracing.sqs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.util.ThreadLocalScopeManager;
import org.junit.Test;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SQSTracingTestForAws2 {
    private final MockTracer mockTracer = new MockTracer(new ThreadLocalScopeManager(), MockTracer.Propagator.TEXT_MAP);
    private final String defaultContext = "span_ctx";

    @Test
    public void producerMustLeaveRequestUnchangedWhenNoContextIsActive() {
        final SendMessageRequest request = request();
        final SendMessageRequest traced = new SQSTracing(mockTracer).injectInto(request);

        assertEquals("traced request must stay unchanged", traced, request);
    }

    @Test
    public void producerMustLeaveRequestUnchangedForNullContext() {
        mockTracer.buildSpan("meep").startActive(true).span()
            .setBaggageItem("baggage-key", "baggage-value");

        final SendMessageRequest request = request();
        final SendMessageRequest traced = new SQSTracing(mockTracer).injectInto(request, null);

        assertEquals("traced request must stay unchanged", traced, request);
    }

    @Test
    public void producerMustAddSpanContextToSingleMessage() throws IOException {
        mockTracer.buildSpan("meep").startActive(true).span()
            .setBaggageItem("baggage-key", "baggage-value");

        final String customName = "span_context";

        final SendMessageRequest request = request();
        final SendMessageRequest traced = new SQSTracing(mockTracer, customName).injectInto(request);

        assertEquals("message must have one additional attribute", 2,
            traced.messageAttributes().size());
        asList(customName, "preexisting").forEach(key ->
            assertTrue("message attributes contain " + key, traced.messageAttributes().containsKey(key))
        );

        final MessageAttributeValue attribute = traced.messageAttributes().get(customName);
        assertEquals("attribute data type must be string", "String", attribute.dataType());

        final String context = attribute.stringValue();
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
        final SendMessageRequest traced = new SQSTracing(mockTracer).injectInto(request, inactive.context());

        assertEquals("message must have one additional attribute", 2,
            traced.messageAttributes().size());
        asList(defaultContext, "preexisting").forEach(key ->
            assertTrue("message attributes contain " + key, traced.messageAttributes().containsKey(key))
        );

        final MessageAttributeValue attribute = traced.messageAttributes().get(defaultContext);
        assertEquals("attribute data type must be string", "String", attribute.dataType());

        final String context = attribute.stringValue();
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
    public void producerMustLeaveBatchEntryUnchangedForNullContext() {
        final SendMessageBatchRequestEntry request = entry();
        final SendMessageBatchRequestEntry traced = new SQSTracing(mockTracer).injectInto(request, null);

        assertEquals("traced request must stay unchanged", traced, request);
    }

    @Test
    public void producerMustAddSpanContextToBatchEntry() throws IOException {
        mockTracer.buildSpan("meep").startActive(true).span()
            .setBaggageItem("baggage-key", "baggage-value");

        final SendMessageBatchRequestEntry request = entry();
        final SendMessageBatchRequestEntry traced = new SQSTracing(mockTracer)
            .injectInto(request, mockTracer.activeSpan().context());

        assertEquals("message must have one additional attribute", 2,
            traced.messageAttributes().size());
        asList(defaultContext, "preexisting").forEach(key ->
            assertTrue("message attributes contain " + key, traced.messageAttributes().containsKey(key))
        );

        final MessageAttributeValue attribute = traced.messageAttributes().get(defaultContext);
        assertEquals("attribute data type must be string", "String", attribute.dataType());

        final String context = attribute.stringValue();
        final JsonNode root = new ObjectMapper().readTree(context);
        assertTrue("context must contain span id", root.has("spanid"));
        assertTrue("context must contain trace id", root.has("traceid"));
        assertTrue("context must contain baggage", root.has("baggage-baggage-key"));
        assertEquals("baggage value must be correct", "baggage-value",
            root.get("baggage-baggage-key").asText());
    }

    @Test
    public void producerMustLeaveBatchRequestUnchangedWhenNoContextIsActive() {
        final SendMessageBatchRequest request = batch();
        final SendMessageBatchRequest traced = new SQSTracing(mockTracer).injectInto(request);

        assertEquals("traced request must stay unchanged", traced, request);
    }

    @Test
    public void producerMustLeaveBatchRequestUnchangedForNullContext() {
        mockTracer.buildSpan("meep").startActive(true).span()
            .setBaggageItem("baggage-key", "baggage-value");

        final SendMessageBatchRequest request = batch();
        final SendMessageBatchRequest traced = new SQSTracing(mockTracer).injectInto(request, null);

        assertEquals("traced request must stay unchanged", traced, request);
    }

    @Test
    public void producerMustAddSpanContextToWholeMessageBatch() {
        mockTracer.buildSpan("meep").startActive(true).span()
            .setBaggageItem("baggage-key", "baggage-value");
        final Span inactive = mockTracer.buildSpan("bleep").start()
            .setBaggageItem("baggage-key", "bleep-value");
        final long spanId = ((MockSpan.MockContext) inactive.context()).spanId();
        final long traceId = ((MockSpan.MockContext) inactive.context()).traceId();

        final SendMessageBatchRequest request = batch();
        final SendMessageBatchRequest traced = new SQSTracing(mockTracer).injectInto(request, inactive.context());

        traced.entries().forEach(entry -> {
            assertEquals("message must have one additional attribute", 2,
                entry.messageAttributes().size());
            asList(defaultContext, "preexisting").forEach(key ->
                assertTrue("message attributes contain " + key, entry.messageAttributes().containsKey(key))
            );

            final MessageAttributeValue attribute = entry.messageAttributes().get(defaultContext);
            assertEquals("attribute data type must be string", "String", attribute.dataType());

            final String context = attribute.stringValue();
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
    public void producerMustAddExplicitSpanContextToWholeMessageBatch() {
        mockTracer.buildSpan("meep").startActive(true).span()
            .setBaggageItem("baggage-key", "baggage-value");

        final SendMessageBatchRequest request = batch();
        final SendMessageBatchRequest traced = new SQSTracing(mockTracer).injectInto(request);

        traced.entries().forEach(entry -> {
            assertEquals("message must have one additional attribute", 2,
                entry.messageAttributes().size());
            asList(defaultContext, "preexisting").forEach(key ->
                assertTrue("message attributes contain " + key, entry.messageAttributes().containsKey(key))
            );

            final MessageAttributeValue attribute = entry.messageAttributes().get(defaultContext);
            assertEquals("attribute data type must be string", "String", attribute.dataType());

            final String context = attribute.stringValue();
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
    public void consumerCanRetrieveSpanContextFromMessages() {
        final ReceiveMessageResponse response = ReceiveMessageResponse.builder().messages(
            Message.builder().messageId("id-1")
                .messageAttributes(simpleAttributes("span_ctx", "{\"spanid\":\"3\",\"traceid\":\"5\"}")).build(),
            Message.builder().messageId(("id-2")).build() // no span context in this one
        ).build();

        final Map<String, SpanContext> spanContexts = new SQSTracing(mockTracer).extractFrom(response);

        assertEquals("must return exactly one span context entry", 1, spanContexts.size());
        assertFalse("span context of second message must not be present", spanContexts.containsKey("id-2"));

        assertTrue("span context of first message must be present", spanContexts.containsKey("id-1"));

        final MockSpan.MockContext spanContext = (MockSpan.MockContext) spanContexts.get("id-1");
        assertEquals("span id must be correct", 3, spanContext.spanId());
        assertEquals("trace id must be correct", 5, spanContext.traceId());
    }

    private static SendMessageRequest request() {
        return SendMessageRequest.builder()
            .messageAttributes(simpleAttributes("preexisting", "entry"))
            .messageBody("body")
            .queueUrl("queue-url")
            .build();
    }

    private static SendMessageBatchRequestEntry entry() {
        return SendMessageBatchRequestEntry.builder()
            .messageAttributes(simpleAttributes("preexisting", "entry"))
            .id("id")
            .messageBody("body")
            .build();
    }

    private static SendMessageBatchRequest batch() {
        return SendMessageBatchRequest.builder().entries(
            SendMessageBatchRequestEntry.builder()
                .messageAttributes(simpleAttributes("preexisting", "entry-1"))
                .id("id-1")
                .messageBody("body-1")
                .build(),
            SendMessageBatchRequestEntry.builder()
                .messageAttributes(simpleAttributes("preexisting", "entry-2"))
                .id("id-2")
                .messageBody("body-2")
                .build()
        ).build();
    }

    private static Map<String, MessageAttributeValue> simpleAttributes(final String key, final String value) {
        final Map<String, MessageAttributeValue> attributes = new HashMap<>();
        attributes.put(key, MessageAttributeValue.builder().stringValue(value).build());
        return attributes;
    }
}
