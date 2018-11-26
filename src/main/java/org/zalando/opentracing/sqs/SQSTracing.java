package org.zalando.opentracing.sqs;

import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.TextMapExtractAdapter;
import io.opentracing.propagation.TextMapInjectAdapter;
import io.opentracing.tag.Tags;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.opentracing.References.FOLLOWS_FROM;
import static io.opentracing.propagation.Format.Builtin.TEXT_MAP;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * Utility for injecting OpenTracing-based tracing information into SQS messages, as well as extracting it again
 * from SQS messages. This information is injected as a single SQS message attribute called "span_context", and
 * encoded as a simple JSON structure.
 * <p>
 * Note that SQS message attributes have a limit of (currently) 256 KB, so huge span contexts won't work properly.
 * However, in practice, span contexts should be kept as small as possible anyway.
 * <p>
 * This utility works with both the AWS SDK and the AWS SDK 2.0.
 */
public final class SQSTracing {

    /**
     * Constant for the default name of the SQS message attribute key that is used to store the span context.
     */
    private static final String SPAN_CONTEXT_ATTRIBUTE_KEY = "span_ctx";

    /**
     * Constant for the name of the "String" datatype of SQS.
     */
    private static final String STRING_DATA_TYPE = "String";

    // the mapper is thread-safe, so we can keep around just one instance
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Tracer tracer;

    /**
     * The name to use for the message attribute key to store the span context in.
     */
    private final String attributeKey;

    /**
     * Initializes this utility for a given tracer implementation.
     * This is a lightweight operation, so you don't have to keep the instance around everywhere.
     * <p>
     * This constructor will use {@link #SPAN_CONTEXT_ATTRIBUTE_KEY} as the message attribute key.
     *
     * @param tracer The vendor-specific tracer implementation to use.
     */
    public SQSTracing(final Tracer tracer) {
        this(tracer, SPAN_CONTEXT_ATTRIBUTE_KEY);
    }

    /**
     * Initializes this utility for a given tracer implementation.
     * This is a lightweight operation, so you don't have to keep the instance around everywhere.
     *
     * @param tracer       The vendor-specific tracer implementation to use.
     * @param attributeKey The name to use for the message attribute key to use for storing the span context.
     *                     If you customise this, make sure to keep it consistent across projects.
     */
    public SQSTracing(final Tracer tracer, final String attributeKey) {
        this.tracer = tracer;
        this.attributeKey = attributeKey;
    }

    /**
     * Injects the currently active span context into a single-message SQS send request (AWS SDK 1.0).
     * Does not do anything if there is no currently active span.
     * <p>
     * See the class description for more details.
     * <p>
     * Attention: This modifies the original request (and only returns it for convenience),
     * as the AWS SDK does not provide easy request copying.
     *
     * @param request The original request to injectInto the span context into. This request will get modified by
     *                this method.
     * @return The original request, with the span context injected into it.
     */
    public com.amazonaws.services.sqs.model.SendMessageRequest injectInto(
        final com.amazonaws.services.sqs.model.SendMessageRequest request) {

        return injectInto(request, activeContext().orElse(null));
    }

    /**
     * Injects the given span context into a single-message SQS send request (AWS SDK 1.0).
     * Does not do anything if the given span context is null.
     * <p>
     * See the class description for more details.
     * <p>
     * Attention: This modifies the original request (and only returns it for convenience),
     * as the AWS SDK does not provide easy request copying.
     *
     * @param request     The original request to injectInto the span context into. This request will get modified by
     *                    this method.
     * @param spanContext The span context to injectInto. If null, this method will do nothing.
     * @return The original request, with the span context injected into it.
     */
    public com.amazonaws.services.sqs.model.SendMessageRequest injectInto(
        final com.amazonaws.services.sqs.model.SendMessageRequest request, final SpanContext spanContext) {

        if (spanContext != null) {
            final Map<String, String> contextMap = new HashMap<>();
            tracer.inject(spanContext, TEXT_MAP, new TextMapInjectAdapter(contextMap));
            request.addMessageAttributesEntry(
                attributeKey,
                new com.amazonaws.services.sqs.model.MessageAttributeValue()
                    .withDataType(STRING_DATA_TYPE).withStringValue(jsonEncode(contextMap)));
        }

        return request;
    }

    /**
     * Injects the given span context into the entry of a batch of messages to send over SQS (AWS SDK 1.0).
     * Does not do anything if the given span context is null.
     * <p>
     * See the class description for more details.
     * <p>
     * Attention: This modifies the original request entry (and only returns it for convenience),
     * as the AWS SDK does not provide easy request entry copying.
     * <p>
     * This lets you injectInto different span contexts into each individual message entry for a whole batch of
     * messages. If you want to injectInto the same span context into all the message entries of your batch,
     * you can use the {@link #injectInto(com.amazonaws.services.sqs.model.SendMessageBatchRequest)} method instead.
     *
     * @param request     The original request entry to injectInto the span context into. This entry will get modified
     *                    by this method.
     * @param spanContext The span context to injectInto. If null, this method will do nothing.
     * @return The original request entry, with the span context injected into it.
     */
    public com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry injectInto(
        final com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry request,
        final SpanContext spanContext) {

        if (spanContext != null) {
            final Map<String, String> contextMap = new HashMap<>();
            tracer.inject(spanContext, TEXT_MAP, new TextMapInjectAdapter(contextMap));
            request.addMessageAttributesEntry(
                attributeKey,
                new com.amazonaws.services.sqs.model.MessageAttributeValue()
                    .withDataType(STRING_DATA_TYPE).withStringValue(jsonEncode(contextMap)));
        }

        return request;
    }

    /**
     * Injects the currently active span context into the whole batch of messages to send over SQS (AWS SDK 1.0).
     * Does not do anything if there is no currently active span.
     * <p>
     * See the class description for more details.
     * <p>
     * Attention: This modifies the original request entry (and only returns it for convenience),
     * as the AWS SDK does not provide easy request entry copying.
     * <p>
     * This lets you injectInto the same span context into all messages for a whole batch of messages.
     * If you want to injectInto different span contexts for each individual message of your batch, you can use the
     * {@link #injectInto(com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry, SpanContext)} method instead.
     *
     * @param request The original request entry to injectInto the span context into. This entry will get modified
     *                by this method.
     * @return The original request entry, with the span context injected into it.
     */
    public com.amazonaws.services.sqs.model.SendMessageBatchRequest injectInto(
        final com.amazonaws.services.sqs.model.SendMessageBatchRequest request) {

        return injectInto(request, activeContext().orElse(null));
    }

    /**
     * Injects the given span context into the whole batch of messages to send over SQS (AWS SDK 1.0).
     * Does not do anything if the given span context is null.
     * <p>
     * See the class description for more details.
     * <p>
     * Attention: This modifies the original request entry (and only returns it for convenience),
     * as the AWS SDK does not provide easy request entry copying.
     * <p>
     * This lets you injectInto the same span context into all messages for a whole batch of messages.
     * If you want to injectInto different span contexts for each individual message of your batch, you can use the
     * {@link #injectInto(com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry, SpanContext)} method instead.
     *
     * @param request     The original request entry to injectInto the span context into. This entry will get modified
     *                    by this method.
     * @param spanContext The span context to injectInto. If null, this method will do nothing.
     * @return The original request entry, with the span context injected into it.
     */
    public com.amazonaws.services.sqs.model.SendMessageBatchRequest injectInto(
        final com.amazonaws.services.sqs.model.SendMessageBatchRequest request, final SpanContext spanContext) {

        if (spanContext != null) {
            final Map<String, String> contextMap = new HashMap<>();
            tracer.inject(spanContext, TEXT_MAP, new TextMapInjectAdapter(contextMap));
            request.getEntries().forEach(entry ->
                entry.addMessageAttributesEntry(
                    attributeKey,
                    new com.amazonaws.services.sqs.model.MessageAttributeValue()
                        .withDataType(STRING_DATA_TYPE).withStringValue(jsonEncode(contextMap)))
            );
        }

        return request;
    }

    // AWS SDK 1.0 API above, AWS SDK 2.0 API below

    /**
     * Injects the currently active span context into a single-message SQS send request (AWS SDK 2.0).
     * Returns the given request unchanged if there is no currently active span.
     * <p>
     * See the class description for more details.
     * <p>
     * In the spirit of AWS SDK 2.0, this does not modify the original request,
     * but instead returns a new request that is derived from the original one, with the added span context attribute
     * injected.
     *
     * @param request The original request to injectInto the span context into.
     * @return A request that is the same as the original one, but with the span context injected into it.
     */
    public SendMessageRequest injectInto(final SendMessageRequest request) {
        return injectInto(request, activeContext().orElse(null));
    }

    /**
     * Injects the given span context into a single-message SQS send request (AWS SDK 2.0).
     * Returns the given request unchanged if the given span context is null.
     * <p>
     * See the class description for more details.
     * <p>
     * In the spirit of AWS SDK 2.0, this does not modify the original request,
     * but instead returns a new request that is derived from the original one, with the added span context attribute
     * injected.
     *
     * @param request     The original request to injectInto the span context into.
     * @param spanContext The span context to injectInto. If null, this method will do nothing.
     * @return A request that is the same as the original one, but with the span context injected into it.
     */
    public SendMessageRequest injectInto(final SendMessageRequest request, final SpanContext spanContext) {
        if (spanContext == null) {
            return request;
        }

        return request.toBuilder()
            .messageAttributes(tracedAttributes(request.messageAttributes(), spanContext))
            .build();
    }

    /**
     * Injects the given span context into the entry of a batch of messages to send over SQS (AWS SDK 2.0).
     * Returns the given request unchanged if the given span context is null.
     * <p>
     * See the class description for more details.
     * <p>
     * In the spirit of AWS SDK 2.0, this does not modify the original entry,
     * but instead returns a new entry that is derived from the original one, with the added span context attribute
     * injected.
     * <p>
     * This lets you injectInto different span contexts into each individual message entry for a whole batch
     * of messages. If you want to injectInto the same span context into all the message entries of your batch,
     * you can use the {@link #injectInto(SendMessageBatchRequest)} method instead.
     *
     * @param request     The original entry to injectInto the span context into.
     * @param spanContext The span context to injectInto. If null, this method will do nothing.
     * @return An entry that is the same as the original one, but with the span context injected into it.
     */
    public SendMessageBatchRequestEntry injectInto(
        final SendMessageBatchRequestEntry request, final SpanContext spanContext) {

        if (spanContext == null) {
            return request;
        }

        return request.toBuilder()
            .messageAttributes(tracedAttributes(request.messageAttributes(), spanContext))
            .build();
    }

    /**
     * Injects the currently active span context into a whole batch of messages to send over SQS (AWS SDK 2.0).
     * Returns the given request unchanged if there is no currently active span.
     * <p>
     * See the class description for more details.
     * <p>
     * In the spirit of AWS SDK 2.0, this does not modify the original request,
     * but instead returns a new request that is derived from the original one, with the added span context attribute
     * injected.
     * <p>
     * This lets you injectInto the same span context into all messages for a whole batch of messages.
     * If you want to injectInto different span contexts for each individual message of your batch, you can use the
     * {@link #injectInto(SendMessageBatchRequestEntry, SpanContext)} method instead.
     *
     * @param request The original request to injectInto the span context into.
     * @return A request that is the same as the original one, but with the span context injected into it.
     */
    public SendMessageBatchRequest injectInto(final SendMessageBatchRequest request) {
        return injectInto(request, activeContext().orElse(null));
    }

    /**
     * Injects the given span context into a whole batch of messages to send over SQS (AWS SDK 2.0).
     * Returns the given request unchanged if the given span context is null.
     * <p>
     * See the class description for more details.
     * <p>
     * In the spirit of AWS SDK 2.0, this does not modify the original request,
     * but instead returns a new request that is derived from the original one, with the added span context attribute
     * injected.
     * <p>
     * This lets you injectInto the same span context into all messages for a whole batch of messages.
     * If you want to injectInto different span contexts for each individual message of your batch, you can use the
     * {@link #injectInto(SendMessageBatchRequestEntry, SpanContext)} method instead.
     *
     * @param request     The original request to injectInto the span context into.
     * @param spanContext The span context to injectInto. If null, this method will just return the given request.
     * @return A request that is the same as the original one, but with the span context injected into it.
     */
    public SendMessageBatchRequest injectInto(final SendMessageBatchRequest request, final SpanContext spanContext) {
        if (spanContext == null) {
            return request;
        }

        final List<SendMessageBatchRequestEntry> tracedEntries = request.entries().stream()
            .map(entry -> injectInto(entry, spanContext))
            .collect(toList());

        return request.toBuilder().entries(tracedEntries).build();
    }

    /**
     * Extracts all span contexts from the given batch of retrieved SQS messages (AWS SDK 1.0).
     *
     * @param result The received batch of SQS messages.
     * @return A map of message ids span contexts. For each message id, if a span context was found for the respective
     * message, then that span context is contained in this map, and referenced by the message id.
     * Message ids for messages without span context are not contained in the map.
     */
    public Map<String, SpanContext> extractFrom(final ReceiveMessageResult result) {
        return flattenMap(result.getMessages().stream().collect(toMap(
            com.amazonaws.services.sqs.model.Message::getMessageId,
            msg -> Optional.ofNullable(msg.getMessageAttributes().get(attributeKey))
                .flatMap(json -> jsonDecode(json.getStringValue()))
                .flatMap(this::safeExtract)
        )));
    }

    /**
     * Extracts all span contexts from the given batch of retrieved SQS messages (AWS SDK 2.0).
     *
     * @param response The received batch of SQS messages.
     * @return A map of message ids span contexts. For each message id, if a span context was found for the respective
     * message, then that span context is contained in this map, and referenced by the message id.
     * Message ids for messages without span context are not contained in the map.
     */
    public Map<String, SpanContext> extractFrom(final ReceiveMessageResponse response) {
        return flattenMap(response.messages().stream().collect(toMap(
            Message::messageId,
            msg -> Optional.ofNullable(msg.messageAttributes().get(attributeKey))
                .flatMap(json -> jsonDecode(json.stringValue()))
                .flatMap(this::safeExtract)
        )));
    }

    /**
     * Create and activate an OpenTracing span for a message about to be published into a queue.
     * <p>
     * This little helper will take care of setting the span kind tag to "producer",
     * and the message bus destination tag to the queue url.
     * <p>
     * If there is an active span when the method gets called,
     * it will also get added to the newly created span as a "follows from" reference.
     *
     * @param queueUrl The queue url this span should refer to.
     * @param spanName The name the span should have.
     * @return The created and activated span.
     */
    public Span createActiveQueueingSpan(final String queueUrl, final String spanName) {
        return createQueueingSpan(queueUrl, spanName, true);
    }

    /**
     * Create an OpenTracing span for a message about to be published into a queue.
     * <p>
     * This little helper will take care of setting the span kind tag to "producer",
     * and the message bus destination tag to the queue url.
     * <p>
     * If there is an active span when the method gets called,
     * it will also get added to the newly created span as a "follows from" reference.
     *
     * @param queueUrl The queue url this span should refer to.
     * @param spanName The name the span should have.
     * @param activate Set to true if you want to activate the span, or to false if you want to leave it inactive.
     * @return The created span.
     */
    public Span createQueueingSpan(final String queueUrl, final String spanName, final boolean activate) {
        final Tracer.SpanBuilder builder = tracer.buildSpan(spanName)
            .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_PRODUCER)
            .withTag(Tags.MESSAGE_BUS_DESTINATION.getKey(), queueUrl);
        Optional.ofNullable(tracer.activeSpan()).ifPresent(span -> builder.addReference(FOLLOWS_FROM, span.context()));

        final Span span = builder.start();

        if (activate) {
            // it does not make sense to finish the span on close,
            // because it will get transferred to another system via the queue for finishing.
            tracer.scopeManager().activate(span, false);
        }

        return span;
    }

    /**
     * Private helper for safely extracting a span context from a text map.
     * <p>
     * Unfortunately, the OpenTracing API decided to throw a runtime exception in case of a failure to parse,
     * which is very error-prone.
     * This API silently suppresses unparseable span contexts instead for better stability.
     *
     * @param contextMap A text map containing a previously serialized span context.
     * @return The span contexted extracted from the given map, or an empty value if the extraction has failed.
     */
    private Optional<SpanContext> safeExtract(final Map<String, String> contextMap) {
        try {
            return Optional.ofNullable(tracer.extract(TEXT_MAP, new TextMapExtractAdapter(contextMap)));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    private Optional<SpanContext> activeContext() {
        return Optional.ofNullable(tracer.activeSpan()).map(Span::context);
    }

    /**
     * Private helper for turning a map where some entries have optional values
     * into a map which does not contain these values at all.
     *
     * @param mapWithOptionals A map with optional values inside.
     * @param <T>              The type of the values in the map.
     * @return A map without any optional values inside, with all keys deleted that had empty values in the
     * original map.
     */
    private static <T> Map<String, T> flattenMap(final Map<String, Optional<T>> mapWithOptionals) {
        final Map<String, T> flattened = new HashMap<>();
        mapWithOptionals.forEach((k, v) -> v.ifPresent(t -> flattened.put(k, t)));
        return flattened;
    }

    /**
     * Private helper that injects the given span context into the given map of message attributes (AWS SDK 2.0).
     *
     * @param attributes  The message attributes to add the span context to.
     * @param spanContext The span contest to injectInto.
     * @return The modified map of message attributes, now including the injected span context
     * in addition to all message attributes that were already there.
     */
    private Map<String, MessageAttributeValue> tracedAttributes(
        final Map<String, MessageAttributeValue> attributes, final SpanContext spanContext) {

        final Map<String, MessageAttributeValue> tracedAttributes = new HashMap<>(attributes);
        final Map<String, String> contextMap = new HashMap<>();
        tracer.inject(spanContext, TEXT_MAP, new TextMapInjectAdapter(contextMap));
        tracedAttributes.put(attributeKey,
            MessageAttributeValue.builder().dataType(STRING_DATA_TYPE).stringValue(jsonEncode(contextMap)).build());

        return tracedAttributes;
    }

    /**
     * Private helper for decoding a text map from a json string.
     * <p>
     * As the json encoding is completely under control of this library, exceptions during json parsing will
     * simply return an empty value.
     *
     * @param json The string containing the text map serialized to json.
     * @return A text map, or an empty value if parsing from json has failed.
     */
    private Optional<Map<String, String>> jsonDecode(final String json) {
        try {
            return Optional.of(MAPPER.readValue(json, new TypeReference<HashMap<String, String>>() {
            }));
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    /**
     * Private helper for encoding a text map into a json string.
     * <p>
     * As the json encoding ins completely under control of this library, exceptions during json processing will
     * simply lead to returning an empty json object.
     *
     * @param map The map to encode into json.
     * @return A string containing the json representation of the map (as a simple json object).
     */
    private String jsonEncode(final Map<String, String> map) {
        try {
            return MAPPER.writer().writeValueAsString(map);
        } catch (JsonProcessingException e) {
            return "{}";
        }
    }
}
