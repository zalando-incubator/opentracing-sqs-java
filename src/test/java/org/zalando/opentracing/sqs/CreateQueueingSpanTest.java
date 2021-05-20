package org.zalando.opentracing.sqs;

import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.tag.Tags;
import io.opentracing.util.ThreadLocalScopeManager;
import org.junit.Test;

import static io.opentracing.References.FOLLOWS_FROM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CreateQueueingSpanTest {
    private final MockTracer mockTracer = new MockTracer(new ThreadLocalScopeManager(), MockTracer.Propagator.TEXT_MAP);

    @Test
    public void spanMustBeCreatedCorrectlyWithParentSpan() {
        mockTracer.activateSpan(mockTracer.buildSpan("meep").start()
                .setBaggageItem("baggage-key", "baggage-value"));

        final MockSpan parent = (MockSpan) mockTracer.activeSpan();
        final MockSpan span = (MockSpan) new SQSTracing(mockTracer)
            .createActiveQueueingSpan("meep", "bloop");

        assertEquals("span must be named correctly", "bloop", span.operationName());
        assertSame("span must be set to currently active span", span, mockTracer.activeSpan());
        assertEquals("span must have correct queue url", "meep",
            span.tags().get(Tags.MESSAGE_BUS_DESTINATION.getKey()));
        assertEquals("span must be of the producer kind", Tags.SPAN_KIND_PRODUCER,
            span.tags().get(Tags.SPAN_KIND.getKey()));

        assertEquals("created span must have exactly one reference", 1, span.references().size());
        final MockSpan.Reference ref = span.references().get(0);

        assertEquals("span reference must be of type \"follows from \"", FOLLOWS_FROM, ref.getReferenceType());
        assertEquals("span parent must be correct", parent.context().spanId(), span.parentId());
    }

    @Test
    public void spanMustBeCreatedCorrectlyWithoutParentSpan() {
        final MockSpan span = (MockSpan) new SQSTracing(mockTracer)
            .createActiveQueueingSpan("meep", "bloop");

        assertSame("span must be set to currently active span", span, mockTracer.activeSpan());
        assertTrue("created span must not have any references", span.references().isEmpty());
    }

    @Test
    public void spanMustBeCreatedAsInactiveWhenRequested() {
        final MockSpan span = (MockSpan) new SQSTracing(mockTracer)
            .createQueueingSpan("meep", "bloop", false);
        assertNotSame("span must be set to currently active span", span, mockTracer.activeSpan());
    }

    @Test
    public void spanMustBeCreatedAsActiveWhenRequested() {
        final MockSpan span = (MockSpan) new SQSTracing(mockTracer)
            .createQueueingSpan("meep", "bloop", true);
        assertSame("span must be set to currently active span", span, mockTracer.activeSpan());
    }
}
