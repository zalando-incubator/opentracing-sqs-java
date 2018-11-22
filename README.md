# opentracing-sqs-java

[![Build Status](https://travis-ci.org/zalando-incubator/opentracing-sqs-java.svg?branch=master)](https://travis-ci.org/zalando-incubator/opentracing-sqs-java)
[![codecov.io](https://codecov.io/github/zalando-incubator/opentracing-sqs-java/coverage.svg?branch=master)](https://codecov.io/github/zalando-incubator/opentracing-sqs-java?branch=master)

A little Java utility library for simplifying instrumentation of [SQS](https://aws.amazon.com/sqs) messages 
with [OpenTracing](http://opentracing.io/).
 
## Features

* Serialization and deserialization of the span context as a SQS message attribute
* Helper to easily create a span for tracing a message across SQS
* Works with both AWS SDK 1.0 and AWS SDK 2.0

## Dependencies

* Java 8
* AWS SDK 1.0 or AWS SDK 2.0
* OpenTracing

## Installation

*To Do:* The library is not published anywhere yet. This will be done asap.

For now, please clone and publish locally via:
```
./gradlew publishToMavenLocal
```

Please also remember to explicitly depend on either AWS SDK 1.0 or 2.0 with your project, as these dependencies 
are not included transitively with this library.
 
## Usage

### Injecting the span context into a send request
```java
SQSTracing tracing = new SQSTracing(tracer);

// works similarly for SendMessageBatchRequestEntry and SendMessageBatchRequest,
// as well as with the respective equivalent builders in AWS SDK 1.0
SendMessageRequest request = SendMessageRequest.builder()
    .messageBody("body")
    .queueUrl("queue-url")
    .build();

SendMessageRequest tracedRequest = tracing.injectInto(request); // injects the currently active span context
```

### Extracting the span contexts from a received batch of messages

```java
SQSTracing tracing = new SQSTracing(tracer);

ReceiveMessageResponse response = ... // retrieved from SQS 

// the following works the same for ReceiveMessageResult from AWS SDK 1.0

Map<String, SpanContext> spanContexts = tracing.extractFrom(response);

// the map now contains all message ids of messages which had span contexts associated, 
// mapped to the respective span contexts
```

### Creating and activating a span for publishing a message to SQS

```java
SQSTracing tracing = new SQSTracing(tracer);

Span span = tracing.createActiveQueueingSpan(
    "https://sqs.us-east-2.amazonaws.com/123456789012/MyQueue", 
    "publish-update-event");

// span is created and active now, you can continue by injecting it into your request(s), for example:
SendMessageBatchRequestEntry entry = ...
SendMessageBatchRequestEntry tracedEntry = tracing.injectInto(entry, span); 
```

## Contributing

This project happily accepts contributions from the open-source community.

Please read [CONTRIBUTING.md](CONTRIBUTING.md).

Please also note our [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

## Contact

These [email addresses](MAINTAINERS) serve as the main contact addresses for this project.

Bug reports and feature requests are more likely to be addressed
if posted as [issues](../../issues) here on GitHub.
  
## License

This project is licensed under the terms of the MIT license, see also the full [LICENSE](LICENSE).

The MIT License (MIT)

Copyright (c) 2018 Zalando SE

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
