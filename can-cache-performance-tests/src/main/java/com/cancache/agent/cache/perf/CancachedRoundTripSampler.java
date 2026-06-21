package com.cancache.agent.cache.perf;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CancachedRoundTripSampler extends AbstractJavaSamplerClient
{

    private static final Logger LOG = LoggerFactory.getLogger(CancachedRoundTripSampler.class);

    private static final String PARAM_TARGET_HOST = "targetHost";
    private static final String PARAM_TARGET_PORT = "targetPort";
    private static final String PARAM_TTL_SECONDS = "ttlSeconds";
    private static final String PARAM_CONNECT_TIMEOUT = "connectTimeoutMillis";
    private static final String PARAM_READ_TIMEOUT = "readTimeoutMillis";
    private static final String PARAM_KEY_PREFIX = "keyPrefix";
    private static final String PARAM_PAYLOAD_SIZE = "payloadSize";
    private static final String PARAM_PAYLOAD_SIZES = "payloadSizes";
    private static final String PARAM_PAYLOAD_SELECTION = "payloadSelection";
    private static final String PARAM_CONNECTION_MODE = "connectionMode";

    private static final String PAYLOAD_SELECTION_CYCLE = "cycle";
    private static final String PAYLOAD_SELECTION_RANDOM = "random";
    private static final String CONNECTION_MODE_SINGLE = "single";
    private static final String CONNECTION_MODE_SEPARATE = "separate";

    private static final AtomicInteger PAYLOAD_COUNTER = new AtomicInteger();

    @Override
    public Arguments getDefaultParameters() {
        Arguments arguments = new Arguments();
        arguments.addArgument(PARAM_TARGET_HOST, "127.0.0.1");
        arguments.addArgument(PARAM_TARGET_PORT, "11211");
        arguments.addArgument(PARAM_TTL_SECONDS, "60");
        arguments.addArgument(PARAM_CONNECT_TIMEOUT, "1000");
        arguments.addArgument(PARAM_READ_TIMEOUT, "3000");
        arguments.addArgument(PARAM_KEY_PREFIX, "perf-");
        arguments.addArgument(PARAM_PAYLOAD_SIZE, "64");
        arguments.addArgument(PARAM_PAYLOAD_SIZES, "64,512,2048,8192");
        arguments.addArgument(PARAM_PAYLOAD_SELECTION, PAYLOAD_SELECTION_CYCLE);
        arguments.addArgument(PARAM_CONNECTION_MODE, CONNECTION_MODE_SINGLE);
        return arguments;
    }

    @Override
    public SampleResult runTest(JavaSamplerContext context) {
        SampleResult result = new SampleResult();
        result.setSampleLabel("cancached Round Trip");

        String host = context.getParameter(PARAM_TARGET_HOST, "127.0.0.1");
        int port = context.getIntParameter(PARAM_TARGET_PORT, 11211);
        int ttlSeconds = context.getIntParameter(PARAM_TTL_SECONDS, 60);
        int connectTimeout = context.getIntParameter(PARAM_CONNECT_TIMEOUT, 1000);
        int readTimeout = context.getIntParameter(PARAM_READ_TIMEOUT, 3000);
        int payloadSize = determinePayloadSize(context);
        String keyPrefix = context.getParameter(PARAM_KEY_PREFIX, "perf-");
        String connectionMode = context.getParameter(PARAM_CONNECTION_MODE, CONNECTION_MODE_SINGLE);

        result.sampleStart();
        try {
            String random = UUID.randomUUID().toString().replace("-", "");
            String payload = buildPayload(random, payloadSize);
            String keySuffix = random.isEmpty() ? "" : random.substring(0, Math.min(16, random.length()));
            String key = keyPrefix + keySuffix;
            RoundTripResponse response = CONNECTION_MODE_SEPARATE.equalsIgnoreCase(connectionMode)
                    ? runSeparateConnectionRoundTrip(host, port, connectTimeout, readTimeout, key, payload, ttlSeconds)
                    : runSingleConnectionRoundTrip(host, port, connectTimeout, readTimeout, key, payload, ttlSeconds);

            result.setSuccessful(true);
            result.setResponseCodeOK();
            result.setResponseMessage("Round trip succeeded");
            result.setResponseData(("SET:" + response.setResponse()
                    + ";GET:" + response.getHeader()
                    + ";DEL:" + response.deleteResponse()
                    + ";MODE:" + connectionMode).getBytes(StandardCharsets.UTF_8));
            result.setDataType(SampleResult.TEXT);
        } catch (Exception ex) {
            LOG.error("cancached round trip failed", ex);
            result.setSuccessful(false);
            result.setResponseCode("500");
            result.setResponseMessage(ex.getMessage());
            result.setResponseData(stackTrace(ex), StandardCharsets.UTF_8.name());
        } finally {
            result.sampleEnd();
        }

        return result;
    }

    private static RoundTripResponse runSingleConnectionRoundTrip(String host, int port, int connectTimeout, int readTimeout,
                                                                  String key, String payload, int ttlSeconds)
            throws IOException
    {
        return withConnection(host, port, connectTimeout, readTimeout, (writer, reader) -> {
            String setResponse = executeSet(writer, reader, key, payload, ttlSeconds);
            String getHeader = executeGet(writer, reader, key, payload);
            String deleteResponse = executeDelete(writer, reader, key);
            return new RoundTripResponse(setResponse, getHeader, deleteResponse);
        });
    }

    private static RoundTripResponse runSeparateConnectionRoundTrip(String host, int port, int connectTimeout, int readTimeout,
                                                                    String key, String payload, int ttlSeconds)
            throws IOException
    {
        String setResponse = withConnection(host, port, connectTimeout, readTimeout,
                (writer, reader) -> executeSet(writer, reader, key, payload, ttlSeconds));
        String getHeader = withConnection(host, port, connectTimeout, readTimeout,
                (writer, reader) -> executeGet(writer, reader, key, payload));
        String deleteResponse = withConnection(host, port, connectTimeout, readTimeout,
                (writer, reader) -> executeDelete(writer, reader, key));
        return new RoundTripResponse(setResponse, getHeader, deleteResponse);
    }

    private static <T> T withConnection(String host, int port, int connectTimeout, int readTimeout,
                                        SocketOperation<T> operation)
            throws IOException
    {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), connectTimeout);
            socket.setSoTimeout(readTimeout);
            socket.setTcpNoDelay(true);

            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
                 BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
                return operation.execute(writer, reader);
            }
        }
    }

    private static String executeSet(BufferedWriter writer, BufferedReader reader, String key, String payload, int ttlSeconds)
            throws IOException
    {
        byte[] payloadBytes = payload.getBytes(StandardCharsets.UTF_8);
        writeLine(writer, "set " + key + " 0 " + ttlSeconds + " " + payloadBytes.length);
        writer.write(payload);
        writer.write("\r\n");
        writer.flush();

        String setResponse = reader.readLine();
        if (!"STORED".equals(setResponse)) {
            throw new IOException("SET failed with response: " + setResponse);
        }
        return setResponse;
    }

    private static String executeGet(BufferedWriter writer, BufferedReader reader, String key, String payload)
            throws IOException
    {
        writeLine(writer, "get " + key);
        writer.flush();

        String header = reader.readLine();
        if (header == null || !header.startsWith("VALUE")) {
            throw new IOException("Unexpected GET header: " + header);
        }

        String returned = reader.readLine();
        String trailer = reader.readLine();

        if (!payload.equals(returned)) {
            int returnedLength = returned == null ? -1 : returned.length();
            throw new IOException("Returned payload mismatch (" + returnedLength + " vs expected " + payload.length() + ")");
        }

        if (!"END".equals(trailer)) {
            throw new IOException("Missing END after GET, received: " + trailer);
        }

        return header;
    }

    private static String executeDelete(BufferedWriter writer, BufferedReader reader, String key)
            throws IOException
    {
        writeLine(writer, "delete " + key);
        writer.flush();

        String deleteResponse = reader.readLine();
        if (deleteResponse == null || !("DELETED".equals(deleteResponse) || "NOT_FOUND".equals(deleteResponse))) {
            throw new IOException("DELETE failed with response: " + deleteResponse);
        }
        return deleteResponse;
    }

    private static int determinePayloadSize(JavaSamplerContext context) {
        List<Integer> parsedSizes = parsePayloadSizes(context.getParameter(PARAM_PAYLOAD_SIZES, ""));
        if (!parsedSizes.isEmpty()) {
            String selection = context.getParameter(PARAM_PAYLOAD_SELECTION, PAYLOAD_SELECTION_CYCLE);
            if (PAYLOAD_SELECTION_RANDOM.equalsIgnoreCase(selection)) {
                return parsedSizes.get(ThreadLocalRandom.current().nextInt(parsedSizes.size()));
            }

            int index = Math.floorMod(PAYLOAD_COUNTER.getAndIncrement(), parsedSizes.size());
            return parsedSizes.get(index);
        }

        return Math.max(0, context.getIntParameter(PARAM_PAYLOAD_SIZE, 64));
    }

    private static List<Integer> parsePayloadSizes(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return Collections.emptyList();
        }

        LinkedHashSet<Integer> sizes = Pattern.compile(",")
                .splitAsStream(raw)
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(CancachedRoundTripSampler::safeParseInt)
                .filter(size -> size > 0)
                .collect(LinkedHashSet::new, LinkedHashSet::add, LinkedHashSet::addAll);

        return new ArrayList<>(sizes);
    }

    private static String buildPayload(String source, int payloadSize) {
        if (payloadSize <= 0) {
            return "";
        }

        String safeSource = source == null || source.isEmpty() ? "x" : source;
        StringBuilder builder = new StringBuilder(payloadSize);
        while (builder.length() < payloadSize) {
            builder.append(safeSource);
        }
        return builder.substring(0, payloadSize);
    }

    private static int safeParseInt(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ex) {
            LOG.warn("Ignoring invalid payload size entry: {}", value, ex);
            return -1;
        }
    }

    private static void writeLine(BufferedWriter writer, String line) throws IOException {
        writer.write(line);
        writer.write("\r\n");
    }

    private static String stackTrace(Exception ex) {
        StringWriter sw = new StringWriter();
        try (PrintWriter pw = new PrintWriter(sw)) {
            ex.printStackTrace(pw);
        }
        return sw.toString();
    }

    @FunctionalInterface
    private interface SocketOperation<T>
    {
        T execute(BufferedWriter writer, BufferedReader reader) throws IOException;
    }

    private record RoundTripResponse(String setResponse, String getHeader, String deleteResponse)
    {
    }
}
