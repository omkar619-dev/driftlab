package driftlab;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Writes the driver's events, one JSON object per line. Every line is written under one lock, so
 * lines from two threads never mix (rule 7), and flushed as soon as it is written, so none waits
 * in a buffer (rule 1).
 */
final class Events {
    private static final ObjectMapper JSON = JsonMapper.builder().build();

    private final OutputStream out; // stdout, or a buffer in the tests

    Events(OutputStream out) {
        this.out = out;
    }

    /**
     * Starts an event's line with the event's name. The caller adds the event's own fields, which
     * keep the order they are added in.
     */
    ObjectNode event(String name) {
        return JSON.createObjectNode().put("event", name);
    }

    /** Writes one event as one line, and flushes it. */
    synchronized void write(ObjectNode e) throws IOException {
        out.write(JSON.writeValueAsBytes(e));
        out.write('\n');
        out.flush();
    }
}
