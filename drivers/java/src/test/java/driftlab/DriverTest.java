package driftlab;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The driver's tests. They run against kafka-clients' own MockProducer, so no Kafka is needed. The
 * driver still makes kafka-clients' real ProducerConfig for ready, so the tests also show that
 * kafka-clients accepts both sets of settings the driver makes.
 */
class DriverTest {
    /** The brokers the runner names: deploy/kafka.yaml's client listeners. */
    static final List<String> BROKERS =
            List.of("127.0.0.1:29092", "127.0.0.1:39092", "127.0.0.1:49092");

    /** ready for defaults: kafka-clients 4.4.0 as it ships. */
    static final String READY_DEFAULTS = """
            {"event":"ready","driver":"java","client":"org.apache.kafka:kafka-clients@4.4.0",\
            "settings":{"acks":"-1","delivery.timeout.ms":"120000","enable.idempotence":"true",\
            "max.in.flight.requests.per.connection":"5","retries":"2147483647"}}""";

    /** ready for acks-one: the leader's acknowledgement alone, and no idempotence. */
    static final String READY_ACKS_ONE = """
            {"event":"ready","driver":"java","client":"org.apache.kafka:kafka-clients@4.4.0",\
            "settings":{"acks":"1","delivery.timeout.ms":"120000","enable.idempotence":"false",\
            "max.in.flight.requests.per.connection":"5","retries":"2147483647"}}""";

    /** The runner's setup command, as it writes one, with these settings and options. */
    static String setup(String settings, String options) {
        return """
                {"cmd":"setup","protocol":1,"brokers":["127.0.0.1:29092","127.0.0.1:39092",\
                "127.0.0.1:49092"],"topic":"t","partitions":1,"settings":"%s","options":%s}\
                """.formatted(settings, options);
    }

    /** Turns single quotes into double ones, so a test's JSON reads without backslashes. */
    static String json(String s) {
        return s.replace('\'', '"');
    }

    /**
     * What one run of the driver left: the lines it wrote, the settings it made each producer
     * with, and the error it stopped with, or null if stdin ended first.
     */
    record Run(List<String> lines, List<Map<String, Object>> producers, DriverException error) {}

    /** Runs the driver on these commands, with a MockProducer for its producer. */
    static Run drive(String... commands) throws IOException {
        return driveWith(config -> new MockProducer<>(
                true, null, new StringSerializer(), new StringSerializer()), commands);
    }

    /**
     * Runs the driver on these commands, one per line, with stdin ending after the last, and
     * newProducer making its producer.
     */
    static Run driveWith(Function<Map<String, Object>, Producer<String, String>> newProducer,
            String... commands) throws IOException {
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        List<Map<String, Object>> producers = new ArrayList<>();
        Driver d = new Driver(new Events(stdout), Driver.client(), config -> {
            producers.add(config);
            return newProducer.apply(config);
        });
        byte[] stdin = String.join("\n", commands).getBytes(StandardCharsets.UTF_8);
        DriverException error = null;
        try {
            d.serve(new ByteArrayInputStream(stdin));
        } catch (DriverException e) {
            error = e;
        }
        List<String> lines = stdout.toString(StandardCharsets.UTF_8).lines().toList();
        return new Run(lines, producers, error);
    }

    @Test
    void readyWithDefaults() throws IOException {
        Run r = drive(setup("defaults", "{}"));
        assertNull(r.error());
        assertEquals(List.of(READY_DEFAULTS), r.lines());
        assertEquals(List.of(Map.of(
                "bootstrap.servers", BROKERS,
                "client.id", "driftlab-java",
                "key.serializer", StringSerializer.class,
                "value.serializer", StringSerializer.class)), r.producers());
    }

    @Test
    void readyWithAcksOne() throws IOException {
        Run r = drive(setup("acks-one", "{}"));
        assertNull(r.error());
        assertEquals(List.of(READY_ACKS_ONE), r.lines());
        assertEquals(List.of(Map.of(
                "acks", "1",
                "enable.idempotence", false,
                "bootstrap.servers", BROKERS,
                "client.id", "driftlab-java",
                "key.serializer", StringSerializer.class,
                "value.serializer", StringSerializer.class)), r.producers());
    }

    /**
     * Each run the driver must refuse: its name, its commands, what the error must say, and how
     * many lines the driver writes before it stops.
     */
    static Stream<Arguments> refusals() {
        String ok = setup("defaults", "{}");
        return Stream.of(
                Arguments.of("protocol 2",
                        List.of(ok.replace("\"protocol\":1", "\"protocol\":2")),
                        "command \"setup\": protocol 2, but this driver speaks 1", 0),
                Arguments.of("sarama's settings",
                        List.of(setup("idempotent", "{}")),
                        "command \"setup\": no settings named \"idempotent\": want defaults or acks-one", 0),
                Arguments.of("an option",
                        List.of(setup("defaults", json("{'produce_timeout':'2s'}"))),
                        "command \"setup\": option \"produce_timeout\": this driver takes no options", 0),
                Arguments.of("no brokers",
                        List.of(json("{'cmd':'setup','protocol':1,'brokers':[],'topic':'t',"
                                + "'partitions':1,'settings':'defaults','options':{}}")),
                        "command \"setup\": needs brokers, a topic and at least one partition", 0),
                Arguments.of("publish before setup",
                        List.of(json("{'cmd':'publish','value':1,'partition':0}")),
                        "command \"publish\": came before setup", 0),
                Arguments.of("setup twice",
                        List.of(ok, ok),
                        "command \"setup\": came twice", 1),
                Arguments.of("no such command",
                        List.of(ok, json("{'cmd':'flush'}")),
                        "command \"flush\": no such command", 1),
                Arguments.of("not JSON",
                        List.of("nope"),
                        "reading \"nope\": Unrecognized token 'nope'", 0));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("refusals")
    void refuses(String name, List<String> commands, String want, int written)
            throws IOException {
        Run r = drive(commands.toArray(String[]::new));
        assertNotNull(r.error(), name + ": no error");
        String got = r.error().getMessage();
        assertTrue(got.contains(want), name + ": got \"" + got + "\", want \"" + want + "\"");
        assertEquals(written, r.lines().size(), name + ": lines written before the error");
    }

    /**
     * A producer kafka-clients can't make stops setup, with kafka-clients' own words and their
     * cause, as KafkaProducer says them for a broker address with no port.
     */
    @Test
    void setupStopsOnKafkasError() throws IOException {
        Run r = driveWith(config -> {
            throw new KafkaException("Failed to construct kafka producer",
                    new ConfigException("Invalid url in bootstrap.servers: 127.0.0.1"));
        }, setup("defaults", "{}"));
        assertNotNull(r.error());
        assertEquals("command \"setup\": starting kafka-clients' producer: "
                + "Failed to construct kafka producer: "
                + "Invalid url in bootstrap.servers: 127.0.0.1",
                r.error().getMessage());
        assertEquals(List.of(), r.lines());
    }

    @Test
    void stdinEnds() throws IOException {
        Run r = drive();
        assertNull(r.error());
        assertEquals(List.of(), r.lines());
        assertEquals(List.of(), r.producers());
    }
}
