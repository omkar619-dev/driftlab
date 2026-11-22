package driftlab;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.function.Function;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * driftlab's Java driver: Kafka's own Java client, kafka-clients, and nothing more (DESIGN.md,
 * Driver split). The runner, bin/driftlab-run, starts it as a child process and does everything
 * else, from creating the topic to the faults and the final read. The runner writes commands on
 * the driver's stdin, and the driver answers them on its stdout, one JSON object per line. An
 * error that stops the driver goes to stderr, never stdout, and so does kafka-clients' own log.
 */
public final class Driver {
    /**
     * The protocol this driver speaks. setup carries the runner's number, and a driver refuses a
     * number it doesn't know (DESIGN.md, What stays).
     */
    static final int PROTOCOL = 1;

    /** The name the driver goes by in kafka-clients' log, and in the brokers'. */
    static final String CLIENT_ID = "driftlab-java";

    /**
     * Reads the runner's commands. A field that no command has is skipped, as encoding/json skips
     * it in the Go drivers: a field that changes what a driver must do comes with a new protocol
     * number, which setup refuses.
     */
    private static final ObjectMapper JSON = JsonMapper.builder()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    private final Events out; // where events go: stdout, or a buffer in the tests
    private final String client; // the kafka-clients the driver loaded, for ready

    /**
     * Makes kafka-clients' producer from its settings: KafkaProducer's own constructor, or one that
     * hands back kafka-clients' MockProducer in the tests.
     */
    private final Function<Map<String, Object>, Producer<String, String>> newProducer;

    private boolean ready; // setup is done, and ready written
    private Producer<String, String> producer; // kafka-clients' producer, which setup made

    Driver(Events out, String client,
            Function<Map<String, Object>, Producer<String, String>> newProducer) {
        this.out = out;
        this.client = client;
        this.newProducer = newProducer;
    }

    /**
     * Runs the driver on the process's stdin and stdout. The protocol's lines go to stdout through
     * a stream of their own, and System.out is pointed at stderr, so nothing a library prints can
     * land among them (rule 2). The driver exits with status 0 once stdin ends (rule 8), and with
     * status 1, after saying why on stderr, at a line it can't carry out.
     */
    public static void main(String[] args) {
        OutputStream stdout = new BufferedOutputStream(new FileOutputStream(FileDescriptor.out));
        System.setOut(System.err);
        Driver d = new Driver(new Events(stdout), client(), KafkaProducer::new);
        try {
            d.serve(System.in);
        } catch (DriverException e) {
            System.err.println("driftlab-java: " + e.getMessage());
            System.exit(1);
        } catch (IOException e) {
            System.err.println("driftlab-java: " + e);
            System.exit(1);
        }
        // Exit now, with status 0, whatever threads kafka-clients still runs.
        System.exit(0);
    }

    /**
     * Reads the runner's commands, one per line, and carries out each in turn. It returns once
     * stdin ends, as it does when the runner closes it or dies (rule 8). At a line it can't carry
     * out, it throws, saying which line or command it was and why.
     */
    void serve(InputStream in) throws DriverException, IOException {
        BufferedReader lines =
                new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        for (String line = lines.readLine(); line != null; line = lines.readLine()) {
            Command c;
            try {
                c = JSON.readValue(line, Command.class);
            } catch (JsonProcessingException e) {
                throw new DriverException(
                        "reading \"" + line + "\": " + e.getOriginalMessage(), e);
            }
            if (c == null) { // the line was JSON's null
                throw new DriverException("reading \"" + line + "\": not a command");
            }
            try {
                run(c);
            } catch (DriverException e) {
                throw new DriverException("command \"" + c.cmd() + "\": " + e.getMessage(), e);
            }
        }
    }

    /** Carries out one command. setup comes first, and only once (rule 8). */
    private void run(Command c) throws DriverException, IOException {
        boolean isSetup = c.cmd().equals("setup");
        if (isSetup && ready) {
            throw new DriverException("came twice");
        }
        if (!isSetup && !ready) {
            throw new DriverException("came before setup");
        }
        switch (c.cmd()) {
            case "setup" -> setup(c);
            default -> throw new DriverException("no such command");
        }
    }

    /**
     * Makes kafka-clients' producer, with the named set of settings, and answers ready: the
     * driver's name, the kafka-clients it loaded, and the value in force of each setting that
     * decides what a crash can cost. Making the producer waits for no broker: kafka-clients looks
     * up the topic only on the first send, so setup answers ready even when no broker is up. It
     * does look up the brokers' names, so a name that doesn't resolve stops setup. Nothing is
     * written before ready, so it is the first line (rule 8).
     */
    private void setup(Command c) throws DriverException, IOException {
        if (c.protocol() != PROTOCOL) {
            throw new DriverException(
                    "protocol " + c.protocol() + ", but this driver speaks " + PROTOCOL);
        }
        if (c.brokers().isEmpty() || c.topic().isEmpty() || c.partitions() < 1) {
            throw new DriverException("needs brokers, a topic and at least one partition");
        }
        Map<String, Object> config = producerConfig(c.brokers(), c.settings(), c.options());
        Map<String, String> settings = inForce(config);
        try {
            producer = newProducer.apply(config);
        } catch (KafkaException e) {
            throw new DriverException("starting kafka-clients' producer: " + causes(e), e);
        }
        ready = true;
        ObjectNode line = out.event("ready").put("driver", "java").put("client", client);
        ObjectNode shown = line.putObject("settings");
        for (Map.Entry<String, String> s : settings.entrySet()) {
            shown.put(s.getKey(), s.getValue());
        }
        out.write(line);
    }

    /**
     * kafka-clients' producer settings for a named set, as KafkaProducer takes them.
     *
     * <p>defaults is kafka-clients as it ships: acknowledgements from every in-sync copy
     * (acks=all) with idempotence on, up to 5 requests in flight per connection, and retries until
     * delivery.timeout.ms, two minutes, has passed. Its docs promise that with idempotence on,
     * "the producer will ensure that exactly one copy of each message is written in the stream"
     * (enable.idempotence), and the checker holds it to that.
     *
     * <p>acks-one asks for the leader's acknowledgement alone, for the settings check (DESIGN.md,
     * Java and Python). Kafka's docs for acks=1 say that "should the leader fail immediately after
     * acknowledging the record but before the followers have replicated it then the record will
     * be lost". kafka-clients turns idempotence off by itself when acks isn't all, but only when
     * nothing set it, and says so only in its log; acks-one sets it off, so the change is here, in
     * the code.
     *
     * <p>A scenario's options are refused (rule 6). kafka-clients sends request.timeout.ms as the
     * produce request's timeout, and also waits that long for each answer, so it can't change
     * produce_timeout alone; and it sizes batches in bytes, so it has no max_batch_messages. None
     * of the five table faults sets either. The first option in order is the one named, so the
     * same options are always refused the same way.
     *
     * <p>Four more settings serve the driver, whatever the name: the brokers, the client.id that
     * names the driver in the logs, and the two serializers, which turn the value's decimal text
     * into bytes. A record never has a key (rule 4), but kafka-clients wants a key serializer all
     * the same.
     */
    static Map<String, Object> producerConfig(
            List<String> brokers, String settings, Map<String, String> options)
            throws DriverException {
        Map<String, Object> config = new HashMap<>();
        switch (settings) {
            case "defaults" -> {
                // kafka-clients as it ships: nothing to change
            }
            case "acks-one" -> {
                config.put(ProducerConfig.ACKS_CONFIG, "1");
                config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
            }
            default -> throw new DriverException(
                    "no settings named \"" + settings + "\": want defaults or acks-one");
        }
        if (!options.isEmpty()) {
            String first = new TreeMap<>(options).firstKey();
            throw new DriverException("option \"" + first + "\": this driver takes no options");
        }
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return config;
    }

    /**
     * The value in force, under kafka-clients' own names, of each setting that decides what a
     * crash can cost: the two that acks-one changes, and three that no scenario changes, which say
     * how many requests may be in flight and how long a value is retried before it is given up
     * on. The values come from kafka-clients' own ProducerConfig, made from the same settings
     * KafkaProducer gets, so they are what kafka-clients made of them: it holds acks=all as -1,
     * the same thing in Kafka's docs. ProducerConfig also checks the settings, and refuses a set
     * that contradicts itself, as KafkaProducer would. Making it logs every setting to stderr,
     * which KafkaProducer then does again.
     */
    static Map<String, String> inForce(Map<String, Object> config) throws DriverException {
        ProducerConfig c;
        try {
            c = new ProducerConfig(config);
        } catch (ConfigException e) {
            throw new DriverException("kafka-clients refused the settings: " + e.getMessage(), e);
        }
        Map<String, String> s = new TreeMap<>(); // in order by name, as Go writes a map
        s.put(ProducerConfig.ACKS_CONFIG, c.getString(ProducerConfig.ACKS_CONFIG));
        s.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                String.valueOf(c.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)));
        s.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                String.valueOf(c.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)));
        s.put(ProducerConfig.RETRIES_CONFIG,
                String.valueOf(c.getInt(ProducerConfig.RETRIES_CONFIG)));
        s.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
                String.valueOf(c.getInt(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)));
        return s;
    }

    /**
     * Which kafka-clients the driver loaded: the version its jar records in
     * kafka/kafka-version.properties, the file kafka-clients itself reads to report its version.
     * A jar that records none says unknown.
     */
    static String client() {
        Properties p = new Properties();
        try (InputStream in =
                KafkaProducer.class.getResourceAsStream("/kafka/kafka-version.properties")) {
            if (in != null) {
                p.load(in);
            }
        } catch (IOException e) {
            // A file that can't be read leaves the version unknown, as a missing one does.
        }
        String version = p.getProperty("version", "unknown").trim();
        return "org.apache.kafka:kafka-clients@" + version;
    }

    /** An exception's message, then each of its causes' in turn, the way Go chains errors. */
    static String causes(Throwable e) {
        StringBuilder b = new StringBuilder(String.valueOf(e.getMessage()));
        for (Throwable c = e.getCause(); c != null; c = c.getCause()) {
            b.append(": ").append(c.getMessage());
        }
        return b.toString();
    }

    /**
     * One of the runner's commands, with the fields any of them can carry. Each command fills in
     * its own, and Jackson leaves the rest at null or 0. The compact constructor below, which runs
     * before the fields are set, turns those nulls into empty values, so no code meets a null.
     */
    record Command(String cmd, int protocol, List<String> brokers, String topic, int partitions,
            String settings, Map<String, String> options, long value, int partition) {
        Command {
            cmd = cmd == null ? "" : cmd;
            brokers = brokers == null ? List.of() : brokers;
            topic = topic == null ? "" : topic;
            settings = settings == null ? "" : settings;
            options = options == null ? Map.of() : options;
        }
    }
}
