package org.szymie;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lsr.paxos.replica.Replica;
import org.apache.commons.cli.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.szymie.client.TransactionFactory;
import org.szymie.server.SerializableCertificationService;
import org.szymie.server.FrontActor;
import org.szymie.server.ResourceRepository;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

@SpringBootApplication
public class ReplicaMain implements CommandLineRunner {

    @Value("${id}")
    private int id;

    @Value("${address}")
    private String address;

    @Value("${port}")
    private int port;

    private SerializableCertificationService serializableCertificationService;

    public static void main(String[] args) throws ParseException {

        CommandLine commandLine = createCommandLine(args);

        if(commandLine.hasOption("B")) {
            runBenchmark(commandLine);
        } else {

            String[] arguments = Stream.of("id", "port", "address")
                    .map(argument -> String.format("--%s=%s", argument, commandLine.getOptionValue(argument)))
                    .toArray(String[]::new);

            SpringApplication application = new SpringApplication(ReplicaMain.class);
            application.run(arguments);
        }
    }

    private static CommandLine createCommandLine(String[] args) throws ParseException {
        Options options = createOptions();
        CommandLineParser parser = new DefaultParser();
        return parser.parse(options, args);
    }

    private static Options createOptions() {

        Options options = new Options();

        addToOptions(options, "B", "benchmark", false);
        addToOptions(options, null, "id", true);
        addToOptions(options, null, "port", true);
        addToOptions(options, null, "address", true);
        addToOptions(options, null, "keys", true);
        addToOptions(options, null, "threads", true);
        addToOptions(options, null, "readsInQuery", true);
        addToOptions(options, null, "readsInUpdate", true);
        addToOptions(options, null, "writesInUpdate", true);
        addToOptions(options, null, "delay", true);
        addToOptions(options, null, "saturation", true);

        return options;
    }

    private static void addToOptions(Options options, String shortOption, String longOption, boolean hasArg) {
        options.addOption(Option.builder(shortOption).longOpt(longOption).hasArg(hasArg).build());
    }

    private static void runBenchmark(CommandLine commandLine) {

        String address = commandLine.getOptionValue("address", "127.0.0.1");
        String port = commandLine.getOptionValue("port", "2550");
        int numberOfKeys = Integer.parseInt(commandLine.getOptionValue("keys"));
        int numberOfThreads = Integer.parseInt(commandLine.getOptionValue("threads"));
        int readsInQuery = Integer.parseInt(commandLine.getOptionValue("readsInQuery"));
        int readsInUpdate = Integer.parseInt(commandLine.getOptionValue("readsInUpdate"));
        int writesInUpdate = Integer.parseInt(commandLine.getOptionValue("writesInUpdate"));
        long delay = Long.parseLong(commandLine.getOptionValue("delay"));
        int saturation = Integer.parseInt(commandLine.getOptionValue("saturation"));

        Properties properties = new Properties();
        properties.setProperty("akka.remote.netty.tcp.hostname", address);
        properties.setProperty("akka.remote.netty.tcp.port", port);

        Config overrides = ConfigFactory.parseProperties(properties);
        Config config = overrides.withFallback(ConfigFactory.load());

        ActorSystem actorSystem = ActorSystem.create("client", config);
        TransactionFactory transactionFactory = new TransactionFactory(actorSystem);

        Benchmark benchmark = new Benchmark(transactionFactory, numberOfKeys, readsInQuery, readsInUpdate, writesInUpdate, delay);

        Benchmark.SaturationLevel saturationLevel = Benchmark.SaturationLevel.LOW;

        for(Benchmark.SaturationLevel level : Benchmark.SaturationLevel.values()) {
            if(saturation > level.value) {
                saturationLevel = level;
            }
        }

        benchmark.execute(saturationLevel, numberOfThreads);
    }

    @Override
    public void run(String... strings) throws Exception {
        Replica replica = new Replica(new lsr.common.Configuration("src/main/resources/paxos.properties"), id, serializableCertificationService);
        replica.start();
    }

    @Bean
    public ActorSystem actorSystem() {

        Properties properties = new Properties();
        properties.setProperty("akka.remote.netty.tcp.hostname", address);
        properties.setProperty("akka.remote.netty.tcp.port", String.valueOf(port));

        Config overrides = ConfigFactory.parseProperties(properties);
        Config config = overrides.withFallback(ConfigFactory.load());

        return ActorSystem.create(String.format("replica-%d", id), config);
    }

    @Bean
    public ResourceRepository resourceRepository() {
        return new ResourceRepository();
    }

    @Bean
    public ActorRef frontActor(ActorSystem actorSystem, ResourceRepository resourceRepository, AtomicLong timestamp) {
        return actorSystem.actorOf(Props.create(FrontActor.class, resourceRepository, timestamp), "front");
    }

    @Bean
    public AtomicLong timestamp() {
        return new AtomicLong(0);
    }

    @Autowired
    public void setSerializableCertificationService(SerializableCertificationService serializableCertificationService) {
        this.serializableCertificationService = serializableCertificationService;
    }

}
