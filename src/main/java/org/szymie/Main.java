package org.szymie;

import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lsr.paxos.replica.Replica;
import lsr.service.SerializableService;
import org.apache.commons.cli.*;
import org.apache.commons.lang.ArrayUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.messaging.simp.SimpMessageSendingOperations;
import org.szymie.client.strong.optimistic.TransactionFactory;
import org.szymie.server.strong.ChannelInboundHandlerFactory;
import org.szymie.server.strong.ReplicaServer;
import org.szymie.server.strong.ServerChannelInitializer;
import org.szymie.server.strong.optimistic.*;
import org.szymie.server.strong.pessimistic.BeginTransactionService;
import org.szymie.server.strong.pessimistic.GroupMessenger;
import org.szymie.server.strong.pessimistic.StateUpdateReceiver;
import org.szymie.server.strong.pessimistic.TransactionMetadata;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

@SpringBootApplication
public class Main implements CommandLineRunner {

    public static class ExtendedDefaultParser extends DefaultParser {

        @Override
        public CommandLine parse(Options options, String[] arguments) throws ParseException {

            String[] filteredArguments = Stream.of(arguments)
                    .filter(argument -> !argument.startsWith("--spring")).toArray(String[]::new);

            Arrays.asList(filteredArguments).forEach(System.out::println);

            return super.parse(options, filteredArguments);
        }
    }

    public static void main(String[] args) throws ParseException {

        CommandLine commandLine = createCommandLine(args);

        if(commandLine.hasOption("B")) {
            runBenchmark(commandLine);
        } else {

            String[] arguments = Stream.of("id", "port", "address")
                    .map(argument -> String.format("--%s=%s", argument, commandLine.getOptionValue(argument)))
                    .toArray(String[]::new);

            String[] allArguments = (String[]) ArrayUtils.addAll(args, arguments);

            SpringApplication application = new SpringApplication(Main.class);
            application.run(allArguments);
        }
    }

    private static CommandLine createCommandLine(String[] args) throws ParseException {
        Options options = createOptions();
        ExtendedDefaultParser parser = new ExtendedDefaultParser();
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
        TransactionFactory transactionFactory = new TransactionFactory();

        Benchmark benchmark = new Benchmark(transactionFactory, numberOfKeys, readsInQuery, readsInUpdate, writesInUpdate, delay);

        Benchmark.SaturationLevel saturationLevel = Benchmark.SaturationLevel.LOW;

        for(Benchmark.SaturationLevel level : Benchmark.SaturationLevel.values()) {
            if(saturation > level.value) {
                saturationLevel = level;
            }
        }

        ExecutorService executor = Executors.newFixedThreadPool(1);
        Benchmark.SaturationLevel finalSaturationLevel = saturationLevel;
        Future<?> benchmarkTask = executor.submit(() -> benchmark.execute(finalSaturationLevel, numberOfThreads));

        Scanner in = new Scanner(System.in);
        in.next();

        benchmark.stop();

        try {
            benchmarkTask.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Value("${id}")
    protected int id;

    @Value("${port}")
    protected int port;

    private SerializableService service;

    @Autowired
    public void setService(SerializableService service) {
        this.service = service;
    }

    @Profile("optimistic")
    private static class OptimisticConfig {

        @Value("${id}")
        protected int id;

        @Value("${address}")
        protected String address;

        @Value("${port}")
        protected int port;

        /*@Bean
        public ActorSystem actorSystem() {

            Properties properties = new Properties();
            properties.setProperty("akka.remote.netty.tcp.hostname", address);
            properties.setProperty("akka.remote.netty.tcp.port", String.valueOf(port));

            Config overrides = ConfigFactory.parseProperties(properties);
            Config config = overrides.withFallback(ConfigFactory.load());

            return ActorSystem.create(String.format("replica-%d", id), config);
        }

        @Bean
        public ActorRef frontActor(ActorSystem actorSystem, ResourceRepository resourceRepository, AtomicLong timestamp) {
            return actorSystem.actorOf(Props.create(FrontActor.class, resourceRepository, timestamp), "front");
        }*/

        @Bean
        public OptimisticServerChannelInboundHandlerFactory optimisticChannelHandlerFactory(ResourceRepository resourceRepository, AtomicLong timestamp) {
            return new OptimisticServerChannelInboundHandlerFactory(resourceRepository, timestamp);
        }

        @Bean
        public SerializableCertificationService serializableCertificationService(ResourceRepository resourceRepository, AtomicLong timestamp) {
            return new SerializableCertificationService(resourceRepository, timestamp);
        }
    }

    @Bean
    public ServerChannelInitializer serverChannelInitializer(ChannelInboundHandlerFactory channelInboundHandlerFactory) {
        return new ServerChannelInitializer(channelInboundHandlerFactory);
    }

    @Bean
    public ReplicaServer replicaServer(ServerChannelInitializer serverChannelInitializer) {

        try {
            ReplicaServer replicaServer = new ReplicaServer(port, serverChannelInitializer);
            replicaServer.start();
            return replicaServer;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Bean
    public AtomicLong timestamp() {
        return new AtomicLong(0);
    }

    @Bean
    public ResourceRepository resourceRepository() {
        return new ResourceRepository();
    }

    @Profile("pessimistic")
    private static class PessimisticConfig {

        @Bean
        public StateUpdateReceiver stateUpdateReceiver(Map<Long, TransactionMetadata> activeTransactions,
                                                       ResourceRepository resourceRepository, Map<Long, String> sessionIds,
                                                       SimpMessageSendingOperations messagingTemplate) {
            return new StateUpdateReceiver(activeTransactions, resourceRepository, sessionIds, messagingTemplate);
        }

        @Bean
        public Map<Long, TransactionMetadata> activeTransactions() {
            return new ConcurrentHashMap<>();
        }

        @Bean
        public String groupName() {
            return "cluster-0";
        }

        @Bean
        public GroupMessenger groupMessenger(String groupName, StateUpdateReceiver receiver) {
            return new GroupMessenger(groupName, receiver);
        }

        @Bean
        public Map<Long, String> sessionIds() {
            return new ConcurrentHashMap<>();
        }

        @Bean
        public BeginTransactionService beginTransactionService(AtomicLong timestamp, Map<Long, TransactionMetadata> activeTransactions) {
            return new BeginTransactionService(activeTransactions, timestamp);
        }
    }

    @Override
    public void run(String... strings) throws Exception {
        Replica replica = new Replica(new lsr.common.Configuration(getClass().getClassLoader().getResourceAsStream("paxos.properties")), id, service);
        replica.start();
    }
}
