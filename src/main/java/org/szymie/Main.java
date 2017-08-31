package org.szymie;

import com.google.common.collect.TreeMultiset;
import io.netty.channel.ChannelHandlerContext;
import lsr.common.PID;
import lsr.paxos.replica.Replica;
import lsr.service.SerializableService;
import org.apache.commons.cli.*;
import org.apache.commons.lang.ArrayUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.szymie.client.strong.optimistic.ClientChannelInitializer;
import org.szymie.client.strong.optimistic.NettyRemoteGateway;
import org.szymie.client.strong.optimistic.OptimisticClientMessageHandlerFactory;
import org.szymie.client.strong.RemoteGateway;
import org.szymie.client.strong.pessimistic.PessimisticClientMessageHandlerFactory;
import org.szymie.messages.Messages;
import org.szymie.server.strong.ChannelInboundHandlerFactory;
import org.szymie.server.strong.ReplicaServer;
import org.szymie.server.strong.ServerChannelInitializer;
import org.szymie.server.strong.optimistic.*;
import org.szymie.server.strong.pessimistic.*;

import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication(exclude = {MongoAutoConfiguration.class, MongoDataAutoConfiguration.class})
public class Main implements CommandLineRunner, PaxosProcessesCreator {

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

        if(commandLine.hasOption("I")) {
            runInit(commandLine);
            System.exit(0);
        } else {

            String[] arguments = Stream.of("id", "port", "address", "paxosProcesses", "bossThreads", "workerThreads")
                    .map(argument -> String.format("--%s=%s", argument, commandLine.getOptionValue(argument)))
                    .toArray(String[]::new);

            String[] allArguments = (String[]) ArrayUtils.addAll(args, arguments);

            SpringApplication application = new SpringApplication(Main.class);

            try {
                application.run(allArguments);
            } catch (Throwable e) {
                System.err.println("Uncaught exception: " + e.getMessage());
                e.printStackTrace(System.err);
            }
        }
    }

    private static CommandLine createCommandLine(String[] args) throws ParseException {
        Options options = createOptions();
        ExtendedDefaultParser parser = new ExtendedDefaultParser();
        return parser.parse(options, args);
    }

    private static Options createOptions() {

        Options options = new Options();

        addToOptions(options, "I", "init", false);
        addToOptions(options, null, "id", true);
        addToOptions(options, null, "port", true);
        addToOptions(options, null, "paxosProcesses", true);
        addToOptions(options, null, "replicas", true);
        addToOptions(options, null, "bossThreads", true);
        addToOptions(options, null, "workerThreads", true);
        addToOptions(options, null, "numberOfKeys", true);
        addToOptions(options, null, "initNumberOfValues", true);

        return options;
    }

    private static void addToOptions(Options options, String shortOption, String longOption, boolean hasArg) {
        options.addOption(Option.builder(shortOption).longOpt(longOption).hasArg(hasArg).build());
    }

    private static void runInit(CommandLine commandLine) {

        //NettyRemoteGateway remoteGateway = new NettyRemoteGateway(new ClientChannelInitializer(new OptimisticClientMessageHandlerFactory()));
        NettyRemoteGateway remoteGateway = new NettyRemoteGateway(new ClientChannelInitializer(new PessimisticClientMessageHandlerFactory()));
        String replicas = commandLine.getOptionValue("replicas");

        int numberOfKeys = Integer.parseInt(commandLine.getOptionValue("numberOfKeys"));
        int initNumberOfValues = Integer.parseInt(commandLine.getOptionValue("initNumberOfValues"));

        int[] initKeys = ThreadLocalRandom.current().ints(0, numberOfKeys).distinct().limit(initNumberOfValues).toArray();

        Map<String, String> writes = new HashMap<>();

        for(int initKey : initKeys) {
            writes.put("key" + initKey, "1");
        }

        Messages.InitRequest initRequest = Messages.InitRequest.newBuilder()
                .putAllWrites(writes)
                .build();

        Messages.Message request = Messages.Message.newBuilder()
                .setInitRequest(initRequest)
                .build();

        Stream.of(replicas.split(",")).forEach(replica -> {
            remoteGateway.connect(replica);
            System.err.println("connected");
            remoteGateway.sendAndReceive(request, Messages.InitResponse.class);
            remoteGateway.disconnect();
        });
    }

    @Value("${id}")
    protected int id;

    @Value("${port}")
    protected int port;

    @Value("${paxosProcesses}")
    protected String paxosProcesses;

    @Value("${bossThreads}")
    protected int bossThreads;

    @Value("${workerThreads}")
    protected int workerThreads;

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

        @Bean
        public TreeMultiset<Long> liveTransactions() {
            return TreeMultiset.create();
        }

        @Bean
        public Lock liveTransactionsLock() {
            return new ReentrantLock(true);
        }

        @Bean
        public OptimisticServerChannelInboundHandlerFactory optimisticChannelHandlerFactory(ResourceRepository resourceRepository,  @Qualifier("timestamp") AtomicLong timestamp,
                                                                                            TreeMultiset<Long> liveTransactions, Lock liveTransactionsLock) {
            return new OptimisticServerChannelInboundHandlerFactory(resourceRepository, timestamp, liveTransactions, liveTransactionsLock);
        }

        @Bean
        public SerializableCertificationService serializableCertificationService(ResourceRepository resourceRepository,  @Qualifier("timestamp") AtomicLong timestamp,
                                                                                 TreeMultiset<Long> liveTransactions, Lock liveTransactionsLock) {
            return new SerializableCertificationService(resourceRepository, timestamp, liveTransactions, liveTransactionsLock);
        }
    }

    @Bean
    public ServerChannelInitializer serverChannelInitializer(ChannelInboundHandlerFactory channelInboundHandlerFactory) {
        return new ServerChannelInitializer(channelInboundHandlerFactory);
    }

    @Bean
    public ReplicaServer replicaServer(ServerChannelInitializer serverChannelInitializer) {

        System.err.println("bossThreads: " + bossThreads);
        System.err.println("workerThreads: " + workerThreads);

        try {
            ReplicaServer replicaServer = new ReplicaServer(port, serverChannelInitializer, bossThreads, workerThreads);
            replicaServer.start();
            return replicaServer;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Bean(name = "timestamp")
    public AtomicLong timestamp() {
        return new AtomicLong(0);
    }

    @Bean
    public ResourceRepository resourceRepository() {
        return new ResourceRepository();
    }

    @Profile("pessimistic")
    private static class PessimisticConfig {

        @Value("${id}")
        protected int id;

        @Value("${paxosProcesses}")
        protected String paxosProcesses;

        @Bean
        public TreeMultiset<Long> liveTransactions() {
            return TreeMultiset.create();
        }

        @Bean
        public Lock liveTransactionsLock() {
            return new ReentrantLock(true);
        }

        @Bean
        public StateUpdateReceiver stateUpdateReceiver(Map<Long, TransactionMetadata> activeTransactions, BlockingMap<Long, Boolean> activeTransactionFlags,
                                                       ResourceRepository resourceRepository,  BlockingMap<Long, BlockingQueue<ChannelHandlerContext>> contexts,
                                                       TreeMultiset<Long> liveTransactions, Lock liveTransactionsLock, @Qualifier("lastCommitted") AtomicLong lastCommitted) {
            return new StateUpdateReceiver(activeTransactions, activeTransactionFlags, resourceRepository, contexts, liveTransactions, liveTransactionsLock, lastCommitted);
        }

        @Bean
        public Map<Long, TransactionMetadata> activeTransactions() {
            return new ConcurrentHashMap<>();
        }

        @Bean
        public BlockingMap<Long, Boolean> activeTransactionFlags() {
            return new BlockingMap<>();
        }

        @Bean
        public String groupName() {
            return "cluster-0";
        }

        @Bean(name = "lastCommitted")
        public AtomicLong lastCommitted() {
            return new AtomicLong(0);
        }

        @Bean
        public GroupMessenger groupMessenger(String groupName, StateUpdateReceiver receiver) {
            return new GroupMessenger(groupName, receiver);
        }

        @Bean
        public BlockingMap<Long, BlockingQueue<ChannelHandlerContext>> contexts() {
            return new BlockingMap<>();
        }

        @Bean
        public PessimisticServerChannelInboundHandlerFactory pessimisticServerChannelInboundHandlerFactory(
                ResourceRepository resourceRepository, @Qualifier("timestamp") AtomicLong timestamp, BlockingMap<Long, BlockingQueue<ChannelHandlerContext>> contexts,
                Map<Long, TransactionMetadata> activeTransactions,
                BlockingMap<Long, Boolean> activeTransactionFlags,
                TreeMultiset<Long> liveTransactions, Lock liveTransactionsLock,
                GroupMessenger groupMessenger,
                @Qualifier("lastCommitted") AtomicLong lastCommitted) {
            return new PessimisticServerChannelInboundHandlerFactory(id, paxosProcesses, resourceRepository, timestamp, contexts, activeTransactions, activeTransactionFlags, liveTransactions, liveTransactionsLock, groupMessenger, lastCommitted);
        }

        @Bean
        public TransactionService transactionService(@Qualifier("timestamp") AtomicLong timestamp, Map<Long, TransactionMetadata> activeTransactions, BlockingMap<Long,
                BlockingQueue<ChannelHandlerContext>> contexts, ResourceRepository resourceRepository,
                                                     BlockingMap<Long, Boolean> activeTransactionFlags,
                                                     TreeMultiset<Long> liveTransactions, Lock liveTransactionsLock) {
            return new TransactionService(id, resourceRepository, timestamp, activeTransactions, activeTransactionFlags, contexts, liveTransactions, liveTransactionsLock);
        }
    }

    @Override
    public void run(String... strings) throws Exception {

        Replica replica;
        InputStream paxosProperties = getClass().getClassLoader().getResourceAsStream("paxos.properties");

        List<PID> processes = createPaxosProcesses(paxosProcesses);

        if(!processes.isEmpty()) {
            replica = new Replica(new lsr.common.Configuration(processes, paxosProperties), id, service);
        } else {
            replica = new Replica(new lsr.common.Configuration(paxosProperties), id, service);
        }

        replica.start();
    }
}
