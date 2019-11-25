package io.pravega.example.gettingstarted;

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.ByteArraySerializer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.*;

import java.net.URI;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.pravega.example.gettingstarted.HelloWorldWriter.getOptions;
import static io.pravega.example.gettingstarted.HelloWorldWriter.parseCommandLineArgs;

public class PressureWriter {
    private final static Logger logger = LogManager.getLogger(PressureWriter.class);
    private final String scope;
    private final String streamName;
    private final URI controllerURI;
    private final  ClientConfig config;
    private final BlockingQueue<byte[]> eventsQueue = new LinkedBlockingQueue<>(10000);
    private static final int MESSAGE_SIZE = 500 * 1024;
    private static final int THREAD_POOL_SIZE = 10;
    private final AtomicInteger messageCount = new AtomicInteger(0);
    private static final int READER_TIMEOUT_MS = 1000;
    public PressureWriter(String scope, String streamName, URI controllerURI) {
        this.scope = scope;
        this.streamName = streamName;
        this.controllerURI = controllerURI;
        this.config = ClientConfig.builder().controllerURI(controllerURI).build();
    }

    public void init(){
        StreamManager streamManager = StreamManager.create(controllerURI);
        final boolean scopeIsNew = streamManager.createScope(scope);

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        final boolean streamIsNew = streamManager.createStream(scope, streamName, streamConfig);
        final String readerGroup = "readerGroup-default";
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, streamName))
                .build();
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
            readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
        }
    }

    public void startWrite() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE * 2);
        executor.submit(new EventGenerator());
        for(int i = 0 ; i < THREAD_POOL_SIZE - 1; i++) {
            executor.submit(new WriterRunnable("routingKey" + i));
            executor.submit(new ReaderRunnable("reader" + i));
        }
        logger.info("dispatched writers to {} threads", THREAD_POOL_SIZE);
        while(true){
            long start = System.nanoTime();
            Thread.sleep(1000);
            long end = System.nanoTime();
            logger.info("sent {} message in {} us \n", messageCount.get(), (end - start)/1000);
            logger.info("event queue size {} \n", eventsQueue.size());
            messageCount.set(0);
        }
    }
    public static void main(String[] args) throws InterruptedException {
        logger.info("start main");
        Options options = getOptions();
        CommandLine cmd = null;
        try {
            cmd = parseCommandLineArgs(options, args);
        } catch (ParseException e) {
            logger.error("exception", e);
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("HelloWorldWriter", options);
            System.exit(1);
        }
        final String uriString = cmd.getOptionValue("uri") == null ? Constants.DEFAULT_CONTROLLER_URI : cmd.getOptionValue("uri");
        PressureWriter writer = new PressureWriter("aaron", "pressure", URI.create(uriString));
        writer.init();
        logger.info("start to write to {}", uriString);
        writer.startWrite();
    }

    class ReaderRunnable implements Runnable {

        private final String reader;

        ReaderRunnable(String reader) {
            this.reader = reader;
        }

        @Override
        public void run() {
            try (ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
                 EventStreamReader<byte[]> readerStream = clientFactory.createReader(reader,
                         "readerGroup-default",
                         new ByteArraySerializer(),
                         ReaderConfig.builder().build())) {
                System.out.format("Reading all the events from %s/%s%n", scope, streamName);
                EventRead<byte[]> event = null;
                do {
                    try {
                        event = readerStream.readNextEvent(READER_TIMEOUT_MS);
                        if (event.getEvent() != null) {
                           logger.info("Read event with size {}", event.getEvent().length);
                        }
                    } catch (ReinitializationRequiredException e) {
                        //There are certain circumstances where the reader needs to be reinitialized
                        e.printStackTrace();
                    }
                } while (event != null);
                logger.info("No more events from {}, {}", scope, streamName);
            }
        }
    }

     class WriterRunnable implements Runnable {

        private final String routingKey;

         WriterRunnable(String routingKey) {
             this.routingKey = routingKey;
         }

         @Override
        public void run() {
            try (ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
                 EventStreamWriter<byte[]> writer = clientFactory.createEventWriter(streamName,
                         new ByteArraySerializer(),
                         EventWriterConfig.builder().build())) {
                while (true) {
                    byte[] b = eventsQueue.poll();
                    final CompletableFuture writeFuture = writer.writeEvent("default_routing", b);
                    writeFuture.get();
                    messageCount.incrementAndGet();
                }
            } catch (InterruptedException e) {
                logger.error("exception", e);
            } catch (ExecutionException e) {
                logger.error("ExecutionException", e);
            }
        }

    }



    class EventGenerator implements Runnable {
        private final Random r = new Random();
        @Override
        public void run() {
            while(true) {
                byte[] b = new byte[MESSAGE_SIZE];
                r.nextBytes(b);
                if (!eventsQueue.offer(b)) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        logger.error("exception", e);
                    }
                }
            }
        }
    }

}

