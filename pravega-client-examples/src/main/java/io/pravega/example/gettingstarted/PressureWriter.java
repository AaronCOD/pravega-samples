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
    private final BlockingQueue<byte[]> eventsQueue = new LinkedBlockingQueue<>(1000);
    private static final int MESSAGE_SIZE = 10 * 1024;
    private static final int THREAD_POOL_SIZE = 20;
    private final AtomicInteger messageCount = new AtomicInteger(0);
    private static final int READER_TIMEOUT_MS = 30 * 1000;
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
                .scalingPolicy(ScalingPolicy.fixed(1)).retentionPolicy(RetentionPolicy.bySizeBytes(1024))
                .build();
        final boolean streamIsNew = streamManager.updateStream(scope, streamName, streamConfig);
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
            int n1 = messageCount.get();
            Thread.sleep(1000);
            long end = System.nanoTime();
            logger.info("sent {} message in {} us \n", messageCount.get() - n1, (end - start)/1000);
            logger.info("event queue size {} \n", eventsQueue.size());

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
            formatter.printHelp("PressureWriter", options);
            System.exit(1);
        }
        final String uriString = cmd.getOptionValue("uri") == null ? Constants.DEFAULT_CONTROLLER_URI : cmd.getOptionValue("uri");
        PressureWriter writer = new PressureWriter("aaron", "pressure1", URI.create(uriString));
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
               logger.info("Reading all the events from {} {}", scope, streamName);
                EventRead<byte[]> event = null;
                do {
                    try {
                        event = readerStream.readNextEvent(READER_TIMEOUT_MS);
                        if (event != null) {
                           logger.info("Read event with size {}", event.getEvent().length);
                        }
                    } catch (ReinitializationRequiredException e) {
                        //There are certain circumstances where the reader needs to be reinitialized
                        logger.error("Read event exception", e);
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
                    if (b == null) {
                        Thread.sleep(1000);
                        continue;
                    }
                    final CompletableFuture writeFuture = writer.writeEvent(routingKey, b);
                    writeFuture.get();
                    messageCount.incrementAndGet();
//                    logger.info("sent message");
                }
            } catch (InterruptedException e) {
                logger.error("exception", e);
            } catch (ExecutionException e) {
                logger.error("writer ExecutionException", e);
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
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        logger.error("exception", e);
                    }
                }
            }
        }
    }

}

