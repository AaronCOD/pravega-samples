package io.pravega.example.gettingstarted;

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ByteArraySerializer;
import io.pravega.client.stream.impl.JavaSerializer;
import org.apache.logging.log4j.*;

import java.net.URI;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class PressureWriter {
    private final static Logger logger = LogManager.getLogger(PressureWriter.class);
    private final String scope;
    private final String streamName;
    private final URI controllerURI;
    private final  ClientConfig config;
    private final BlockingQueue<byte[]> eventsQueue = new LinkedBlockingQueue<>(100000);
    private static final int MESSAGE_SIZE = 500 * 1024;
    private final AtomicInteger messageCount = new AtomicInteger(0);
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
    }

    public void startWrite() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(100);
        executor.submit(new EventGenerator());
        for(int i = 0 ; i < 100; i++) {
            executor.submit(new WriterRunnable());
        }
        while(true){
            long start = System.nanoTime();
            Thread.sleep(1000);
            long end = System.nanoTime();
            System.out.format("sent %s message in %s us \n", messageCount.get(), (end - start)/1000);
            System.out.format("event queue size %s \n", eventsQueue.size());
            messageCount.set(0);
        }


    }
    public static void main(String[] args) throws InterruptedException {
        PressureWriter writer = new PressureWriter("aaron", "pressure", URI.create("tcp://10.37.1.191:9090"));
        writer.init();
        writer.startWrite();
    }

     class WriterRunnable implements Runnable {
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
                System.out.format("%s.%n", e.getMessage());
            } catch (ExecutionException e) {
                System.out.format("%s.%n", e.getMessage());
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
                        System.out.format("%s.%n", e.getMessage());
                    }
                }
            }
        }
    }

}

