import io.pravega.example.gettingstarted.PressureWriter;
import org.junit.Test;

import java.net.URI;


public class PressureWriterTest {
    @Test
    public void testWriter() throws InterruptedException {

        PressureWriter writer = new PressureWriter("aaron", "pressure", URI.create("tcp://10.37.1.191:9090"));
        writer.init();
        writer.startWrite();

    }
}
