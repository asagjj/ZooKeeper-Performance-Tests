import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * Created by prayag on 11/18/15.
 * Interface for a Runnable Test. Every test must implement this to be run by the driver.
 */
public interface RunnableTest {
    public void runTest() throws KeeperException, InterruptedException, IOException;
}
