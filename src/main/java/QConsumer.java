import org.apache.zookeeper.KeeperException;

/**
 * Created by prayag on 11/18/15.
 * Every consumer must implement this.
 */
public interface QConsumer {
    public void startConsuming() throws KeeperException, InterruptedException;
}
