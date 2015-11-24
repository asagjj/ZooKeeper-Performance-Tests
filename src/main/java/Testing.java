/**
 * Created by prayag on 11/17/15.
 */
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * Driver class for all tests
 * Runs all tests deterministically, one after the other and
 */
public class Testing{
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        int numThreads = 1;
        for(String arg : args) {
            System.out.println(arg);
        }
        if(args.length < 1){
            System.out.println("Usage: Testing hostAndPort [numThreads]");
            System.exit(1);
        }
        String hostAndPort = args[0];
        if(args.length > 1) numThreads = Integer.parseInt(args[1]);
        //RunnableTest test = new SyncReadWriteRunner(hostAndPort, numThreads);
        //test.runTest();

        //RunnableTest test = new QRunner(hostAndPort, numThreads);
        //test.runTest();

        RunnableTest test = new LockRunner(new String[]{hostAndPort}, numThreads);
        test.runTest();
        System.out.println("Finished!");
    }
}
