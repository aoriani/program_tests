import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

public class NamenodeLookupZooKeeper implements Watcher, NamenodeLookup {

    private static final Logger LOG = Logger
            .getLogger(NamenodeLookupZooKeeper.class);

    private static final String NAMENODE_ZOO_PATH = "/namenode";
    private static final int TIMEOUT = 180000;

    private CountDownLatch connectedSignal;
    private String connectionString;
    private NamenodeChangedListener listener;
    private volatile String namenodeAddress = null;
    private final Watcher namenodeWatcher = new Watcher() {

        @Override
        public void process(WatchedEvent event) {
            if (event.getState() == KeeperState.Expired) {
                // Reinitialize
                init();
            } else if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
                LOG.info("Namenode has changed");
                retrieveNamenodeAddress();
            }

        }

    };
    private ZooKeeper zooConn;

    public NamenodeLookupZooKeeper(String zookeeperConnString) {
        this.connectionString = zookeeperConnString;
        init();
    }

    private void init() {
        try {
            connectedSignal = new CountDownLatch(1);
            connect();
            retrieveNamenodeAddress();
        } catch (IOException e) {
            LOG.fatal("Problem when connecting to Zookeeper", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void connect() throws IOException, InterruptedException {
        zooConn = new ZooKeeper(connectionString, TIMEOUT, this);
        connectedSignal.await();
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getState()) {
            case SyncConnected:
                LOG.info("Connected to ZooKeeper");
                connectedSignal.countDown();
                break;
            case Expired:
                // Reinitialize
                init();
                break;
        }

    }

    private void retrieveNamenodeAddress() {
        byte[] data = null;
        try {
            data = (new GetDataTransaction(zooConn, NAMENODE_ZOO_PATH,
                    namenodeWatcher)).invoke();
        } catch (KeeperException e) {
            // There is nothing we can do. We suppose the node exist
            LOG.warn(
                    "Something bad happening when retrieving the namenode address",
                    e);
        } catch (InterruptedException e) {
            // Propagate
            Thread.currentThread().interrupt();
        }
        if (data != null) {
            String address = new String(data, Charset.forName("UTF-8"));

            if (namenodeAddress == null) { // First time we retrieved the address
                namenodeAddress = address;
            } else { // We already have the address, check if it has changed
                if (!namenodeAddress.equals(address)) {// address has changed
                    // Update and notify
                    namenodeAddress = address;
                    if (listener != null) {
                        listener.namenodeChanged(namenodeAddress);
                    }
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see NamenodeLookup#getListener()
     */
    @Override
    public NamenodeChangedListener getListener() {
        return listener;
    }

    /*
     * (non-Javadoc)
     * 
     * @see NamenodeLookup#getNamenodeAddress()
     */
    @Override
    public String getNamenodeAddress() {
        return namenodeAddress;
    }

    /*
     * (non-Javadoc)
     * 
     * @see NamenodeLookup#setListener(NamenodeChangedListener)
     */
    @Override
    public void setListener(NamenodeChangedListener listener) {
        this.listener = listener;
    }

    /*
     * (non-Javadoc)
     * 
     * @see NamenodeLookup#shutdown()
     */
    @Override
    public void shutdown() throws InterruptedException {
        if (zooConn != null) {
            zooConn.close();
        }
    }

}
