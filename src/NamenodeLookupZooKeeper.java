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

    private static final Logger LOG = Logger.getLogger(NamenodeLookupZooKeeper.class);


    private static final int TIMEOUT = 180000;
    private static final String NAMENODE_ZOO_PATH = "/namenode";


    private ZooKeeper zooConn;
    private String connectionString;
    private CountDownLatch connectedSignal;
    private volatile String namenodeAdress = "127.0.0.1";
    private NamenodeChangedListener listener;
    private final Watcher namenodeWatcher = new Watcher(){

        @Override
        public void process(WatchedEvent event) {
        	if(event.getState() == KeeperState.Expired){
                //Reinitialize
                init();
        	}else if(event.getType() == Watcher.Event.EventType.NodeDataChanged){
                LOG.info("Namenode has changed");
                retrieveNamenodeAddress();
            }

        }

    };



    private void connect() throws IOException, InterruptedException{
        zooConn = new ZooKeeper(connectionString, TIMEOUT,this);
        connectedSignal.await();
    }


    private void retrieveNamenodeAddress() {
        byte[] data = null;
        try {
            data = (new GetDataTransaction(zooConn,NAMENODE_ZOO_PATH,namenodeWatcher)).invoke();
        } catch (KeeperException e) {
            //There is nothing we can do. We suppose the node exist
            LOG.warn("Something bad happening when retrieving the namenode address", e);
        } catch (InterruptedException e) {
            //Propagate
            Thread.currentThread().interrupt();
        }
        if(data != null){
            String address = new String(data, Charset.forName("UTF-8"));
            namenodeAdress = address;
            if(listener != null){
                listener.namenodeChanged(address);
            }
        }

    }

    private void init(){
        try {
        	connectedSignal = new CountDownLatch(1);
            connect();
            retrieveNamenodeAddress();
        } catch (IOException e) {
            LOG.fatal("Problem when connecting to Zookeeper",e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }       
    }


    public NamenodeLookupZooKeeper(String zookeeperConnString){
        this.connectionString = zookeeperConnString;
        init();
    }

    /* (non-Javadoc)
	 * @see NamenodeLookup#getNamenodeAddress()
	 */
    @Override
	public String getNamenodeAddress(){
        return namenodeAdress;
    }



    /* (non-Javadoc)
	 * @see NamenodeLookup#setListener(NamenodeChangedListener)
	 */
    @Override
	public void setListener(NamenodeChangedListener listener) {
        this.listener = listener;
    }


    /* (non-Javadoc)
	 * @see NamenodeLookup#getListener()
	 */
    @Override
	public NamenodeChangedListener getListener() {
        return listener;
    }


    @Override
    public void process(WatchedEvent event) {
        switch (event.getState()) {
        case SyncConnected:
            LOG.info("Connected to ZooKeeper");
            connectedSignal.countDown();
            break;
        case Expired:
            //Reinitialize
            init();
            break;
        }

    }

    /* (non-Javadoc)
	 * @see NamenodeLookup#shutdown()
	 */
    @Override
	public void shutdown() throws InterruptedException{
        if(zooConn != null){
            zooConn.close();
        }
    }

}
