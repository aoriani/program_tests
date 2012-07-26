

import java.util.Arrays;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;


/**
 * Resilient {@link ZooKeeper#getData}
 */
public final class GetDataTransaction extends Transaction<byte[]> {

    /** The watcher to be notified of changes in the node*/
    private Watcher watcher;

    /**
     * @param conn ZooKeeper connection
     * @param nodePath the path to znode you want to get data
     * @param watcher TODO
     */
    public GetDataTransaction(ZooKeeper conn,String nodePath, Watcher watcher){
        super(conn,nodePath);
        this.watcher = watcher;
    }



    @Override
    public String toString() {
        return "GetDataTransaction [result=" + Arrays.toString(result) + ", path=" + path
                + ", zooConn=" + zooConn + ", watcher=" + watcher +"]";
    }

    @Override
    protected void trasactionBody() throws KeeperException, InterruptedException{
        result = zooConn.getData(path, watcher, null);
    }
}
