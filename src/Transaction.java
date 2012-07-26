

import java.util.Random;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;


/**
 * This is the base class to implement all operations with zoopkeeper.
 * It is responsible to retry the operation in case of connection failures
 * with ZooKeeper
 *
 * @param <ReturnType> The type of the return of operation
 *
 */
public abstract class Transaction<ReturnType> {

    /** Logger */
    private static final Logger LOG = Logger.getLogger(GetDataTransaction.class);

    /** The maximum numbers that an transaction can retried */
    private static final int MAX_RETRIES = 10;
    /** The basic delay factor for exponential back off  */
    private static final long RETRY_DELAY_FACTOR = 250L;

    private Random  randomGen = new Random();

    /** The path for the node we are operating on*/
    protected String path;
    /** The system ZooKeeper instance */
    protected ZooKeeper zooConn;
    /**The result for the zookeeper operation*/
    protected ReturnType result;



    /**
     * @param conn pass the system's ZooKeeper connection
     * @param nodePath the path for the node we are working on
     */
    public Transaction(ZooKeeper conn,String nodePath){
        zooConn = conn;
        path = nodePath;
    }

    /**
     * Executes the transaction body. If it fails, it retries at a maximum of {@link #MAX_RETRIES} times.
     * Subclasses must call this via a convenient method to user.
     *
     * <p><b>Sample code:</b></p>
     *
     * <pre>
     * protected void trasactionBody(){
     * 	result = zooConn.exists(path,null,null);
     * }
     *
     * public Stat exists(){
     * 	execute();
     * 	return result;
     * }
     *
     * </pre>
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    protected final void execute() throws KeeperException, InterruptedException {
        KeeperException exception = null;

        LOG.debug("Executing " + this.toString());

        for(int i = 1;i <= MAX_RETRIES; i++){

            try{
                trasactionBody();
                //If we reach this  line, we succeed. We can return safely
                return;
            }
            catch(KeeperException.SessionExpiredException e){
                LOG.fatal("Session expired :" + this, e);
                //We do not handle this exception because we are believed to be dead
                throw e;
            }
            catch (KeeperException.ConnectionLossException e ){
                LOG.warn("Connection loss occured at attempt #"+ i + ": " + this , e);
                //record the exception to throw later if all attempts fail;
                exception = e;
            }

            delay(i);
        }
        //If we are here it is because all attempts have failed.
        //Pass exception to caller so it can try to handle
        throw exception;
    }

    /**
     * Implements a exponential back off protocol
     * @param attempts
     * @throws InterruptedException
     */
    private void delay(int attempts) throws InterruptedException {
        if(attempts > 0){
            long  delay = RETRY_DELAY_FACTOR * randomGen.nextInt(1 << attempts);
            LOG.info("Waiting " + delay + " milliseconds to retry");
            Thread.sleep(delay);
        }

    }

    /**
     * Subclasses must implement the Zookeeper operation here
     * @throws KeeperException if something goes really wrong, e.g.,
     * you are trying to read a non existent node you are going to get a {@link KeeperException.NoNodeException}
     * @throws InterruptedException
     */
    protected abstract void trasactionBody() throws KeeperException,
            InterruptedException;


    /**
     * Calls and execute operation on zookeeper ensemble
     * @return the result of zookeeper operation
     * @throws KeeperException
     * @throws InterruptedException
     */
    public ReturnType invoke() throws KeeperException, InterruptedException{
        execute();
        return result;

    }

}
