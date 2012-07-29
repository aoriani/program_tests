import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class FailoverTest implements NamenodeChangedListener {

    private static final Logger LOG = Logger.getLogger(FailoverTest.class);

    // Waiting time configs
    private static final int FAILOVER_INPROGRESS_WAIT_MIN = 5;
    private static final int REGULAR_ERROR_WAIT_MS = 2000;
    private static final int FAILOVER_COMPLETED_WAIT_MS = 2000;

    private static final int BUFFER_SIZE = 4096;
    private static final int MAX_ATTEMPTS = 10;

    private NamenodeLookup namenodeLookup;
    private int blocks;
    private int files;
    private String hostname;
    private volatile String namenodeAddress;
    private volatile CountDownLatch waitFailoverLatch;
    private volatile boolean isFailoverComplete;

    public FailoverTest(NamenodeLookup namenode, int blocks, int files)
            throws UnknownHostException {
        this.namenodeLookup = namenode;
        namenodeLookup.setListener(this);
        this.blocks = blocks;
        this.files = files;
        this.namenodeAddress = namenode.getNamenodeAddress();
        this.hostname = InetAddress.getLocalHost().getHostName();
        this.waitFailoverLatch = new CountDownLatch(1);
        this.isFailoverComplete = false;

    }

    @Override
    public void namenodeChanged(String newAddress) {
        synchronized (this) {
            LOG.info("EVENT: NAMENODE_CHANGED " + newAddress);
            namenodeAddress = newAddress;
            isFailoverComplete = true;
            // Signalize that we should no longer wait for failover
            waitFailoverLatch.countDown();
        }

    }

    private synchronized String getNamenodeAddress() {
        return namenodeAddress;
    }

    private void run() throws InterruptedException {

        for (int i = 1; i <= files; i++) {
            String filename = hostname + "-" + i;
            oneRun(filename);
        }
    }

    private void oneRun(String filename) throws InterruptedException {
        try {
            create(filename);
        } catch (IOException e) {
            LOG.error("I/O error", e);
        }
    }

    private void create(String filename) throws InterruptedException,
    IOException {
        for (int i = 1; i <= MAX_ATTEMPTS; i++) {
            LOG.info("EVENT: ATTEMPT_WRITE " + Integer.toString(i) + " to "
                    + filename);
            try {
                attemptCreate(filename);
                return;

            } catch (NameNodeChangedException e) {
                // We have been alerted that NN changed
                LOG.info("EVENT: FAILURE  NN");
                LOG.warn("Namenode Changed in the middle of write");
                handleFailoverAlreadyDone();

            } catch (IOException e) {
                // Failover? Assume HADOOP RPC already retries so
                // if we reach here is because failover is on the way
                LOG.info("EVENT: FAILURE IO");
                LOG.error("I/O error", e);
                handleGenericIOException();
            }
        }
        // If we reach here something bad happened
        LOG.fatal("Failed completely to write the file " + filename);
        throw new IOException("Could not create file " + filename);

    }

    private void attemptCreate(final String filename) throws IOException {
        InputStream in = null;
        FileSystem hdfs = null;
        OutputStream out = null;

        LOG.info("BEGIN attemptCreate" + filename);

        try {
            String currentNamenodeAddress = getNamenodeAddress();
            LOG.info("EVENT: WRITE_STARTED " + filename + " on "
                    + currentNamenodeAddress);
            in = new BufferedInputStream(new DeadBeefInputStream(blocks));
            String hdfsFileUri = "hdfs://" + currentNamenodeAddress + "/"
                    + filename;
            Configuration conf = new Configuration();
            hdfs = FileSystem.get(URI.create(hdfsFileUri), conf);
            out = hdfs.create(new Path(hdfsFileUri), true, BUFFER_SIZE, null);
            doWrite(in, out);
            LOG.info("EVENT: WRITE_COMPLETE " + filename);

        } finally {
            LOG.info("BEGIN FINALLY" + filename);
            if (out != null) {
                LOG.info("Closing FILE " + filename);
                try {
                    out.close();
                } catch (Exception e) {
                    LOG.error("Error closing file " + filename);
                }
            }

            if (hdfs != null) {
                LOG.info("Closing HDFS");
                try {
                    hdfs.close();
                } catch (Exception e) {
                    LOG.error("Error closing HDFS for file " + filename);
                }
            }
            LOG.info("END FINALLY" + filename);
        }

        LOG.info("END attemptCreate" + filename);
    }

    private void doWrite(InputStream in, OutputStream out) throws IOException {
        int countBytes = 0;
        int data;
        boolean failoverStateBefore = isFailoverComplete;

        while ((data = in.read()) != -1) {
            out.write(data);
            if ((++countBytes) == BUFFER_SIZE) {
                countBytes = 0;
                out.flush();
            }

            boolean currentFailoverState = isFailoverComplete;
            if (failoverStateBefore != currentFailoverState)
                throw new NameNodeChangedException();
        }
    }

    private void handleFailoverAlreadyDone() {
        /*
         * We know already that Hot Standby Node has already complete failover.
         * So just wait a little before trying again. In real world we should
         * wait a random time to prevent waves of clients flooding the standby
         */
        try {
            LOG.info("Start small wait after failover complete");
            Thread.sleep(FAILOVER_COMPLETED_WAIT_MS);
            LOG.info("Finish small wait after failover complete");
        } catch (InterruptedException e) {
            // Just propagate the interruption as we already stopping sleeping
            Thread.currentThread().interrupt();
        }
    }

    private void handleGenericIOException() {

        if (!isFailoverComplete) {
            /*
             * Right now, we don't know what happened. We may be in the middle
             * of a failover or of some hiccup of namenode. Let's expect for the
             * worst: a failover. Although being pessimistic here may be not the
             * more efficient solution here, it is the simpler and safer
             */
            LOG.info("Failover hasn't happened yet. Assuming it is in progress."
                    + " We'll wait until it is finished");
            try {
                // Wait for little bit longer than ZooKeeper timeout
                // or for failover complete
                waitFailoverLatch.await(FAILOVER_INPROGRESS_WAIT_MIN,
                        TimeUnit.MINUTES);
                if (isFailoverComplete) {
                    // We do wait until it is complete
                    LOG.info("EVENT: FAILOVER_COMPLETE");
                } else {
                    // Wait timeout
                    LOG.warn("Some big failure happened, because faiolver did not happen");
                }
            } catch (InterruptedException e) {
                // Just propagate the interruption as we already stopping
                // sleeping
                Thread.currentThread().interrupt();
            }
        } else {
            // Okay this might be a standby hiccup, so wait 5 sec and try again
            LOG.warn("We know that NN already changed, some I/0 error "
                    + "or the RPC has not given up yet");
            try {
                Thread.sleep(REGULAR_ERROR_WAIT_MS);
            } catch (InterruptedException e) {
                // Just propagate the interruption as we already stopping
                // sleeping
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException,
    IOException {

        if ((args.length == 3)) {
            String zookeeper = args[0];
            int files = Integer.parseInt(args[1]);
            int blocks = Integer.parseInt(args[2]);

            NamenodeLookup nnlkp = new NamenodeLookupZooKeeper(zookeeper);
            FailoverTest cli = new FailoverTest(nnlkp, blocks, files);
            cli.run();
        } else {
            System.err.println("Client received wrong arguments");
        }
    }

}

@SuppressWarnings("serial")
final class NameNodeChangedException extends IOException {

    public NameNodeChangedException() {
        super("NameNode has changed");
    }
}
