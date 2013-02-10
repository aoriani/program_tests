import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;


public class ClientTest implements NamenodeChangedListener {

	private static final int ONE_MB = 1024*1024;

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
    private volatile Configuration conf;


    public ClientTest(NamenodeLookup namenode, int blocks, int files) throws UnknownHostException{
        this.namenodeLookup = namenode;
        namenodeLookup.setListener(this);
        this.blocks = blocks;
        this.files = files;
        this.namenodeAddress = namenode.getNamenodeAddress();
        this.hostname = InetAddress.getLocalHost().getHostName();
        this.waitFailoverLatch = new CountDownLatch(1);
        this.isFailoverComplete = false;
        this.conf= new Configuration();
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
    }


    @Override
    public void namenodeChanged(String newAddress) {
        synchronized(this){
            System.out.println("Changed namenode to " + newAddress);
            namenodeAddress = newAddress;
            this.isFailoverComplete = true;
            this.waitFailoverLatch.countDown();
        }

    }

    private synchronized String getNamenodeAddress(){
        return namenodeAddress;
    }
    
    
    private void doIO(InputStream in, OutputStream out) throws IOException {
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

    //========================================================================
    // File Creation
    //=======================================================================
    
    private void attemptCreate(String filename) throws IOException{
        InputStream in= null;
        FileSystem hdfs = null;
        OutputStream out = null;

        try{
            System.out.println("Attempt to create " + filename);
            in = new BufferedInputStream(new DeadBeefInputStream(blocks));
            String currentNamenodeAddress = getNamenodeAddress();
            String hdfsFileUri = "hdfs://" + currentNamenodeAddress + "/" +filename;
            hdfs = FileSystem.get(URI.create(hdfsFileUri), conf);
            out = hdfs.create(new Path(hdfsFileUri), true, 4096,
                    new Progressable() {

                        @Override
                        public void progress() {
                            System.out.print(".");

                        }
                    });
            long start = System.currentTimeMillis();
            doIO(in, out);
            long end = System.currentTimeMillis();
            long elapsedTime = (end-start)/1000;
            double throughput = blocks*DeadBeefInputStream.HDFS_BLOCK/ONE_MB/((double)elapsedTime);
            System.out.println(String.format(Locale.ENGLISH,"\n%d client: hostName=%s, event=write, readTime=0, readThroughput=0, writeTime=%d, writeThroughput=%f",
            		System.currentTimeMillis(),hostname,elapsedTime,throughput));
        }finally{
            final FileSystem hdfs_final = hdfs;
            final OutputStream out_final = out;

            Runnable cleanupRunnable = new Runnable() {

                @Override
                public void run() {
                    // TODO Auto-generated method stub
                    if (out_final != null) {
                        try {
                            out_final.close();
                        } catch (Exception e) {
                        }
                    }

                    if (hdfs_final != null) {
                        try {
                            hdfs_final.close();
                        } catch (Exception e) {
                        }
                    }
                }
            };
            //Do in background
            new Thread(cleanupRunnable, "Cleanup-Creating-" + filename).start();
        }
    }

    private void create(String filename) throws InterruptedException, IOException{
        System.out.println("Creating " + filename);
        for (int i = 1; i <= MAX_ATTEMPTS; i++) {
            try {
                attemptCreate(filename);
                return;

            } catch (NameNodeChangedException e) {
                handleFailoverAlreadyDone();

            } catch (IOException e) {
                // Failover? Assume HADOOP RPC already retries so
                // if we reach here is because failover is on the way
                handleGenericIOException();
            }
        }
        // If we reach here something bad happened
        throw new IOException("Could not create file " + filename);
    }

    private void handleFailoverAlreadyDone() {
        /*
         * We know already that Hot Standby Node has already complete failover.
         * So just wait a little before trying again. In real world we should
         * wait a random time to prevent waves of clients flooding the standby
         */
        try {
            System.out.println("Start small wait after failover complete");
            Thread.sleep(FAILOVER_COMPLETED_WAIT_MS);
            System.out.println("Finish small wait after failover complete");
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
            System.out.println("Failover hasn't happened yet. Assuming it is in progress."
                    + " We'll wait until it is finished");
            try {
                // Wait for little bit longer than ZooKeeper timeout
                // or for failover complete
                waitFailoverLatch.await(FAILOVER_INPROGRESS_WAIT_MIN,
                        TimeUnit.MINUTES);
                if (isFailoverComplete) {
                    // We do wait until it is complete
                    System.out.println("EVENT: FAILOVER_COMPLETE");
                } else {
                    // Wait timeout
                    System.out.println("Some big failure happened, because faiolver did not happen");
                }
            } catch (InterruptedException e) {
                // Just propagate the interruption as we already stopping
                // sleeping
                Thread.currentThread().interrupt();
            }
        } else {
            // Okay this might be a standby hiccup, so wait 5 sec and try again
            System.out.println("We know that NN already changed, some I/0 error "
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

    //========================================================================
    // File listing
    //========================================================================

    private void attemptFileListing() throws IOException{
        System.out.println("Attempt to list root");
        String hdfsFileUri = "hdfs://" + getNamenodeAddress() + "/";
        FileSystem hdfs = null;
        try{
            hdfs = FileSystem.get(URI.create(hdfsFileUri), conf);
            FileStatus[] fss = hdfs.listStatus(new Path(hdfsFileUri));
            for (FileStatus fs:fss){
                System.out.println(fs.getPath().toString());
            }
        }finally{
            final FileSystem hdfs_final = hdfs;
            Runnable cleanupRunnable = new Runnable(){

                @Override
                public void run() {
                    if(hdfs_final != null){
                        try {
                            hdfs_final.close();
                        } catch (IOException e) {
                        }
                    }
                }
                
            };
                    
            //Do in background
            new Thread(cleanupRunnable, "Cleanup-Listing-" + hostname).start();
        }
    }

    private void fileListing() throws InterruptedException, IOException{
        System.out.println("Listing files ");
        for(int i=1; i<=MAX_ATTEMPTS;i++){
            try{
                attemptFileListing();
                return;
            }
            catch (NameNodeChangedException e){
                System.out.println("NameNode Changed while listing");
                handleFailoverAlreadyDone();
            }
            catch (IOException e){
                System.out.println("IOException when listing files on "
                        + namenodeAddress + "on Attempt " + i);
                handleGenericIOException();
            }
        }
        //If we reach here something bad happened
        throw new IOException("Could not list files");
    }


    //========================================================================
    // File reading
    //========================================================================

    //Attention lower throughput if run co-located on nodes with data
    //https://issues.apache.org/jira/browse/HDFS-347


    private void attemptRead(String filename) throws IOException{
        System.out.println("attempt to read" + filename);
        OutputStream out = new NullOutputStream();
        FileSystem hdfs = null;

        try {

            String hdfsFileUri = "hdfs://" + getNamenodeAddress() + "/" + filename;
            hdfs = FileSystem.get(URI.create(hdfsFileUri), conf);

            InputStream in = hdfs.open(new Path(hdfsFileUri), 4096);
            long start = System.currentTimeMillis();
            doIO(in, out);// has a finally to close the streams
            long end = System.currentTimeMillis();
            long elapsedTime = (end-start)/1000;
            double throughput = blocks*DeadBeefInputStream.HDFS_BLOCK/ONE_MB/((double)elapsedTime);
            System.out.println(String.format(Locale.ENGLISH,"\n%d client: hostName=%s, event=read, readTime=%d, readThroughput=%f, writeTime=0, writeThroughput=0",
            		System.currentTimeMillis(),hostname,elapsedTime,throughput));
        } finally {
            final FileSystem hdfs_final = hdfs;
            final OutputStream out_final = out;

            Runnable cleanupRunnable = new Runnable() {

                @Override
                public void run() {
                    // TODO Auto-generated method stub
                    if (out_final != null) {
                        try {
                            out_final.close();
                        } catch (Exception e) {
                        }
                    }

                    if (hdfs_final != null) {
                        try {
                            hdfs_final.close();
                        } catch (Exception e) {
                        }
                    }
                }
            };
            //Do in background
            new Thread(cleanupRunnable, "Cleanup-Creating-" + filename).start();
        }
    }


    private void read(String filename) throws InterruptedException, IOException{
        System.out.println("Reading " + filename);
        for(int i=1; i<=MAX_ATTEMPTS;i++){
            try{
                attemptRead(filename);
                return;
            } catch (NameNodeChangedException e) {
                handleFailoverAlreadyDone();
    
            } catch (IOException e) {
                // Failover? Assume HADOOP RPC already retries so
                // if we reach here is because failover is on the way
                System.out.println(e.getMessage());
                e.printStackTrace();
                handleGenericIOException();
            }
        }
        //If we reach here something bad happened
        throw new IOException("Could not read file " + filename);

    }

    //============================================================
    // Test Case


    private void oneRun(String filename) throws InterruptedException{
        try{
            create(filename);
            fileListing();
            read(filename);
        }catch (IOException e ){
            System.out.print(e.getMessage());
        }
    }

    private void run() throws InterruptedException{

        for(int i=1; i<=files;i++){
             String filename = hostname+"-" + i;
             oneRun(filename);
        }

    }



    /**
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException, IOException {

        String hostname = InetAddress.getLocalHost().getHostName();
        System.out.println(String.format("\n%d client: hostName=%s, event=start, readTime=0, readThroughput=0, writeTime=0, writeThroughput=0",
        		System.currentTimeMillis(),hostname));


        if (args.length == 6 || args.length ==5){
            int nnLookupType = NamenodeLookup.STATIC;
            if(args[0].equalsIgnoreCase("ha")){nnLookupType=NamenodeLookup.HA;}
            String primaryNamenode = args[1];
            String zookeeper = args[2];
            int files = Integer.parseInt(args[3]);
            int blocks = Integer.parseInt(args[4]);

            NamenodeLookup nnlkp = NamenodeLookupFactory.create(nnLookupType, primaryNamenode, zookeeper);
            ClientTest cli = new ClientTest(nnlkp,blocks,files);
            cli.run();
        }else{
            System.out.println("Client received wrong arguments");
        }

        System.out.println(String.format("\n%d client: hostName=%s, event=end, readTime=0, readThroughput=0, writeTime=0, writeThroughput=0",
        		System.currentTimeMillis(),hostname));

    }
}
