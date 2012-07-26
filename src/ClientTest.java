import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;


public class ClientTest implements NamenodeChangedListener {

	private static final int ONE_MB = 1024*1024;

    private static final int MAX_ATTEMPTS = 10;
    private NamenodeLookup namenodeLookup;
    private int blocks;
    private int files;
    private boolean shouldDelete;

    private String namenodeAddress;
    private String hostname;


    public ClientTest(String hostname, NamenodeLookup namenode, int blocks, int files, boolean shouldDelete) throws UnknownHostException{
        this.namenodeLookup = namenode;
        namenodeLookup.setListener(this);
        this.blocks = blocks;
        this.files = files;
        this.namenodeAddress = namenode.getNamenodeAddress();
        this.hostname = hostname;
        this.shouldDelete = shouldDelete;
    }


    @Override
    public void namenodeChanged(String newAddress) {
        synchronized(this){
            System.out.println("Changed namenode to " + newAddress);
            namenodeAddress = newAddress;
        }

    }

    private synchronized String getNamenodeAddress(){
        return namenodeAddress;
    }

    //========================================================================
    // File Creation
    //========================================================================

    private void attemptCreate(String filename) throws IOException{
        InputStream in= null;
        FileSystem hdfs = null;

        try{
            System.out.println("Attempt to create " + filename);
            in = new BufferedInputStream(new DeadBeefInputStream(blocks));
            String hdfsFileUri = "hdfs://" + getNamenodeAddress() + "/" +filename;
            Configuration conf = new Configuration();
            hdfs = FileSystem.get(URI.create(hdfsFileUri), conf);
            OutputStream out = hdfs.create(new Path(hdfsFileUri), true, 4096,
                    new Progressable() {

                        @Override
                        public void progress() {
                            System.out.print(".");

                        }
                    });
            long start = System.currentTimeMillis();
            IOUtils.copyBytes(in, out, 4096, true);
            long end = System.currentTimeMillis();
            long elapsedTime = (end-start)/1000;
            double throughput = blocks*DeadBeefInputStream.HDFS_BLOCK/ONE_MB/((double)elapsedTime);
            System.out.println(String.format(Locale.ENGLISH,"\n%d client: hostName=%s, event=write, readTime=0, readThroughput=0, writeTime=%d, writeThroughput=%f",
            		System.currentTimeMillis(),hostname,elapsedTime,throughput));


        }finally{
            if(hdfs != null){
                hdfs.close();
            }
        }
    }

    private void create(String filename) throws InterruptedException, IOException{
        System.out.println("Creating " + filename);
        for(int i=1; i<=MAX_ATTEMPTS;i++){
            try{
                attemptCreate(filename);
                return;
            }catch (IOException e){
                System.out.println("IOException when creating the file  "
                        + namenodeAddress + "on Attempt " + i);
                e.printStackTrace();
                // Wait some time to try againg
                Thread.sleep(5000);
            }
        }
            //If we reach here something bad happened
            throw new IOException("Could not create file " + filename) ;

    }

    //========================================================================
    // File listing
    //========================================================================

    private void attemptFileListing() throws IOException{
        System.out.println("Attempt to list root");
        String hdfsFileUri = "hdfs://" + getNamenodeAddress() + "/";
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create(hdfsFileUri), conf);
        FileStatus[] fss = hdfs.listStatus(new Path(hdfsFileUri));
        for (FileStatus fs:fss){
            System.out.println(fs.getPath().toString());
        }
    }

    private void fileListing() throws InterruptedException, IOException{
        System.out.println("Listing files ");
        for(int i=1; i<=MAX_ATTEMPTS;i++){
            try{
                attemptFileListing();
                return;
            }
            catch (IOException e){
                System.out.println("IOException when listing files on "
                        + namenodeAddress + "on Attempt " + i);
                e.printStackTrace();
                // Wait some time to try againg
                Thread.sleep(5000);
            }
        }
        //If we reach here something bad happened
        throw new IOException("Could not list files");
    }

    //========================================================================
    // File deletion
    //========================================================================

    private void attemptDelete(String filename) throws IOException{
        System.out.println("attempt to Delete " + filename);
        String hdfsFileUri = "hdfs://" + getNamenodeAddress() + "/" + filename;
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create(hdfsFileUri), conf);
        hdfs.delete(new Path(hdfsFileUri),true);
    }

    private void delete(String filename) throws InterruptedException, IOException{
        System.out.println("Deleting " + filename);
        for(int i=1; i<=MAX_ATTEMPTS;i++){
            try{
                attemptDelete(filename);
                return;
            }
            catch (IOException e){
                System.out.println("IOException when deleting file on "
                        + namenodeAddress + "on Attempt " + i);
                e.printStackTrace();
                // Wait some time to try againg
                Thread.sleep(5000);
            }
        }
        //If we reach here something bad happened
        throw new IOException("Could not delete file " +filename);
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
            Configuration conf = new Configuration();
            hdfs = FileSystem.get(URI.create(hdfsFileUri), conf);

            InputStream in = hdfs.open(new Path(hdfsFileUri), 4096);
            long start = System.currentTimeMillis();
            IOUtils.copyBytes(in, out, 4096, true);// has a finally to close the streams
            long end = System.currentTimeMillis();
            long elapsedTime = (end-start)/1000;
            double throughput = blocks*DeadBeefInputStream.HDFS_BLOCK/ONE_MB/((double)elapsedTime);
            System.out.println(String.format(Locale.ENGLISH,"\n%d client: hostName=%s, event=read, readTime=%d, readThroughput=%f, writeTime=0, writeThroughput=0",
            		System.currentTimeMillis(),hostname,elapsedTime,throughput));
        } finally {
            if(hdfs != null){
                hdfs.close();
            }
        }
    }


    private void read(String filename) throws InterruptedException, IOException{
        System.out.println("Reading " + filename);
        for(int i=1; i<=MAX_ATTEMPTS;i++){
            try{
                attemptRead(filename);
                return;
            }
            catch (IOException e){
                System.out.println("IOException when reading file on "
                        + namenodeAddress + "on Attempt " + i);
                e.printStackTrace();
                // Wait some time to try againg
                Thread.sleep(5000);
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
            if(shouldDelete) {delete(filename);}
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
            boolean shouldDelete = true;
            if(args.length == 6 && args[5].equalsIgnoreCase("false")){
                shouldDelete = false;
            }

            NamenodeLookup nnlkp = NamenodeLookupFactory.create(nnLookupType, primaryNamenode, zookeeper);
            ClientTest cli = new ClientTest(hostname,nnlkp,blocks,files,shouldDelete);
            cli.run();
        }else{
            System.out.println("Client received wrong arguments");
        }

        System.out.println(String.format("\n%d client: hostName=%s, event=end, readTime=0, readThroughput=0, writeTime=0, writeThroughput=0",
        		System.currentTimeMillis(),hostname));

    }
}
