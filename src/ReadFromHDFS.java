import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;


public class ReadFromHDFS implements NamenodeChangedListener{
    private static final Logger LOG = Logger.getLogger(ReadFromHDFS.class);

    private volatile String namenodeAddress;
    private String hdfsFile;
    private NamenodeLookup lookup;

    @Override
    public void namenodeChanged(String newAddress) {
        namenodeAddress = newAddress;
        LOG.info("Namenode changed to "  + newAddress);
    }


    private void read() throws IOException {
    	OutputStream out = new NullOutputStream();
        FileSystem hdfs = null;

        try {

            String hdfsFileUri = "hdfs://" + namenodeAddress + hdfsFile;
            Configuration conf = new Configuration();
            hdfs = FileSystem.get(URI.create(hdfsFileUri), conf);

            InputStream in = hdfs.open(new Path(hdfsFileUri), 4096);
            LOG.info("Reading " +  hdfsFileUri);
            IOUtils.copyBytes(in, out, 4096, true);// has a finally to close the streams
        } finally {
            if(hdfs != null){
                hdfs.close();
            }
        }
    }

    private void run() throws InterruptedException{
        final int MAX_ATTEMPTS = 10;
        for (int i = 1; i <= MAX_ATTEMPTS; i++) {
            try {
                read();
                // Copy succeed, get out the loop
                return;
            } catch(FileNotFoundException e) {
            	LOG.fatal("The file " + hdfsFile + " does not exist");
            	System.exit(1);
            } catch (IOException e) {
                LOG.warn("IOException when reading the file from "
                        + namenodeAddress + "on Attempt " + i, e);
                // Wait some time to try againg
                Thread.sleep(5000);
            }
        }

        //If we reach here something bad happened.
        System.exit(1);

    }


    public ReadFromHDFS(String zooConnString,String hdfsFile){
        this.hdfsFile = hdfsFile;
        this.lookup = new NamenodeLookupZooKeeper(zooConnString);
        this.namenodeAddress = lookup.getNamenodeAddress();
        lookup.setListener(this);
    }

    public static void main(String... args) throws InterruptedException{
        if(args.length != 2){
            LOG.fatal("Missing parameters");
            System.exit(1);
        }
        ReadFromHDFS reader = new ReadFromHDFS(args[0], args[1]);
        reader.run();

    }


}
