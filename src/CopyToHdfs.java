import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;


//FIXME: read/write locks for the namenodeaddress.



public class CopyToHdfs implements NamenodeChangedListener {

    private static final Logger LOG = Logger.getLogger(CopyToHdfs.class);

    private volatile String namenodeAddress;
    private String localFile;
    private String hdfsFile;
    private NamenodeLookup lookup;

    @Override
    public void namenodeChanged(String newAddress) {
        namenodeAddress = newAddress;
        LOG.info("Namenode changed to "  + newAddress);
    }



    private void copy() throws IOException {
        InputStream in = null;
        FileSystem hdfs = null;
        try {

            try {
                in = new BufferedInputStream(new FileInputStream(localFile));
            } catch (FileNotFoundException e) {
                LOG.fatal("The local file does not exist");
                System.exit(1);
            }
            String hdfsFileUri = "hdfs://" + namenodeAddress + hdfsFile;
            Configuration conf = new Configuration();
            hdfs = FileSystem.get(URI.create(hdfsFileUri), conf);

            OutputStream out = hdfs.create(new Path(hdfsFileUri), true, 4096,
                    new Progressable() {

                        @Override
                        public void progress() {
                            System.out.print(".");

                        }
                    });
            LOG.info("Copying " + localFile + " to " + hdfsFileUri);
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
                copy();
                // Copy succeed, get out the loop
                return;
            } catch (IOException e) {
                LOG.warn("IOException when copying the file to "
                        + namenodeAddress + "on Attempt " + i, e);
                // Wait some time to try againg
                Thread.sleep(5000);
            }
        }
        //If we reach here something bad happened.
        System.exit(1);
    }


    public CopyToHdfs(String zooConnString, String localFile, String hdfsFile){
        this.localFile = localFile;
        this.hdfsFile = hdfsFile;
        this.lookup = new NamenodeLookupZooKeeper(zooConnString);
        this.namenodeAddress = lookup.getNamenodeAddress();
        lookup.setListener(this);
    }

    public static void main(String... args) throws InterruptedException{
        if(args.length != 3){
            LOG.fatal("Missing parameters");
            System.exit(1);
        }
        CopyToHdfs copier = new CopyToHdfs(args[0], args[1] ,args[2]);
        copier.run();

    }

}
