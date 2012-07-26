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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;



public class FlushTest {

	public static void main(String... args){
		
		InputStream in=null;
		FileSystem hdfs = null;
		FSDataOutputStream out = null;
		
		try {
			in = new BufferedInputStream(new DeadBeefInputStream(1000));
			String hdfsUri = "hdfs://127.0.0.1/testfile";
			Configuration conf= new Configuration();
			hdfs = FileSystem.get(URI.create(hdfsUri),conf);
			out = hdfs.create(new Path(hdfsUri),true,4096,null);
		
		outer:
			while (true) {
				for (long byteCount=0; byteCount< 2* DeadBeefInputStream.HDFS_BLOCK; ++byteCount) {
					int data = in.read();
					if (data != -1) {
						out.write(data);
					}else {
						break outer;
					}
				}
				out.flush();
				out.sync();
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			try {
				if (hdfs != null) {
					hdfs.close();
				}
				
				if (in != null) {
					in.close();
				}
				
				if (out != null) {
					out.close();
				}
			}
			catch (Exception e) {
				e.printStackTrace();
			}
			
		}
		
		
	}

}