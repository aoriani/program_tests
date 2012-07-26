import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class CopyToS3 {

	public static void main(String... args) throws IOException{
		final String S3_SCHEME = "s3n://";
		final String AWSID = "AKIAJTXNJM3UBEJYAXRA";
		final String AWSSECRET = "kWCtvuSY8gLL%2FXayAIc+9x1NSTiHHjqZnQjEkwUu";

		String bucket = args[0];
		String filePath = "file://" + args[1];
		String S3uri = S3_SCHEME+ AWSID+":"+AWSSECRET+"@"+bucket+"/";
        Configuration conf = new Configuration();
        FileSystem s3 = FileSystem.get(URI.create(S3uri), conf);
        s3.copyFromLocalFile(new Path(filePath), new Path(S3uri));
        s3.close();

	}

}
