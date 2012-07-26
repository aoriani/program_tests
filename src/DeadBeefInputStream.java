import java.io.IOException;
import java.io.InputStream;


public class DeadBeefInputStream extends InputStream {

	public static final long HDFS_BLOCK = 64 * 1024 * 1024;
	public static final int EOF = -1;

	private long bytesToProduce;
	private long bytesProduced;


	public DeadBeefInputStream(int hdfsBlocks){
		bytesToProduce = hdfsBlocks * HDFS_BLOCK;
		bytesProduced = 0L;
	}

	@Override
	public int read() throws IOException {
		int data = 0;
		if(bytesProduced < bytesToProduce){
			switch ((int)(bytesProduced % 4)){
				case 0: data = 0xde;break;
				case 1: data = 0xad;break;
				case 2: data = 0xbe;break;
				case 4: data = 0xef;break;
			}
			++bytesProduced;
		}else{
			data = EOF;
		}

		return data;
	}

}
