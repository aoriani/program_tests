import java.io.IOException;
import java.io.OutputStream;


public class NullOutputStream extends OutputStream {

	private static final int ONE_MB = 1024 * 1024;

	int bytes = 0;

	@Override
	public void write(int arg0) throws IOException {
		bytes =  (++bytes % ONE_MB);
		if(bytes == 0)
			System.err.print(".");
	}

}
