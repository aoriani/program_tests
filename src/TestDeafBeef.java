import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;


public class TestDeafBeef {

	public static void main(String... args) throws IOException{
		BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream("teste"));
		BufferedInputStream in = new BufferedInputStream(new DeadBeefInputStream(2));

		try{
			int data = in.read();
			while(data != -1){
				out.write(data);
				data = in.read();
			}
		}finally{
			out.close();
			in.close();
		}

	}
}
