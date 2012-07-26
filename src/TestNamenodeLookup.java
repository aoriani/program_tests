import org.apache.log4j.Logger;


public class TestNamenodeLookup {

	private static final Logger LOG = Logger.getLogger(TestNamenodeLookup.class);

	public static void main(String[] args) throws InterruptedException{
		final NamenodeLookup lookup = new NamenodeLookupZooKeeper("127.0.0.1");
		LOG.info("Current namenode is at" + lookup.getNamenodeAddress());
		lookup.setListener(new NamenodeChangedListener() {

			@Override
			public void namenodeChanged(String newAddress) {
				LOG.info("Namenode changed to " +newAddress + "-" + lookup.getNamenodeAddress());

			}

		});

		Thread.sleep(Long.MAX_VALUE);
	}

}
