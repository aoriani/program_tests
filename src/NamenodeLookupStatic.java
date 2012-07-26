
public class NamenodeLookupStatic implements NamenodeLookup {

	private final String namenodeAddress;
	private NamenodeChangedListener listener;

	public NamenodeLookupStatic(String nnAddr){
		namenodeAddress = nnAddr;
	}

	@Override
	public String getNamenodeAddress() {
		return namenodeAddress ;
	}

	@Override
	public void setListener(NamenodeChangedListener listener) {
		this.listener = listener;

	}

	@Override
	public NamenodeChangedListener getListener() {
		return listener;
	}

	@Override
	public void shutdown() throws InterruptedException {
		// Do nothing

	}
}
