public interface NamenodeLookup {

	public static final int STATIC = 1;
	public static final int  HA = 2;

	public abstract String getNamenodeAddress();

	public abstract void setListener(NamenodeChangedListener listener);

	public abstract NamenodeChangedListener getListener();

	public abstract void shutdown() throws InterruptedException;

}