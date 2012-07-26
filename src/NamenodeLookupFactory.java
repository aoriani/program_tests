

public class NamenodeLookupFactory {

	private NamenodeLookupFactory(){}

	public static NamenodeLookup create(int type, String namenode, String zookeeper ){
		switch(type){
			case NamenodeLookup.STATIC:
				return new NamenodeLookupStatic(namenode);
			case NamenodeLookup.HA:
				return new NamenodeLookupZooKeeper(zookeeper);
			default:
				return new NamenodeLookupStatic(namenode);
		}
	}

}
