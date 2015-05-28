package nosql.documentdb.mongodb;



public class Main {
	
	public static final String DATABASENAME = "plz";
	public static final String COLLECTIONNAME = "plz";

	public static void main(String[] args) {
		Programm p = new Programm();
		p.start();
//		MongoDB dbClient = new MongoDB ("141.22.29.87", 27017);
//		dbClient.setDatabase(DATABASENAME);
//	//	dbClient.fillDatabase(COLLECTIONNAME); //nur einmal
//		
//		
//        City c = dbClient.getCityByPlz(COLLECTIONNAME, 99722);
//		System.out.println("State: " + c.getState() + "  City: " + c.getCity());
//		
//		List<City> list  = dbClient.getPLZsForCityName(COLLECTIONNAME, "HAMBURG");
//		for (City cur : list) {
//			System.out.println(cur);
//		}
//		
//		List<City> list2  = dbClient.getPLZsForCityName(COLLECTIONNAME, "TUMTUM");
//		for (City cur : list2) {
//			System.out.println(cur);
//		}
//		
//		//System.out.println(c.getCity() + " " + c.getState() );
	}

}
