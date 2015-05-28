package nosql.documentdb.mongodb;


import static com.mongodb.client.model.Filters.eq;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import nosql.key_value.redis.City;

import org.bson.Document;

import com.google.gson.Gson;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class MongoDB {
	
	public MongoClient mongoClient;
	public MongoDatabase db;
	
	public MongoDB (String IP, int port){
		mongoClient = new MongoClient (IP,port);
	}
	
	public void setDatabase(String name){
		db = mongoClient.getDatabase(name);
	}
	
	public MongoCollection<Document> getCollection (String name){
		return db.getCollection(name);
	}
	
	public Document createDocumentofCity(City city){
		Document doc = new Document("_id", String.valueOf(city.get_Id()))
								.append("name", city.getCity())
								.append("loc", city.getLoc())
//								.append("loc",
//										new Document("x", String.valueOf(city.getLoc()[0]))
//												.append("y", String.valueOf(city.getLoc()[1]))) 
								.append("pop", String.valueOf(city.getPop()))
								.append("state", city.getState());
		return doc;
	}
	

	public void fillDatabase(String collectionName) {
		Gson gson = new Gson();
		List<City> cities = new ArrayList<>();

		try (Scanner sc = new Scanner(new File("plz.data"))) {
			while (sc.hasNextLine()) {

				String jsonObj = (String) sc.nextLine();
				//System.out.println(jsonObj);
				City city = gson.fromJson(jsonObj, City.class);
				cities.add(city);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		for (Iterator<City> iterator = cities.iterator(); iterator.hasNext();) {
			City city = (City) iterator.next();
			getCollection(collectionName).insertOne(createDocumentofCity(city));
//			removeList(city.getCity(), String.valueOf(city.get_Id()));
		}
	}
	
	public long CountCollection(String collectionName){
		return getCollection(collectionName).count();
	}
	
	
	public String findAllDocumentsInCollection(String collectName){
		MongoCursor<Document> cursor = getCollection(collectName).find().iterator();
		try {
		    while (cursor.hasNext()) {
		        System.out.println(cursor.next().toJson());
		    }
		} finally {
		    cursor.close();
		}
		return "";
	}
	
	
	
	private List<Document> findPlzByCityName(String collectName, String name){
		List<Document> list = new ArrayList<Document>();
		for (Document cur : getCollection(collectName).find(eq("name",name))) {
		    list.add(cur);
		}
		
		return list;
	}
	
	public List<City> getPLZsForCityName(String collectName, String name){
		List<Document> list = findPlzByCityName(collectName, name);
		List<City> cities = new ArrayList<City>();
		for (Document doc : list) {
			cities.add(DocumentToCity(doc));
		}
		return cities;
	}
	
	
	private Document findCityByPlz(String collectName, int id){
		return getCollection(collectName).find(eq("_id", String.valueOf(id))).first();
	}
	
	//TODO Loc Fehler beim Parsen von Json. City wird erstellt aber ohne LocEintrag
	private City DocumentToCity (Document doc){
		Gson gson = new Gson();
	//	City city = gson.fromJson(doc.toJson(), City.class);
	//	City city = (City) JSON.parse(doc.toJson());
		
		City city = new City();
		city.set_Id(Integer.valueOf(doc.getString("_id")));
		city.setCity(doc.getString("name"));
	//	System.out.println(doc.getString("loc")); 
		city.setPop(Long.valueOf(doc.getString("pop")));
		city.setState(doc.getString("state"));
		return city;
	}
	
	public City getCityByPlz (String collectName, int id){
		return DocumentToCity(findCityByPlz(collectName, id));
	}
	
}
