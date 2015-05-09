package nosql.key_value.redis;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import redis.clients.jedis.Jedis;

import com.google.gson.Gson;

public class JedisDB {

	private static Jedis dbConnector;

	
	public JedisDB(String url, int port) {
  
	  dbConnector=new Jedis(url, port);
	}
	

	public void insertCity(City city) {
		Map<String, String> cityProperties = new HashMap<String, String>();
		cityProperties.put("_id", String.valueOf(city.get_Id()));
		cityProperties.put("name", city.getCity());
		cityProperties.put("loc", city.getLoc().toString());
		cityProperties.put("pop", String.valueOf(city.getPop()));
		cityProperties.put("state", city.getState());

		dbConnector.hmset("city:" + city.get_Id(), cityProperties);

	}
    private void insertCitynameAsListOfPLZ(City city ) {
    	dbConnector.rpush(city.getCity(), String.valueOf(city.get_Id()));
    }
	public City getCityByPLZ(int _id) {
		Map<String, String> city = dbConnector.hgetAll("city:"
				+ String.valueOf(_id));

		return getCityFromMap(city);
	}

	public City getCityByName(String cityName) {
		Map<String, String> city = dbConnector.hgetAll("name:" + cityName);
		System.err.println("ljdkkjd " + city.size());
		return getCityFromMap(city);
	}

	public void fillDatabase() {
		Gson gson = new Gson();
		List<City> cities = new ArrayList<>();

		try (Scanner sc = new Scanner(new File("plz.data"))) {
			while (sc.hasNextLine()) {

				String jsonObj = (String) sc.nextLine();

				System.out.println(jsonObj);
				City city = gson.fromJson(jsonObj, City.class);
				cities.add(city);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		for (Iterator<City> iterator = cities.iterator(); iterator.hasNext();) {
			City city = (City) iterator.next();
			insertCity(city);
			insertCitynameAsListOfPLZ(city);
//			removeList(city.getCity(), String.valueOf(city.get_Id()));
		}
	}

	public City getCityFromMap(Map<String, String> properties) {
		City city = new City();
		if(properties.size()!=0) {
		city.set_Id(Integer.valueOf(properties.get("_id")));
		city.setCity(properties.get("name"));
		city.setState(properties.get("state"));
		}else {
			return null;
		}
		return city;
	}
	public List<String> getListFromCityname(String city_name){
		long listLenght = dbConnector.llen(city_name);
		if(listLenght<1) {
			return new ArrayList<String>();
		}
		return dbConnector.lrange(city_name, 0, listLenght-1);
	}
	public void insertCityUsingCityNameAsID(City city) {
		dbConnector.set(city.getCity(), String.valueOf(city.get_Id()));		
	}
	public void close() {
		dbConnector.close();
	}
	public void removeList(String list, String value) {
		dbConnector.lrem(list, 0, value);
	}
}
