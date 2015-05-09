package nosql.key_value.redis;

import java.util.List;
import java.util.Scanner;

public class Programm {

	Scanner scanner = new Scanner(System.in);
	private JedisDB jedisClient = new JedisDB("192.168.33.30", 6379);

	public static final String PLZ_FEHLER = "Sie haben eine invalide PLZ eingegeben. Bitte versuchen Sie es noch einmal!";
	public static final String EINGABE_FEHLER = "Sie haben eine invalide Eingabe gemacht. Bitte versuchen Sie es noch einmal!";
	public static final String CITYNAME_FEHLER = "Sie haben eine invalide Stadts Name eingegeben. Bitte versuchen Sie es noch einmal!";
	public static final String PLZ_NICHT_GEFUNDEN_FEHLER = "Die Stadt mit dieser PLZ existiert nicht in der Datenbank. Bitte versuchen Sie es noch einmal!";
  
	public Programm() {
//		jedisClient.fillDatabase();
		
	}

	public String textEingeben(String aufforderung) {

		System.out.print(aufforderung + "> ");
		String eingabe = scanner.nextLine();

		System.out.println("Ausgabe: " + eingabe);
		return eingabe;
	}

	public void eingabeValidieren(String modus, String eingabe) {

		System.out.println("Modus: " + modus + " Eingeben ist: '" + eingabe
				+ "'");
		switch (modus) {
		case "p":
			System.out.println("Es wird nach PLZ gefragt");
			gibCityAndLand(eingabe);
			;
			break;
		case "c":
			System.out.println("Es wird nach Cityname gefragt");
			gibPLZZuCity(eingabe);
			break;
		case "q":
			System.out.println("Beenden...");
			scanner.close();
			jedisClient.close();
			System.exit(0);

			break;
		default:
			System.out.println(EINGABE_FEHLER);
			
			break;
		}

	}

	private List<String> gibCityAndLand(String plz) {
		try {
			int plznb = Integer.valueOf(plz);
			City city = jedisClient.getCityByPLZ(plznb);
			System.out.println(city==null);
			if(city==null) {
				System.out.println(PLZ_NICHT_GEFUNDEN_FEHLER);
			}else {
				System.out.println("Eine Stadt wurde gefunden: "+ city.getCity()+ " in " + city.getState());
			}
		}  catch (Exception e) {
			System.out.println(PLZ_FEHLER);

		}
		return null;
	}

	private int gibPLZZuCity(String cityName) {
		String upperCaseCityName = cityName.toUpperCase();
		System.out.println("Gesucht wird nach: '" + upperCaseCityName+"'");
		List<String> list= jedisClient.getListFromCityname(upperCaseCityName);
		System.out.println("Gefunden PLZ für " + cityName + ": " +list.toString());
		return 0;
	}

	public void start() {
		while (true) {
			String eingabe = textEingeben("Bitte geben Sie p und gewünsche Postleitzahl ein. Beipiel: 'p 12345' \n"
					+ "Bitte geben Sie c und Name der gesuchten Stadt ein. Beipiel: 'c hamburg'  \n"
					+ "Geben Sie 'q' ein, um das Programm zu beenden \n");
			String modus = eingabe.substring(0, 1);
			String gesuchtesFeld = "";
			if (eingabe.split(" ").length > 1) {
				gesuchtesFeld = eingabe.substring(2);
			}
			eingabeValidieren(modus, gesuchtesFeld);
		}
	}
}
