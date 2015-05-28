package nosql.documentdb.mongodb;

import java.util.List;
import java.util.Scanner;

import nosql.key_value.redis.City;

/**
 * program logic and minimal console GUI
 * 
 * @author SG
 *
 */
public class Programm {

	Scanner scanner = new Scanner(System.in);
	private MongoDB hbaseClient = new MongoDB("141.22.29.87", 27017);

	public static final String PLZ_FEHLER = "Sie haben eine invalide PLZ eingegeben. Bitte versuchen Sie es noch einmal!";
	public static final String EINGABE_FEHLER = "Sie haben eine invalide Eingabe gemacht. Bitte versuchen Sie es noch einmal!";
	public static final String CITYNAME_FEHLER = "Sie haben eine invalide Stadts Name eingegeben. Bitte versuchen Sie es noch einmal!";
	public static final String PLZ_NICHT_GEFUNDEN_FEHLER = "Die Stadt mit dieser PLZ existiert nicht in der Datenbank. Bitte versuchen Sie es noch einmal!";

	public Programm() {
		// uncomment the line below , to fill database
//		hbaseClient.fillDatabase();

	}

	public String textEingeben(String aufforderung) {

		System.out.print(aufforderung + "> ");
		String eingabe = scanner.nextLine();

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
			City city = hbaseClient.getCityByPlz(Main.COLLECTIONNAME, Integer.valueOf(plz));
			if (city == null) {
				System.out.println(PLZ_NICHT_GEFUNDEN_FEHLER);
			} else {
				System.out.println("Eine Stadt wurde gefunden: "
						+ city.getCity() + " in " + city.getState());
			}
		} catch (Exception e) {
			System.out.println(PLZ_FEHLER);

		}
		return null;
	}

	private int gibPLZZuCity(String cityName) {
		String upperCaseCityName = cityName.toUpperCase();
		List<City> list = hbaseClient.getPLZsForCityName(Main.COLLECTIONNAME, upperCaseCityName);
		System.out.println("Gefunden Cities für " + cityName + ": "
				+ list.toString());
		return 0;
	}

	public void start() {
		while (true) {
			String eingabe = textEingeben(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n"
					+ "Bitte geben Sie p und gewünsche Postleitzahl ein. Beipiel: 'p 12345' \n"
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
