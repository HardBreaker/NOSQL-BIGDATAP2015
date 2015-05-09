package nosql.key_value.redis;

import java.util.Arrays;

public class City {
	private int _id;
	private String city;
	private double [] loc;
	private long pop;
	private String state;
	
	public int get_Id() {
		return _id;
	}
	public void set_Id(int id) {
		this._id = id;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public double[] getLoc() {
		return loc;
	}
	public void setLoc(double[] loc) {
		this.loc = loc;
	}
	public long getPop() {
		return pop;
	}
	public void setPop(long pop) {
		this.pop = pop;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	@Override
	public String toString() {
		return "City [id=" + _id + ", city=" + city + ", loc="
				+ Arrays.toString(loc) + ", pop=" + pop + ", state=" + state
				+ "]";
	}

}
