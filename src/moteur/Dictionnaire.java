package moteur;
import java.util.HashMap;

public class Dictionnaire {
	
	private HashMap<Integer,String> dictionary;
	
	public Dictionnaire() {
		this.dictionary = new HashMap<Integer,String>();
	}
	
	public void add(String value) {
		this.dictionary.put(value.hashCode(),value);
	}
	
	public int getSize() {
		return this.dictionary.size();
	}
	
	public String getValue(int index) {
		return this.dictionary.get(index);
	}

}
