package moteur;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.jena.shared.NotFoundException;

public class Dictionnaire {
	//TODO: mettre en cache ?

	/**
	 * Donn�es non tri�es sous forme de tuples pour cr�er les index
	 */
	private ArrayList<String[]> tuples;

	/**
	 * Donn�es tri�es lexicographiquement
	 */
	private ArrayList<String> sortedRessources;
	private Boolean lexicographical_sort;
	private HashMap<Integer,String> intToString_dico;
	private HashMap<String,Integer> stringToInt_dico;


	public Dictionnaire(Boolean sort) {
		this.intToString_dico = new HashMap<>();
		this.stringToInt_dico = new HashMap<>();
		this.sortedRessources = new ArrayList<>();
		this.tuples = new ArrayList<>();
		this.lexicographical_sort = sort;
	}


	public void addDistinct(String s) {
		if(!sortedRessources.contains(s)) {
			this.sortedRessources.add(s);	
		}
	}

	/**
	 * Ajoute des donn�es dans les ArrayList tuples et sortedRessources
	 */
	public void add(String s,String p,String o) {
		this.tuples.add(new String[] {s,p,o});
		if(this.lexicographical_sort) {
			addDistinct(s);
			addDistinct(p);
			addDistinct(o);
		}
		else {
			this.intToString_dico.put(s.hashCode(),s);
			this.stringToInt_dico.put(s,s.hashCode());

			this.intToString_dico.put(p.hashCode(),p);
			this.stringToInt_dico.put(p,p.hashCode());

			this.intToString_dico.put(o.hashCode(),o);
			this.stringToInt_dico.put(o,o.hashCode());
		}

	}

	/**
	 * Remplissage des deux hashMap à partir des données triees
	 * 	Le tri sert à conserver l'ordre lexicographique
	 */
	public void createDico() {

		if(this.lexicographical_sort)
			this.sortedRessources.sort(null);

		for(int i=0; i<this.sortedRessources.size();i++) {
			this.intToString_dico.put(i,this.sortedRessources.get(i));
			this.stringToInt_dico.put(this.sortedRessources.get(i),i);
		}
		this.sortedRessources.clear();
	}


	public int getSize() {
		return this.intToString_dico.size();
	}

	public String getValue(int index) {
		if(this.intToString_dico.containsKey(index))
			return this.intToString_dico.get(index);
		else
			throw new NotFoundException("String Dictionnaire.getValue (int)");
	}

	public int getValue(String value) {
		if(this.stringToInt_dico.containsKey(value))
			return this.stringToInt_dico.get(value);
		else
			throw new NotFoundException("Int Dictionnaire.getValue (String)");
	}

	public String[] getTuple(int i) {
		return this.tuples.get(i);
	}

	public ArrayList<String[]> getTuples(){
		return this.tuples;
	}

}
