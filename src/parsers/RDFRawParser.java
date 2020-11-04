package parsers;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.RDFHandlerBase;

import moteur.Dictionnaire;
import moteur.Index;

public final class RDFRawParser {

	public static class RDFListener extends RDFHandlerBase {

		public int ressourcesNum = 0;
		public Dictionnaire dico;

		public RDFListener(Dictionnaire d) {
			super();
			this.dico = d;
		}

		@Override
		public void handleStatement(Statement st) {
			String subject = st.getSubject().toString() ;
			String predicate = st.getPredicate().toString();
			String object = st.getObject().toString();

			this.ressourcesNum+=3;
			this.dico.add(subject,predicate,object);
		}
	};


	public static void main(String args[]) throws FileNotFoundException {

		Reader reader = new FileReader("datasets/100K.rdfxml");
		org.openrdf.rio.RDFParser rdfParser = Rio.createParser(RDFFormat.RDFXML);

		Dictionnaire d = new Dictionnaire();
		ArrayList<Index> indexes = new ArrayList<>();

		indexes.add(new Index("spo"));
		indexes.add(new Index("sop"));
		indexes.add(new Index("pos"));
		indexes.add(new Index("pso"));
		indexes.add(new Index("osp"));
		indexes.add(new Index("ops"));

		RDFListener rdf_l = new RDFListener(d);

		rdfParser.setRDFHandler(rdf_l);
		try {
			System.out.println("Parsing des données...");

			rdfParser.parse(reader, "");

			System.out.println("Voulez-vous créer un dictionnire trié ou non ? (y/N)");
			Scanner sc = new Scanner(System.in);
			String s = sc.nextLine();
			if(s.equals("y")) {
				System.out.println("Création du dictionnaire trié...");
				d.createDico(true);
			}
			else {
				System.out.println("Création du dictionnaire non trié...");
				d.createDico(false);
			}

			System.out.println("La taille du dictionnaire est de " + d.getSize());
			System.out.println("Nombre total de ressources lues : " + rdf_l.ressourcesNum);

			System.out.println("Création des index...");
			ArrayList<String[]> tuples = d.getTuples();
			for(Index index : indexes) {
				for(int i=0;i<tuples.size();i++) {
					index.add(d.getValue(tuples.get(i)[0]),d.getValue(tuples.get(i)[1]),d.getValue(tuples.get(i)[2]));
					//System.out.println(d.getValue(tuples.get(i)[0])+" "+d.getValue(tuples.get(i)[1])+" "+d.getValue(tuples.get(i)[2]));
				}
			}

			tuples.clear();		

			System.out.println("Fin du programme");

			//Affichage du début de l'index SOP
			Index sop = indexes.get(0);
			System.out.println("Index SOP : (10 premières valeurs)");
			HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> myMap = sop.getIndex();
			Set listKeys=myMap.keySet();  // Obtenir la liste des clés
			Iterator iterateur=listKeys.iterator();
			int i = 0;
			while(i<10 && iterateur.hasNext())
			{
				Object key= iterateur.next();
				System.out.println (key+"=>");

				HashMap<Integer, ArrayList<Integer>> mySousMap = myMap.get(key);

				Set listSousKeys=mySousMap.keySet();  // Obtenir la liste des clés
				Iterator sousIterateur=listSousKeys.iterator();

				while(i<10 && sousIterateur.hasNext()) {
					Object souskey= sousIterateur.next();
					System.out.println ("       "+souskey+"=>"+mySousMap.get(souskey).toString());
					i++;
				}
			}


		} catch (Exception e) {

		}

		try {
			reader.close();
		} catch (IOException e) {}

	}

}