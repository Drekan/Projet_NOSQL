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
			System.out.println("Parsing des donn�es...");
			
			long startTime_d = System.nanoTime();
			rdfParser.parse(reader,"");
			long timeSpent_d = System.nanoTime() - startTime_d;
			
			
			System.out.println("Voulez-vous cr�er un dictionnire tri� ou non ? (y/N)");
			Scanner sc = new Scanner(System.in);
			String s = sc.nextLine();
			if(s.equals("y")) {
				System.out.println("Cr�ation du dictionnaire tri�...");
				
				startTime_d = System.nanoTime();
				d.createDico(true);
				timeSpent_d += System.nanoTime() - startTime_d;
			}
			else {
				System.out.println("Cr�ation du dictionnaire non tri�...");
				
				startTime_d = System.nanoTime();
				d.createDico(false);
				timeSpent_d += System.nanoTime() - startTime_d;
			}

			System.out.println("La taille du dictionnaire est de " + d.getSize());
			System.out.println("Nombre total de ressources lues : " + rdf_l.ressourcesNum);

			System.out.println("Cr�ation des index...");
			ArrayList<String[]> tuples = d.getTuples();
			
			long startTime_i = System.nanoTime();
			for(Index index : indexes) {
				for(int i=0;i<tuples.size();i++) {
					index.add(d.getValue(tuples.get(i)[0]),d.getValue(tuples.get(i)[1]),d.getValue(tuples.get(i)[2]));
					//System.out.println(d.getValue(tuples.get(i)[0])+" "+d.getValue(tuples.get(i)[1])+" "+d.getValue(tuples.get(i)[2]));
				}
			}
			long timeSpent_i = System.nanoTime() - startTime_i;

			tuples.clear();		

			System.out.println("Fin du programme.");
			
			System.out.println("\n---Statistiques---");
			System.out.println("Temps de génération du dictionnaire : " + timeSpent_d/1000000 + "ms");
			System.out.println("Temps de génération de l'index : " + timeSpent_i/1000000 + "ms");
			System.out.println("Temps total : " + (timeSpent_i+timeSpent_d)/1000000 + "ms\n");

			//Affichage du d�but de l'index SOP
			Index sop = indexes.get(0);
			System.out.println("Index SOP : (10 premi�res valeurs)");
			HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> myMap = sop.getIndex();
			Set listKeys=myMap.keySet();  // Obtenir la liste des cl�s
			Iterator iterateur=listKeys.iterator();
			int i = 0;
			while(i<10 && iterateur.hasNext())
			{
				Object key= iterateur.next();
				System.out.println (key+"=>");

				HashMap<Integer, ArrayList<Integer>> mySousMap = myMap.get(key);

				Set listSousKeys=mySousMap.keySet();  // Obtenir la liste des cl�s
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