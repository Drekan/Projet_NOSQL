package parsers;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;

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

			System.out.println("Création du dictionnaire...");
			d.createDico();

			System.out.println("La taille du dictionnaire est de " + d.getSize());
			System.out.println("Nombre total de ressources lues : " + rdf_l.ressourcesNum);

			System.out.println("Création des index...");
			ArrayList<String[]> tuples = d.getTuples();
			for(Index index : indexes) {
				for(int i=0;i<tuples.size();i++) {
					index.add(d.getValue(tuples.get(i)[0]),d.getValue(tuples.get(i)[1]),d.getValue(tuples.get(i)[2]));
				}
			}

			tuples.clear();		
			
			System.out.println("Fin du programme");

			//Affichage du début de l'index SOP
			/*Index sop = indexes.get(0);
			System.out.println("Index SOP : (10 premières valeurs)");
			for(int i=0;i<10;i++) {
				int[] triple = sop.getTriple(i);
				System.out.println("["+triple[0]+","+triple[1]+","+triple[2]+"]");
			}*/

		} catch (Exception e) {

		}

		try {
			reader.close();
		} catch (IOException e) {}

	}

}