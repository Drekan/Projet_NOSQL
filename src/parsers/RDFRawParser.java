package parsers;
import moteur.*;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.RDFHandlerBase;

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
			
			//this.trace.add(st.getSubject().toString());
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
		
		ArrayList<String> test = new ArrayList<>();
		
		System.out.println(test);
		
		
		rdfParser.setRDFHandler(rdf_l);
		try {
			System.out.println("La taille du dictionnaire est de " + d.getSize());
			System.out.println("Maintenant, on parse");
			
			rdfParser.parse(reader, "");
			
			d.createDico();
			
			System.out.println("La taille du dictionnaire est de " + d.getSize());
			System.out.println("Nombre de ressources lues : " + rdf_l.ressourcesNum);
			
			
			
			ArrayList<String[]> tuples = d.getTuples();
			for(Index index : indexes) {
				for(int i=0;i<tuples.size();i++) {
					index.add(d.getValue(tuples.get(i)[0]),d.getValue(tuples.get(i)[1]),d.getValue(tuples.get(i)[2]));
				}
			
				
			}
			
			tuples.clear();
			
			System.out.println("taille de tuples : "+d.getTuples().size());
			

			Index sop = indexes.get(0);
			System.out.println("Index SOP : (10 premières valeurs)");
			for(int i=0;i<10;i++) {
				int[] triple = sop.getTriple(i);
				System.out.println("["+triple[0]+","+triple[1]+","+triple[2]+"]");
			}

		} catch (Exception e) {

		}

		try {
			reader.close();
		} catch (IOException e) {
		}

	}

}