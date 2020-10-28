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
		
		public int valeur = 0;
		
		public Dictionnaire dico;
		
		public RDFListener(Dictionnaire d) {
			super();
			this.dico = d;
		}
		
		
		@Override
		public void handleStatement(Statement st) {
			//this.trace.add(st.getSubject().toString());
			this.valeur+=3;
			this.dico.add(st.getSubject().toString());
			this.dico.add(st.getPredicate().toString());
			this.dico.add(st.getObject().toString());

		}

	};

	public static void main(String args[]) throws FileNotFoundException {

		Reader reader = new FileReader("datasets/100K.rdfxml");

		org.openrdf.rio.RDFParser rdfParser = Rio.createParser(RDFFormat.RDFXML);
		
		Dictionnaire d = new Dictionnaire();
		
		RDFListener rdf_l = new RDFListener(d);
		
		
		rdfParser.setRDFHandler(rdf_l);
		try {
			System.out.println("La taille du dictionnaire est de " + d.getSize());
			System.out.println("Maintenant, on parse");
			rdfParser.parse(reader, "");
			System.out.println("La taille du dictionnaire est de " + d.getSize());
			System.out.println("Nombre de ressources lues : " + rdf_l.valeur);

		} catch (Exception e) {

		}

		try {
			reader.close();
		} catch (IOException e) {
		}

	}

}