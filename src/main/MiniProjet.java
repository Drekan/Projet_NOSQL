package main;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;

import org.openrdf.model.Statement;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.RDFHandlerBase;

import moteur.*;

public class MiniProjet {

    public static void main(String[] args) throws IOException, MalformedQueryException {
        String optionsLine = ""; // TODO: à supprimer et remplacer par args
        //I. Définition des options
        Options options = new Options(optionsLine);

        //activation du mode verbose
        options.setVerbose(true);
        
        //à commenter
        options.setDataPath("datasets/1M.rdfxml.rdf");
        
        Statistics statistiques = new Statistics(options);

        options.setJena(true);



        //II. Définition des DataStructure
        DataStructure dataStructure = new DataStructure(options,statistiques);

        dataStructure.createDico(false);

        dataStructure.createIndexes();

        if(options.getExport_query_stats()){

        }

        //III. Partie solveur
        Solveur solveur = new Solveur(dataStructure, options, statistiques);


        solveur.traiterQueries();


        //pas sure que 2 stats se soit ok

        //A faire dans les classes
        if(options.getExport_query_stats()){
            dataStructure.getStats().writeStats();
            solveur.getStats().writeStats();
        }
    }

}