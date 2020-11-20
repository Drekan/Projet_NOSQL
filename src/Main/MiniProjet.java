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


        //II. Définition des DataStructure
        DataStructure dataStructure = new DataStructure(options);

        dataStructure.createDico(false);

        dataStructure.createIndexes();

        //III. Partie solveur
        Solveur solveur = new Solveur(dataStructure, options);
        solveur.traiterQueries();

    }

}