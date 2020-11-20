package moteur;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;

import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.Rio;
import org.openrdf.rio.RDFParser;

import parsers.RDFRawParser.RDFListener;

public class DataStructure {
    private Dictionnaire dico;
    private HashMap<String,Index> indexes;

    private Statistics stats;
    private Options opt;

    public DataStructure(Options options) {
        this.opt = options;

        this.stats = new Statistics(opt.getDataPath(),opt.getQueriesPath(),opt.getOutputPath());

        this.indexes = new HashMap<>();
    }

    public Dictionnaire getDico() {
        return dico;
    }

    public HashMap<String,Index> getIndexes() {
        return indexes;
    }

    public Statistics getStats() {
        return stats;
    }

    public Options getOpt() {
        return opt;
    }


    //TODO : mieux gérer FileNotFoundException
    public void createDico(Boolean triLexicographique) throws FileNotFoundException{
        this.dico = new Dictionnaire(triLexicographique);

        String verbose = "";

        RDFParser rdfParser = Rio.createParser(RDFFormat.RDFXML);
        RDFListener rdfListener = new RDFListener(this.dico);
        rdfParser.setRDFHandler(rdfListener);

        try {
            verbose+="Parsing des donn�es...\n";
            long startTime_d = System.nanoTime();
            rdfParser.parse(new FileReader(opt.getDataPath()),"");
            this.dico.createDico();
            long timeSpent_d = System.nanoTime() - startTime_d;

            stats.setRDFTripleNum(dico.getTuples().size());

            verbose+="La taille du dictionnaire est de " + dico.getSize()+"\n";
            verbose+="Nombre total de ressources lues : " + rdfListener.ressourcesNum+"\n";

            if(opt.getVerbose()) {
                System.out.println(verbose);
            }


        } catch (Exception e) {

        }
    }

    public void createIndexes() {
        String verbose = "";

        this.indexes.put("spo",new Index("spo"));
        this.indexes.put("sop",new Index("sop"));
        this.indexes.put("pos",new Index("pos"));
        this.indexes.put("pso",new Index("pso"));
        this.indexes.put("osp",new Index("osp"));
        this.indexes.put("ops",new Index("ops"));

        ArrayList<String[]> tuples = this.dico.getTuples();

        verbose+="Cr�ation des index...";
        long startTime_i = System.nanoTime();
        for(Index index : indexes.values()) {
            for(int i=0;i<tuples.size();i++) {
                index.add(this.dico.getValue(tuples.get(i)[0]),this.dico.getValue(tuples.get(i)[1]),this.dico.getValue(tuples.get(i)[2]));
            }
        }

        long timeSpent_i = System.nanoTime() - startTime_i;

        stats.setIndexesCreationTotalTime((int)timeSpent_i/1000000);

        tuples.clear();

    }

}
