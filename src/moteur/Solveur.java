package moteur;

/*
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.util.FileManager;
 */
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.*;

import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

public class Solveur {
    //TODO: gérer l'UTF-8

    private Dictionnaire dictionnaire;

    private HashMap<String, Index> indexes;

    private Options options;

    private Statistics stats;

    /*
     * indexMap : structure qui permet de savoir quel index utiliser en fonction du pattern que l'on a.
     *
     * Exemple :
     * 12 <=> p et o ont une valeur connue, on utilise donc un index pos
     * 0 <=> s a une valeur connue, on utilise donc un index spo
     */
    private HashMap<String,String> indexMap;


    public Solveur(DataStructure dataStructure, Options options){
        this.indexMap = new HashMap<>();
        this.options = options;
        this.dictionnaire = dataStructure.getDico();
        this.indexes = dataStructure.getIndexes();
        for(Index i: this.indexes.values()){
            this.indexes.put(i.getType(),i);
        }

        this.indexMap.put("","spo"); //cas où pattern = ?x ?p ?y
        this.indexMap.put("0","spo");
        this.indexMap.put("1","pso");
        this.indexMap.put("2","osp");
        this.indexMap.put("01","spo");
        this.indexMap.put("02","sop");
        this.indexMap.put("12","pos");

        this.stats = new Statistics();
    }

    public Statistics getStats(){
        return  this.stats;
    }

    //Appelé si shuffle ou warm
    public ArrayList<String> buildQueriesAL() {
        String queriesPath = this.options.getQueriesPath();

        try {
            File myObj = new File(queriesPath);
            Scanner myReader = new Scanner(myObj);
            ArrayList<String> queries = new ArrayList<>();
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                if (this.options.getShuffle()) {
                    queries.add(data);
                }
            }
            return queries;

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

        //TODO file not found ?
    //TODO: à facto ?
    //TODO: star queries 
    public void traiterQueries() throws MalformedQueryException {
        String queriesPath = this.options.getQueriesPath();

        boolean optimisation = this.options.getOptim_none();

        //TODO: UTILISER METHODE BUILDAL ?
        try {
            File myObj = new File(queriesPath);
            Scanner myReader = new Scanner(myObj);
            ArrayList<String> queries = new ArrayList<>();
            int queryCount = 0;
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                if(this.options.getShuffle()){
                    queryCount++; //TODO à quoi ça sert ?
                    queries.add(data);}
                else {
                    if (optimisation) {
                        solveOptim(data);
                    } else {
                        solve(data);
                    }
                }
            }

            if(this.options.getShuffle()) {
                Collections.shuffle(queries);
                for (String s : queries) {
                    if (optimisation) {
                        solveOptim(s);
                    } else {
                        solve(s);
                    }
                }
            }

            this.stats.setQueriesNum(queries.size());
            //TODO: write stats ?

            myReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

    }

    public String encodePattern(StatementPattern sp) {
        String subject = (sp.getSubjectVar().hasValue()?"0":"");
        String predicate = (sp.getPredicateVar().hasValue()?"1":"");
        String object = (sp.getObjectVar().hasValue()?"2":"");

        return subject+predicate+object;
    }


    //Ajouter les variables de chaque pattern à notre structure allResults (récupère tous les résultats
    // pour chaque variable, pour chaque pattern)
    public void addKey(String v, HashMap<String, ArrayList<ArrayList<Integer>>> allResults){
        if(!allResults.containsKey(v)) {
            allResults.put(v, new ArrayList<>());
            //System.out.println(v+" ajoutée à AllResults");
        }
    }

    //Solve optimisé
    public void solveOptim(String req) throws MalformedQueryException {
        if(options.getWarmPct()!=0){
            this.warm(options.getWarmPct());
        }

        String outputPath = this.options.getOutputPath();
        String verbose ="";
        verbose+="\nRequete: "+req+"\n";

        //Utilisation d'une instance de SPARLQLParser
        SPARQLParser sparqlParser = new SPARQLParser();
        ParsedQuery pq = sparqlParser.parseQuery(req, null);
        List<StatementPattern> patterns = StatementPatternCollector.process(pq.getTupleExpr());

        //TODO: utile dans solveOptim ?!
        HashMap<String, ArrayList<ArrayList<Integer>>> allResults = new HashMap<>();
        // Cette structure permet d'obtenir tous les résultats pour toutes les variables pour tous les patterns
        //Clé = la valeur recherchée
        //Valeurs = un ensemble d'ensembles de résultats pour chaque pattern

        verbose+="-- Lecture de chaque pattern"+"\n";

        HashMap<StatementPattern, Float> selectivities = new HashMap<>();
        for(StatementPattern sp: patterns) {
            selectivities.put(sp,selectivity(sp));
        }
        //Il faut trier cet HM par ordre de selectivité croissant
        //Le plus faible est celui fait en premier


    }

    //Méthode principale de la classe
    // TODO : optimiser les paramètres
    public void solve(String req) throws MalformedQueryException {
        if(options.getWarmPct()!=0){
            this.warm(options.getWarmPct());
        }
        String outputPath = this.options.getOutputPath();
        String verbose ="";
        verbose+="\nRequete: "+req+"\n";

        //Utilisation d'une instance de SPARLQLParser
        SPARQLParser sparqlParser = new SPARQLParser();
        ParsedQuery pq = sparqlParser.parseQuery(req, null);
        List<StatementPattern> patterns = StatementPatternCollector.process(pq.getTupleExpr());

        HashMap<String, ArrayList<ArrayList<Integer>>> allResults = new HashMap<>();
        // Cette structure permet d'obtenir tous les résultats pour toutes les variables pour tous les patterns
        //Clé = la valeur recherchée
        //Valeurs = un ensemble d'ensembles de résultats pour chaque pattern

        verbose+="-- Lecture de chaque pattern"+"\n";

        for(StatementPattern sp: patterns) {

            //on encode le pattern
            String indexType = this.indexMap.get(this.encodePattern(sp));
            Index index = this.indexes.get(indexType);

            verbose+="  Index utilisé: " + indexType+"\n";

            //les termes de la requete sont recuperes...
            ArrayList<Var> varList = new ArrayList<>();
            varList.add(sp.getSubjectVar());
            varList.add(sp.getPredicateVar());
            varList.add(sp.getObjectVar());

            //...puis separes en constante / variable
            ArrayList<String> variables = new ArrayList<>();
            ArrayList<String> constantes = new ArrayList<>();

            for(Var v : varList) {
                if(v.hasValue()) {
                    constantes.add(v.getValue().toString().replace("\"",""));
                }
                else {
                    variables.add(v.getName());
                }
            }

            //on ajoute les variables en clef de allResult
            for(String v: variables) {
                addKey(v,allResults);
            }

            if(constantes.size() == 2) { // deux constantes dans le pattern
                int c1 = this.dictionnaire.getValue(constantes.get(0));
                int c2 = this.dictionnaire.getValue(constantes.get(1));

                allResults.get(variables.get(0)).add(index.getIndex().get(c1).get(c2));
            }
            else if(constantes.size() == 1) { // une constante dans le pattern
                int c1 = this.dictionnaire.getValue(constantes.get(0));

                Set<Integer> keys = index.getIndex().get(c1).keySet();
                ArrayList<Integer> resO = new ArrayList();
                for (int i : keys) {
                    resO.add(i);
                    allResults.get(variables.get(1)).add(index.getIndex().get((c1)).get(i));
                }
                allResults.get(variables.get(0)).add(resO);
            }
            else {
                //Cas où il y a 3 variables
                //Choix de base SPO
                // TODO: est-ce qu'il existe un plus optimisé qu'un autre?
                // TODO: Vérifier noms de variables

                Index spo = this.indexes.get("spo");
                Set<Integer> keysS = spo.getIndex().keySet();
                ArrayList<Integer> resS = new ArrayList();
                ArrayList<Integer> resP = new ArrayList();

                for (int kS : keysS) {
                    resS.add(kS);
                    Set<Integer> keysP = spo.getIndex().get((kS)).keySet();

                    for (int kP : keysP) {
                        resP.add(kP);
                        allResults.get(variables.get(2)).add(spo.getIndex().get((kS)).get(kP));
                    }
                }
                allResults.get(variables.get(0)).add(resS);
                allResults.get(variables.get(1)).add(resP);
            }

        }


        //Dans allResults on a les résultats de chaque variable pour chaque pattern
        //Ici on fait pour chaque variable l'intersection des résultats
        //Ca nous permet d'obtenir pour chaque variable l'ensemble des résultas
        HashMap<String, ArrayList<Integer>> results = new HashMap<>();
        ArrayList<Integer> result = new ArrayList<>();
        for(String key: allResults.keySet()){

            result = allResults.get(key).get(0);

            //System.out.println(allResults.get(key).size()-1);
            //TODO: vérifier la taille
            for(int i = 1; i<allResults.get(key).size()-1;i++){
                result.retainAll(allResults.get(key).get(i));
            }
            results.put(key,result);
        }

        //Cette structure nous permet d'avoir uniquement les variables à retourner (celles dans le SELECT)
        ArrayList<String> varToReturn = new ArrayList<>();
        verbose+="-- Résultat de la requete"+"\n";
        pq.getTupleExpr().visit(new QueryModelVisitorBase<RuntimeException>() {
            public void meet(Projection projection) {
                List<ProjectionElem> test = projection.getProjectionElemList().getElements();
                for(ProjectionElem p: test){
                    varToReturn.add(p.getSourceName());
                }
            }
        });
        //TODO: vérifier les résultats des requetes avec JENA --> how


        for (String s : varToReturn) {
            if(this.options.getExport_query_results()) {
                try {
                    FileWriter myWriter = new FileWriter(outputPath + "queryResult.csv");

                    //TODO : QUELLE STRUCTURRRE POUR LE CSV ???
                    myWriter.write(req + "\n");
                    myWriter.write(s + ": " + printRes(results.get(s)) + "\n\n");
                    myWriter.close();
                } catch (IOException e) {
                    System.out.println("Erreur dans l'écriture du résultat de la requête");
                    e.printStackTrace();
                }
            }
            verbose+=s + ": " + printRes(results.get(s));
        }


        if(this.options.getVerbose()){
            System.out.println(verbose);
        }
    }


    public void jenaQueries(String queriesPath,String dataPath) {
        //TODO: enlever commentaire
    /*
    	Model model = ModelFactory.createDefaultModel();
		InputStream in = FileManager.get().open(dataPath);
		
		model.read(in, null,"RDF/XML");
		
		try {
            File myObj = new File(queriesPath);
            Scanner myReader = new Scanner(myObj);

            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
        		Query query = QueryFactory.create(data);
        		QueryExecution qexec = QueryExecutionFactory.create(query,model);
    			try {				
    				ResultSet rs = qexec.execSelect();
    				System.out.println("Query : "+data);
    				ResultSetFormatter.out(System.out, rs, query);
    				System.out.println();
    				
    			} finally {
    				qexec.close();
    			}

            }

            //(String req,String dataPath,String queriesPath,String outputPath)
            myReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

*/
    }

    public String printRes(ArrayList<Integer> tab){
        String res = "";
        for(Integer i: tab){
            res+=this.dictionnaire.getValue(i)+" ";
        }
        return res;
    }

    public void checkReq(String toCompare){
        System.out.println(this.dictionnaire.getValue(toCompare));
    }

    public Options getOptions(){
        return this.options;
    }


    //TODO: on compare ici le résultat d'une req de nous au res d'une req de jena
    public boolean comparisonJena(String req){
        /*
        Model model = ModelFactory.createDefaultModel();
        InputStream in = FileManager.get().open(dataPath);

        model.read(in, null,"RDF/XML");

        Query query = QueryFactory.create(req);
        QueryExecution qexec = QueryExecutionFactory.create(query,model);
        try {
            ResultSet rs = qexec.execSelect();
            System.out.println("Query : "+req);
            ResultSetFormatter.out(System.out, rs, query);
            System.out.println();

        } finally {
            qexec.close();
        }*/
        return false;

    }

    public void warm(float pct) throws MalformedQueryException {
        ArrayList<String> queries = buildQueriesAL();
        Float queriesToExec=queries.size()*pct;

        ArrayList<Integer> generated = new ArrayList<>();
        for(int i=0; i<queriesToExec;i++){
            Random r = new Random();
            int c = r.nextInt(queries.size());
            while(generated.contains(c)){
                c = r.nextInt(queries.size());
            }
            generated.add(c);
            this.solve(queries.get(c));
        }
    }


    //TODO
    public float selectivity(StatementPattern sp) {
        //Il faut récupérer la valeur dans l'index
        //les termes de la requete sont recuperes...
        ArrayList<Var> varList = new ArrayList<>();
        varList.add(sp.getSubjectVar());
        varList.add(sp.getPredicateVar());
        varList.add(sp.getObjectVar());

        //...puis separes en constante / variable
        ArrayList<String> variables = new ArrayList<>();
        ArrayList<String> constantes = new ArrayList<>();

        for (Var v : varList) {
            if (v.hasValue()) {
                constantes.add(v.getValue().toString().replace("\"", ""));
            } else {
                variables.add(v.getName());
            }
        }
        if (constantes.size() == 2) {
            //return this.indexes.get ;
            //this.indexes.get("sop").getValuesNumber();
        }

        if (constantes.size() == 1) {

        }
        return 0;
    }

}




