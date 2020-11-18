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

public class Solveur {
    //TODO: gérer l'UTF-8
    //TODO: tester avec des requetes avec plus de resultats

    private Dictionnaire dico;
    
    //on accède aux index par leur type : les clefs sont ["spo","sop",...,"ops"]
    private HashMap<String,Index> indexes;
    
    /* 
     * indexMap : structure qui permet de savoir quel index utiliser en fonction du pattern que l'on a.
     * 
     * Exemple :
     * 12 <=> p et o ont une valeur connue, on utilise donc un index pos
     * 0 <=> s a une valeur connue, on utilise donc un index spo
     */
    private HashMap<String,String> indexMap;

    private Options options;
    
    private Statistics stats;
//(String dataPath,String queriesPath,String outputPath)
    public Solveur(Dictionnaire dico, ArrayList<Index> indexes,Statistics stats, String options){
        this.indexes = new HashMap<>();
        this.indexMap = new HashMap<>();
        this.stats = stats;
        
    	this.dico = dico;
    	
        for(Index i: indexes){
            this.indexes.put(i.getType(),i);
        }
        
        this.indexMap.put("","spo"); //cas où pattern = ?x ?p ?y
        this.indexMap.put("0","spo");
        this.indexMap.put("1","pso");
        this.indexMap.put("2","osp");
        this.indexMap.put("01","spo");
        this.indexMap.put("02","sop");
        this.indexMap.put("12","pos");

        //TODO: à améliorer ?
        this.options = new Options();
        this.options.setOptions(options);
    }

    //TODO file not found ?
    //TODO: à facto ?
    public void traiterQueries() throws IOException, MalformedQueryException {
        //TODO: on est d'accord, ça vaut pas le coup de mettre tout dans une collection si pas trié ? ou non ?
        String queriesPath = this.options.getQueriesPath();
        String outputPath = this.options.getOutputPath();
        if(this.options.getShuffle()){
            try {
                File myObj = new File(queriesPath);
                Scanner myReader = new Scanner(myObj);
                ArrayList<String> queries = new ArrayList<>();
                while (myReader.hasNextLine()) {
                    String data = myReader.nextLine();
                    queries.add(data);
                }

                Collections.shuffle(queries);
                for(String s: queries){
                    solve(s);
                }

                this.stats.setQueriesNum(queries.size());

                //(String req,String dataPath,String queriesPath,String outputPath)
                myReader.close();
            } catch (FileNotFoundException e) {
                System.out.println("An error occurred.");
                e.printStackTrace();
            }
        }
        else {
            try {
                File myObj = new File(queriesPath);
                Scanner myReader = new Scanner(myObj);
                int queryCount = 0;
                while (myReader.hasNextLine()) {
                    queryCount++;
                    String data = myReader.nextLine();
                    solve(data);
                }

                this.stats.setQueriesNum(queryCount);

                //(String req,String dataPath,String queriesPath,String outputPath)
                myReader.close();
            } catch (FileNotFoundException e) {
                System.out.println("An error occurred.");
                e.printStackTrace();
            }
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

    //Méthode principale de la classe
    // TODO : optimiser les paramètres
    public void solve(String req) throws MalformedQueryException {
        String outputPath = this.options.getOutputPath();
        System.out.println("\nRequete: "+req);
        //Utilisation d'une instance de SPARLQLParser
        SPARQLParser sparqlParser = new SPARQLParser();
        ParsedQuery pq = sparqlParser.parseQuery(req, null);
        List<StatementPattern> patterns = StatementPatternCollector.process(pq.getTupleExpr());

        HashMap<String, ArrayList<ArrayList<Integer>>> allResults = new HashMap<>();
        // Cette structure permet d'obtenir tous les résultats pour toutes les variables pour tous les patterns
        //Clé = la valeur recherchée
        //Valeurs = un ensemble d'ensembles de résultats pour chaque pattern

        System.out.println("-- Lecture de chaque pattern");

        for(StatementPattern sp: patterns) {
        	
        	//on encode le pattern
        	String indexType = this.indexMap.get(this.encodePattern(sp));
        	Index index = this.indexes.get(indexType);
        	
            System.out.println("  Index utilisé: " + indexType);
            
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
            	int c1 = dico.getValue(constantes.get(0));
            	int c2 = dico.getValue(constantes.get(1));

            	allResults.get(variables.get(0)).add(index.getIndex().get(c1).get(c2));	
            }
            else if(constantes.size() == 1) { // une constante dans le pattern
            	int c1 = dico.getValue(constantes.get(0));
            	
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
        System.out.println("-- Résultat de la requete");
        pq.getTupleExpr().visit(new QueryModelVisitorBase<RuntimeException>() {
            public void meet(Projection projection) {
                List<ProjectionElem> test = projection.getProjectionElemList().getElements();
                for(ProjectionElem p: test){
                    varToReturn.add(p.getSourceName());
                }
            }
        });

        for(String s: varToReturn){
        	try {
                FileWriter myWriter = new FileWriter(outputPath+"queryResult.csv");
                
                //TODO : QUELLE STRUCTURRRE POUR LE CSV ??? 
                myWriter.write(req+"\n");
                myWriter.write(s+": "+ printRes(results.get(s))+"\n\n");
                myWriter.close();
              } catch (IOException e) {
                System.out.println("Erreur dans l'écriture du résultat de la requête");
                e.printStackTrace();
              }
            System.out.println(s+": "+ printRes(results.get(s)));
        }

        //(String req,String dataPath,String queriesPath,String outputPath)
        

        //for(String k: allResults.keySet()){
         //   System.out.println(k + allResults.get(k).toString());
            //for(Integer i: results.get) {
            //System.out.println(k + " ++ " + results.get(k).get(i));
            //}
        //}

        //TODO: vérifier les résultats des requetes avec JENA
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
            res+=this.dico.getValue(i)+" ";
        }
        return res;
    }

    public void checkReq(String toCompare){
        System.out.println(this.dico.getValue(toCompare));
    }

    public Options getOptions(){
        return this.options;
    }

    //TODO: peut etre pas très opti
    //TODO: on compare ici le résultat d'une req de nous au res d'une req de jena
    //TODO: faire jena unitaire
    public boolean comparisonJena(String req){
        //if(solve(req).equals)
        return false;
    }

    //TODO
    public void warm(float pct){

    }


}




