package moteur;

public class Options {
    String queriesPath;
    String dataPath;
    String outputPath;
    Boolean verbose;
    Boolean export_query_stats;
    Boolean export_query_results;
    Boolean jena;
    Boolean shuffle;
    float warmPct;
    Boolean optim_none; //TODO: vaut true si pas d'optimisation donc utilisation de
    Boolean star_queries;

    public String getQueriesPath() {
        return queriesPath;
    }

    public String getDataPath() {
        return dataPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public Boolean getVerbose() {
        return verbose;
    }

    public Boolean getExport_query_stats() {
        return export_query_stats;
    }

    public Boolean getExport_query_results() {
        return export_query_results;
    }

    public Boolean getJena() {
        return jena;
    }

    public Boolean getShuffle() {
        return shuffle;
    }

    public float getWarmPct() {
        return warmPct;
    }

    public Boolean getOptim_none() {
        return optim_none;
    }

    public Boolean getStar_queries() {
        return star_queries;
    }


    public void setQueriesPath(String queriesPath) {
        this.queriesPath = queriesPath;
    }

    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public void setVerbose(Boolean verbose) {
        this.verbose = verbose;
    }

    public void setExport_query_stats(Boolean export_query_stats) {
        this.export_query_stats = export_query_stats;
    }

    public void setExport_query_results(Boolean export_query_results) {
        this.export_query_results = export_query_results;
    }

    public void setJena(Boolean jena) {
        this.jena = jena;
    }

    public void setShuffle(Boolean jena) {
        this.shuffle = jena;
    }

    public void setWarmPct(float warmPct) {
        this.warmPct = warmPct;
    }

    public void setOptim_none(Boolean optim_none) {
        this.optim_none = optim_none;
    }

    public void setStar_queries(Boolean star_queries) {
        this.star_queries = star_queries;
    }

    public Options(String line){
        this.queriesPath="queries.txt";
        this.dataPath="datasets/100K.rdfxml";
        this.outputPath="results/";
        this.verbose=false;
        this.export_query_stats=false;
        this.export_query_results=false;
        this.jena=false;
        this.shuffle=false;
        this.warmPct=0;
        this.optim_none=false;
        this.star_queries=false;

        String[] options = line.split("-");
        for(String opt: options){
            if(opt.contains("queries")){
                this.setQueriesPath(opt.split("\"")[1]);
            }
            if(opt.contains("data")){
                this.setDataPath(opt.split("\"")[1]);
            }
            if(opt.contains("output")){
                this.setOutputPath(opt.split("\"")[1]);
            }
            if(opt.equals("verbose")){
                this.setVerbose(true);
            }
            if(opt.equals("export_query_stats")){
                this.setExport_query_stats(true);
            }
            if(opt.equals("export_query_results")){
                this.setExport_query_results(true);
            }
            if(opt.equals("jena")){
                this.setJena(true);
            }
            if(opt.equals("shuffle")){
                this.setShuffle(true);
            }
            if(opt.contains("warm")){
                //TODO: à améliorer ?
            }
            if(opt.equals("optim_none")){
                this.setOptim_none(true);
            }
            if(opt.equals("star_queries")){
                this.setStar_queries(true);
            }
        }
    }


}
