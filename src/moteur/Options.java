package moteur;

import java.util.Arrays;

public class Options {
	String queriesPath;
	String dataPath;
	String outputPath;
	Boolean output;
	Boolean verbose;
	Boolean export_query_stats;
	Boolean export_query_results;
	Boolean jena;
	Boolean shuffle;
	float warmPct;
	Boolean optim_none;
	Boolean star_queries;
	Boolean diagnostic;
	Boolean workload_time;

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

	public Boolean getOutput(){
	    return output;
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
	
	public Boolean getDiagnostic() {
		return this.diagnostic;
	}
	public Boolean getWorkload_time() {
		return this.workload_time;
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

	public void setOutput(Boolean output ){
	    this.output = output;
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
	
	public void setWorkload_time(Boolean workload_time) {
		this.workload_time = workload_time;
	}
	
	public void setDiagnostic(Boolean diag) {
		this.diagnostic = diag;
	}

	public void diagnostic(String diagnostic){
		if(this.diagnostic){
			System.out.println(diagnostic);
		}
	}

	public Options(String[] args){
		this.queriesPath="queries.txt";
		this.dataPath="datasets/100K.rdfxml";
		this.outputPath="results/";
		this.output=false;
		this.verbose=false;
		this.export_query_stats=false;
		this.export_query_results=false;
		this.jena=false;
		this.shuffle=false;
		this.warmPct=0;
		this.optim_none=false;
		this.star_queries=false;
		this.diagnostic=false;
		this.workload_time=false;

		String line = "";
		for(String arg : args) {
			line+=arg+" ";
		}
		String[] options = line.split("-");
		for(String opt: options){
			//System.out.println("'"+opt+"'");
			if(opt.startsWith("queries")){
				this.setQueriesPath(opt.split(" ")[1]);
			}
			if(opt.startsWith("data")){  //data "datasets/500K.rdfxml" 
				this.setDataPath(opt.split(" ")[1]);
			}
			if(opt.startsWith("output")){
			    this.setOutput(true);
			    String[] otp = opt.split(" ");
			    if(otp.length==2){
                    this.setOutputPath(otp[1]);
                }
			}
			if(opt.startsWith("verbose")){
				this.setVerbose(true);
			}
			if(opt.startsWith("export_query_stats")){
				this.setExport_query_stats(true);
			}
			if(opt.startsWith("export_query_results")){
				this.setExport_query_results(true);
			}
			if(opt.startsWith("jena")){
				this.setJena(true);
			}
			if(opt.startsWith("shuffle")){
				this.setShuffle(true);
			}
			if(opt.startsWith("warm")){
				//TODO: à vérifier que ça marche bien
				this.setWarmPct(Float.parseFloat(opt.split("\\s+")[1]));
			}
			if(opt.startsWith("optim_none")){
				this.setOptim_none(true);
			}
			if(opt.startsWith("star_queries")){
				this.setStar_queries(true);
			}
			if(opt.startsWith("workload_time")){
				this.setWorkload_time(true);
			}
		}
	}
}
