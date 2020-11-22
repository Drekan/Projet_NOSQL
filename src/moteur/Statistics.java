package moteur;

import java.io.FileWriter;
import java.io.IOException;

public class Statistics {
	private String outputPath;
	private String dataPath;
	private String queriesPath;

	private int RDFTripleNum;
	private int queriesNum;
	private int queriesReadTime;
	private int dicCreationTime;
	private int indexesNum = 6; //TODO : demander à quoi ça sert -- faut il compter aussi les index2 et index1?
	private int indexesCreationTotalTime;
	private int workloadEvaluationTime;
	private int optimizationTime = 0; //TODO : AQCS ??
	private long totalTime;


	public Statistics(Options opt) {
		this.dataPath = opt.getDataPath();
		this.queriesPath = opt.getQueriesPath();
		this.outputPath = opt.getOutputPath();
	}


	//---Getters & Setters---

	public int getRDFTripleNum() {
		return RDFTripleNum;
	}

	public void setRDFTripleNum(int rDFTripleNum) {
		RDFTripleNum = rDFTripleNum;
	}

	public int getQueriesNum() {
		return queriesNum;
	}

	public void setQueriesNum(int queriesNum) {
		this.queriesNum = queriesNum;
	}

	public int getQueriesReadTime() {
		return queriesReadTime;
	}

	public void setQueriesReadTime(int queriesReadTime) { this.queriesReadTime = queriesReadTime; }

	public int getDicCreationTime() {
		return dicCreationTime;
	}

	public void setDicCreationTime(int dicCreationTime) {
		this.dicCreationTime = dicCreationTime;
	}

	public int getIndexesNum() {
		return indexesNum;
	}

	public void setIndexesNum(int indexesNum) {
		this.indexesNum = indexesNum;
	}

	public int getIndexesCreationTotalTime() {
		return indexesCreationTotalTime;
	}

	public void setIndexesCreationTotalTime(int indexesCreationTotalTime) {
		this.indexesCreationTotalTime = indexesCreationTotalTime;
	}

	public int getWorkloadEvaluationTime() {
		return workloadEvaluationTime;
	}

	public void setWorkloadEvaluationTime(int workloadEvaluationTime) {
		this.workloadEvaluationTime = workloadEvaluationTime;
	}

	public int getOptimizationTime() {
		return optimizationTime;
	}

	public void setOptimizationTime(int optimizationTime) {
		this.optimizationTime = optimizationTime;
	}

	public long getTotalTime() {
		return totalTime;
	}

	public void setTotalTime(long totalTime) {
		this.totalTime = totalTime;
	}

	public String getDataPath() {
		return dataPath;
	}

	public void setDataPath(String dataPath) {
		this.dataPath = dataPath;
	}

	public String getQueriesPath() {
		return queriesPath;
	}

	public void setQueriesPath(String queriesPath) {
		this.queriesPath = queriesPath;
	}

	//TODO: écrire à la suite ou pas ?
	public void writeStats() {
		try {
			FileWriter myWriter = new FileWriter(outputPath+"queriesStat.csv");
			myWriter.write(
					dataPath+","+
							queriesPath+","+
	        		  RDFTripleNum+","+
	        		  queriesNum+","+
	        		  queriesNum+","+
	        		  queriesReadTime+","+
	        		  dicCreationTime+","+
	        		  indexesNum+","+
	        		  indexesCreationTotalTime+","+
	        		  workloadEvaluationTime+","+
	        		  optimizationTime+","+
	        		  totalTime+","
					);

			myWriter.close();
		} catch (IOException e) {
			System.out.println("Erreur dans l'écriture du résultat de la requête");
			e.printStackTrace();
		}
	}

}
