package adaboost;

import cgl.imr.base.Key;
import cgl.imr.base.MapOutputCollector;
import cgl.imr.base.MapTask;
import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.MapperConf;
import cgl.imr.data.file.FileData;
import cgl.imr.types.BytesValue;
import cgl.imr.types.DoubleVectorData;
import cgl.imr.types.StringKey;
import cgl.imr.types.StringValue;
import cgl.imr.types.IntKey;

import java.util.ArrayList;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;

import java.io.IOException;
import java.lang.System.*;

public class AdaBoostMapper implements MapTask {

	public class InstanceData {
		public int index;
		public ArrayList<Integer> features;
	}

	private FileData fileData;
	private int numInstances; // number of urls of all tasks
	private int numInstancesInTask; // number of urls of current map task
	private InstanceData[] InstancesData; // the adjacency matrix of all instances	
	
	public void close() throws TwisterException {
	
	}
	
	public void configure( JobConf jobConf, MapperConf mapConf) throws TwisterException {
		fileData = (FileData) mapConf.getDataPartition();
		try {
            //System.out.println("configure loading");
			loadDataFromFile(fileData.getFileName());
		} catch (Exception e) {
			throw new TwisterException(e);
		}		
	}
	
	public void loadDataFromFile(String fileName) throws IOException {
        //System.out.println("loading data from file");
        File file = new File(fileName);
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String inputLine = reader.readLine();
        this.numInstancesInTask = Integer.parseInt(inputLine);
        //System.out.println("num task instances: " + numInstancesInTask);
        
        InstancesData = new InstanceData[numInstancesInTask];
        String[] vectorValues=null;
		
        for (int i = 0; i < numInstancesInTask; i++) {
            InstancesData[i] = new InstanceData();
            InstancesData[i].features = new ArrayList<Integer>();
            inputLine = reader.readLine();
            vectorValues = inputLine.split("\t");
            InstancesData[i].index = Integer.parseInt(vectorValues[0]);
            for (int j = 1; j < vectorValues.length; j++) {
                InstancesData[i].features.add(Integer.valueOf(vectorValues[j]));
            }// end for j
        }// end for i
        //System.out.println("finished loading");
        reader.close();
    }
	
	public void map(MapOutputCollector collector, Key key, Value val)
    throws TwisterException {
        
        DoubleVectorData cData = new DoubleVectorData();

        try {
            cData.fromBytes(val.getBytes());
            double[][] weights = cData.getData();
            int numWeights = cData.getNumData();
            for(int i=0; i<InstancesData.length;i++) {
                int iClass= InstancesData[i].features.get(0);
                double estimate=0.0;
                int numFeatures = InstancesData[i].features.size();
                if (numFeatures > 1) {
                    for(int j=1; j<numFeatures; j++) {
                        int wIndex = InstancesData[i].features.get(j);
                        //System.out.println("wIndex: " + wIndex);
                        estimate+=weights[(wIndex-1)][1];
                        //System.out.println("estimate: " + estimate);
                    }
                    if (iClass==-1) estimate*=-1;
                    estimate= 1/(1+Math.exp(estimate));
                    String map_string = iClass + " " + estimate + " " + String.valueOf(InstanceData[i].index);
                    //collector.collect(new IntKey(InstancesData[1].index, new StringValue(estimate));
                    for(int j=1; j<numFeatures;j++)
                        collector.collect(new IntKey(InstancesData[i].features.get(j)), new StringValue(map_string));
                    
                }
            }
            
            
            //collector.collect(new IntKey(1), new BytesValue());
		} catch (SerializationException e) {
            throw new TwisterException(e);
		}

	}

}
