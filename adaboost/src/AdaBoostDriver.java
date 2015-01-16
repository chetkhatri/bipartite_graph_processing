package adaboost;

import java.io.IOException;

import cgl.imr.base.TwisterMonitor;
import cgl.imr.base.impl.JobConf;
import cgl.imr.client.TwisterDriver;
import cgl.imr.types.DoubleVectorData;

import org.safehaus.uuid.UUIDGenerator;

public class AdaBoostDriver {
	
    private UUIDGenerator uuidGen = UUIDGenerator.getInstance();
    
	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
            System.out.println("num args: = " + args.length);
			String errorReport = "AdaBoost: the Correct arguments are \n"
					+ "java AdaBoostDriver "
					+ "<weights file> <num map tasks> <partition file> <total iterations>";
			System.out.println(errorReport);
			System.exit(0);
		}
		String weightFile = args[0];
		int numMapTasks = Integer.parseInt(args[1]);
		String partitionFile = args[2];
        int totalIters = Integer.parseInt(args[3]);
        //System.out.println("here here here");
		
		AdaBoostDriver client;
		try {
			client = new AdaBoostDriver();
			double beginTime = System.currentTimeMillis();
            //System.out.println("num map tasks = " + numMapTasks);
			client.driveMapReduce(partitionFile, numMapTasks, weightFile, totalIters);
			double endTime = System.currentTimeMillis();
			System.out
					.println("------------------------------------------------------");
			System.out.println("AdaBoost took "
					+ (endTime - beginTime) / 1000 + " seconds.");
			System.out
					.println("------------------------------------------------------");
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(0);
	}
		
	public void driveMapReduce(String partitionFile, int numMapTasks, String weightFile, int totalIters) throws Exception {
		long beforeTime = System.currentTimeMillis();
		int numReducers = 1; // we need only one reducer for the above
		
		// JobConfigurations
		JobConf jobConf = new JobConf("adaboost-map-reduce"+ uuidGen.generateTimeBasedUUID());
		
		jobConf.setMapperClass(AdaBoostMapper.class);
        jobConf.setReducerClass(AdaBoostReducer.class);
        jobConf.setCombinerClass(AdaBoostCombiner.class);
		jobConf.setNumMapTasks(numMapTasks);
		jobConf.setNumReduceTasks(1);
        
        TwisterDriver driver = new TwisterDriver(jobConf);
        System.out.println("partition file: " + partitionFile);
        driver.configureMaps(partitionFile);
        System.out.println("run map");
        
        DoubleVectorData cData = new DoubleVectorData();
        try {
            System.out.println(weightFile);
            cData.loadDataFromTextFile(weightFile);
            //int numWeights = cData.getNumData();
            //System.out.println("cdata numweights: " + numWeights);
        } catch (IOException e) {
            e.printStackTrace();
        }
        /*
        int loopCount =0;
        TwisterMonitor monitor = null;
        
        boolean complete = false;
        while (!complete) {
            monitor = driver.runMapReduceBCast(cData);
            monitor.monitorTillCompletion();
        }*/
        
        TwisterMonitor monitor = null;
        
        boolean complete = false;
        int loopCount=0;
        while (!complete) {
            monitor = driver.runMapReduceBCast(cData);
            monitor.monitorTillCompletion();
            DoubleVectorData newCData = ((AdaBoostCombiner) driver.getCurrentCombiner()).getResults();
            cData=newCData;
            loopCount++;
            if (loopCount == totalIters) {
                complete = true;
                break;
            }
        }
        
		
        System.out.println("cData length: " + cData.getNumData());
        cData.writeToTextFile("twister_output_weights.txt");
        System.out.println("closing driver");
		driver.close();
		System.out.println("driver close");
				
	}
	
}