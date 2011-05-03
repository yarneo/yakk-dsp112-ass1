import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

public class AmazonJobMain {
	public void main(String args[]) throws Exception
	{
		if (args.length != 2) {
			System.err.println("Usage: AmazonJobMain <minimumSupport> <minimumRelativeFrequency>");
			System.exit(1);
		}
		
		String minimumSupport = args[0];
		String minimumRelativeFrequency = args[1];
		
		AWSCredentials credentials = 
			new PropertiesCredentials(AmazonJobMain.class.getResourceAsStream(
					"/AwsCredentials.properties"));
		AmazonElasticMapReduce mapReduce = 
			new AmazonElasticMapReduceClient(credentials);
	     
	    HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
	        .withJar("s3n://yekk-dsp112/contexts.jar")
	        .withMainClass("MapReduce1.ContextsMain")
	        .withArgs("s3n://yekk-dsp112/input/", 
	        		  "s3n://yekk-dsp112/output1/",
	        		  "s3n://yekk-dsp112/output2/",
	        		  "s3n://yekk-dsp112/output3/",
	        		  "s3n://yekk-dsp112/output/",
	        		  minimumSupport,
	        		  minimumRelativeFrequency);
	     
	    StepConfig stepConfig = new StepConfig()
	        .withName("contexts")
	        .withHadoopJarStep(hadoopJarStep)
	        .withActionOnFailure("TERMINATE_JOB_FLOW");
	     
	    JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
	        .withInstanceCount(2)
	        .withMasterInstanceType(InstanceType.M1Small.toString())
	        .withSlaveInstanceType(InstanceType.M1Small.toString())
	        .withHadoopVersion("0.20").withEc2KeyName("MyKeyPair")
	        .withKeepJobFlowAliveWhenNoSteps(false)
	        .withPlacement(new PlacementType());
	     
	    RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
	        .withName("contexts job")
	        .withInstances(instances)
	        .withSteps(stepConfig)
	        .withLogUri("s3n://yekk-dsp112/logs/");
	     
	    RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
	    String jobFlowId = runJobFlowResult.getJobFlowId();
	    System.out.println("Ran job flow with id: " + jobFlowId);		
	}
}
