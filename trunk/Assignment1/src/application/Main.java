package application;

import java.util.ArrayList;
import java.util.List;


import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;


public class Main {
	static AmazonEC2      ec2;


    private static void init() throws Exception {
        AWSCredentials credentials = new PropertiesCredentials(
                Main.class.getResourceAsStream("AwsCredentials.properties"));

        ec2 = new AmazonEC2Client(credentials);

    }
	
	

	public static void checkManagerInstance() {
		DescribeInstancesRequest request = new DescribeInstancesRequest();

		List<String> valuesT1 = new ArrayList<String>();
		valuesT1.add("Manager1");
		Filter filter1 = new Filter("tag:Manager1", valuesT1);


		DescribeInstancesResult result = ec2.describeInstances(request.withFilters(filter1));
		List<Reservation> reservations = result.getReservations();

		if(reservations.isEmpty()) {//no manager node
			 try {
				             // Basic 32-bit Amazon Linux AMI 1.0 (AMI Id: ami-08728661)
				             RunInstancesRequest request2 = new RunInstancesRequest("ami-76f0061f", 1, 1);
				             request2.setInstanceType(InstanceType.T1Micro.toString());
				             request2.
				             List<Instance> instances = ec2.runInstances(request2).getReservation().getInstances();
				             System.out.println("Launch instances: " + instances);
				  
				         } catch (AmazonServiceException ase) {
				             System.out.println("Caught Exception: " + ase.getMessage());
				             System.out.println("Reponse Status Code: " + ase.getStatusCode());
				             System.out.println("Error Code: " + ase.getErrorCode());
				             System.out.println("Request ID: " + ase.getRequestId());
				         }
		}
		else {
			System.out.println("instances!");
			for (Reservation reservation : reservations) {
				List<Instance> instances = reservation.getInstances();
				for (Instance instance : instances) {
					instance.getInstanceType();
				}
			}
		}

	}




	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		init();
		checkManagerInstance();

	}

}
