package com.amazonaws.samples;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.Iterator;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TagSpecification;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import jdk.management.resource.internal.TotalResourceContext;

import org.apache.commons.codec.binary.Base64;

public class Mannager1 {

	static int totalNumOfTasks, numOfRunningWorkers, neededWorkers, numOfTasksPerWorker;
	static boolean hasNewMassage = false, canOpenNewWorkers = true;
	static String localAppID;
	static Map<String,Integer> numOfTasksOfLocalApp = new HashMap<String,Integer>();
	static List<String> urlsList;
	static List<Message> finishedJobs, alreadyReadMessages;
	static Map<String, List<File>> sammuryFilesMap = new HashMap<String, List<File>>();
	static final String bucketName = "taskkobiandamirbucket";//FIXME give real name
	static final String key = "konam";//FIXME give real name	
	static final String appQueueName = "konamMANAGER_QUEUE_URL";
	static final String workersMissionQueueName = "konamMANAGER_TASK_QUEUE_URL" ;//"appToManagerQueue";
	static final String workersAnswerQueueName = "konamMANAGER_DONE_QUEUE_URL";
	static final int maxNumberOfTriesToOpenWorkers = 600,  maxWorkers = 19;
	static int counterForcheckingFinishedTask = 500;

	public static void main(String[] args) throws InterruptedException {

		AmazonS3 s3 = AmazonS3ClientBuilder.standard()
				.withRegion("us-east-1")
				.build();

		AmazonSQS sqs = AmazonSQSClientBuilder.standard()
				.withRegion("us-east-1")
				.build();

		AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard()
				.withRegion("us-east-1")
				.build();


		while(true) 
		{	
			hasNewMassage = false;
			List<Message> messages = null;
			while(!hasNewMassage) {
				ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(appQueueName).withMaxNumberOfMessages(1)
						.withVisibilityTimeout(2);
				messages = sqs.receiveMessage(receiveMessageRequest.withMessageAttributeNames("All")).getMessages();
				if(messages.isEmpty()) {
					Thread.sleep(50);
					continue;
				}else {
					hasNewMassage = true;
					//checkForterminatinMessage(); TODO add termination message check
				}
			}

			for (Message message: messages) {
				String inputFileName = "";
				Map<String,MessageAttributeValue> messageAttributes = message.getMessageAttributes();
				if(messageAttributes.containsKey("FileName") && messageAttributes.containsKey("localAppID")) {
					inputFileName  = messageAttributes.get("FileName").getStringValue();
					localAppID = messageAttributes.get("localAppID").getStringValue();
				}
				else
					continue;

				//				inputFileName = "inputTest.txt";
//FIXME optional problem we don't have new messages and we didn't finished processing all tasks - counterForcheckingFinishedTask not decrementing
				configreParameters(s3,ec2,inputFileName);

				int numOfTriesToOpenNewWorkers = 0;		
				while (!canOpenNewWorkers && numOfTriesToOpenNewWorkers < maxNumberOfTriesToOpenWorkers) {
					Thread.sleep(100);
					numOfRunningWorkers = getNumOfInstances(ec2)-1;//-1 for manger
					canOpenNewWorkers = neededWorkers > numOfRunningWorkers && neededWorkers < 19;
					numOfTriesToOpenNewWorkers++;
				}

				numOfTriesToOpenNewWorkers = 0;
				int workersToOpen = neededWorkers - numOfRunningWorkers;

				if(canOpenNewWorkers)
					openWorkers(workersToOpen, String.valueOf(localAppID), ec2);

				parseFileIntoMassagesAndSendToSQS(sqs);
				clearAppManagerSQS(sqs, messages);
				counterForcheckingFinishedTask--;
				if (counterForcheckingFinishedTask == 0) {
					finishedJobs = checkForComplitionMassages(sqs);
					List<String> finishedLocalApps = checkForCompletedLocalApp();
					if(!alreadyReadMessages.isEmpty())
						clearManagerWorkersAnswerSQS(sqs);
					counterForcheckingFinishedTask = 500;
					if(!finishedLocalApps.isEmpty()) {
						downloadProcessedFiles(s3, finishedLocalApps);
						//deleteDownloadedFilesFromS3(s3);
					}
				}
			}
		}
	}

	private static List<String> checkForCompletedLocalApp() {
		totalNumOfTasks = -1;//decrease reamining tasks
		for(Message complitionMessage : finishedJobs) {
			Map<String,MessageAttributeValue> messageAttributes = complitionMessage.getMessageAttributes();
			if(messageAttributes.containsKey("Type") && messageAttributes.get("Type").getStringValue().equals("done PDF task")){
				localAppID = messageAttributes.get("localAppID").getStringValue();
				alreadyReadMessages.add(complitionMessage);
				updateTasksMap();
			}
		}
		List<String> finishedLocalApps = new ArrayList<String>();
		for (Entry<String, Integer> entry : numOfTasksOfLocalApp.entrySet()) {
			if(entry.getValue() == 0)
				finishedLocalApps.add(entry.getKey());
		}
		return finishedLocalApps;
	}

	private static void downloadProcessedFiles(AmazonS3 s3, List<String> finishedLocalApps) {
		for(Message complitionMessage: alreadyReadMessages) {
			Map<String,MessageAttributeValue> messageAttributes = complitionMessage.getMessageAttributes();
			localAppID = messageAttributes.get("localAppID").getStringValue();
			if(finishedLocalApps.contains(localAppID)) {
				String proccessedFileToURL = messageAttributes.get("proccessedFileURL").getStringValue();
				String originalURL = messageAttributes.get("originalURL").getStringValue();
				String commandToExecute = messageAttributes.get("commandToExecute").getStringValue();
				downloadFile(s3, proccessedFileToURL, localAppID);
			
			}
		}

	}

	private static void downloadFile(AmazonS3 s3, String proccessedFileToURL, String localAppID) {
		if (sammuryFilesMap.containsKey(localAppID)) {
			File addMe = new File(proccessedFileToURL);
			s3.getObject(new GetObjectRequest(bucketName, proccessedFileToURL), addMe);
			sammuryFilesMap.get(localAppID).add(addMe);
			
		}
			
		
	}

	private static void configreParameters(AmazonS3 s3, AmazonEC2 ec2, String inputFileName) {
		urlsList = downloadTasks(s3,inputFileName); // updates the urls for each file
		totalNumOfTasks = getNumOfTasksInSourceFile(); // updates the numOfTasks for each file
		updateTasksMap();
		numOfRunningWorkers = getNumOfInstances(ec2)-1;//-1 due to manager instance
		neededWorkers = totalNumOfTasks % numOfTasksPerWorker == 0 ? totalNumOfTasks/numOfTasksPerWorker :  (totalNumOfTasks / numOfTasksPerWorker) + 1;
		canOpenNewWorkers = neededWorkers > numOfRunningWorkers && neededWorkers < 19;
	}

	private static Map<String, Integer> updateTasksMap() {
		if(numOfTasksOfLocalApp.containsKey(localAppID))
			numOfTasksOfLocalApp.put(localAppID,numOfTasksOfLocalApp.get(localAppID) + totalNumOfTasks);
		else//new app joins to the party
			numOfTasksOfLocalApp.put(localAppID, totalNumOfTasks);
		return numOfTasksOfLocalApp;


	}

	public static List<Message> checkForComplitionMassages(AmazonSQS sqs) {
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(workersAnswerQueueName).withAttributeNames("All");
		List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
		//updateTasksMap(); TODO
		return messages;
	}

	public static void clearAppManagerSQS(AmazonSQS sqs, List<Message> messages) {
		for(Message message : messages) {	
			DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest(appQueueName, message.getReceiptHandle());
			sqs.deleteMessage(deleteMessageRequest);
		}

	}

	public static void clearManagerWorkersAnswerSQS(AmazonSQS sqs) {
		Map<String,MessageAttributeValue> messageAttributes;
		for(Message message : alreadyReadMessages) {
			DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest(workersAnswerQueueName, message.getReceiptHandle());
			sqs.deleteMessage(deleteMessageRequest);
		}
	}

	private static int getNumOfTasksInSourceFile() {
		int numOfTasks = 0;
		for(@SuppressWarnings("unused") String message : urlsList) {
			numOfTasks++;
		}
		return numOfTasks;
	}

	public static void parseFileIntoMassagesAndSendToSQS(AmazonSQS sqs) {

		for (Iterator<String> iterator = urlsList.iterator(); iterator.hasNext();) {
			String commandLine = iterator.next();
			if (commandLine.isEmpty()) {
				// Remove the current element from the iterator and the list.
				iterator.remove();
			}
			else {
				String[] commandLineSplitted = commandLine.split(" ");
				SendMessageRequest msgRequest = new SendMessageRequest(workersMissionQueueName,"new PDF task");
				Map<String,MessageAttributeValue> messageAttributes = new HashMap<String,MessageAttributeValue>();
				messageAttributes.put("Type",new MessageAttributeValue()
						.withDataType("String")
						.withStringValue("new PDF task")
						);
				messageAttributes.put("whatToDo",new MessageAttributeValue()
						.withDataType("String")
						.withStringValue(commandLineSplitted[0])
						);
				messageAttributes.put("URL",new MessageAttributeValue()
						.withDataType("String")
						.withStringValue(commandLineSplitted[1])
						);
				msgRequest.withMessageAttributes(messageAttributes);				
				sqs.sendMessage(msgRequest); // send "new task image" msg 
			}
		}
	}

	public static List<String> downloadTasks(AmazonS3 s3, String inputFileName) {

		System.out.println("Downloading an object");
		S3Object sourceFile = s3.getObject(new GetObjectRequest(bucketName, inputFileName));
		System.out.println("Content-Type: "  + sourceFile.getObjectMetadata().getContentType());
		InputStream input = sourceFile.getObjectContent();
		List<String> sourceFileParsed = new ArrayList<String>();
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(input));
			while (true) {
				String line = reader.readLine();
				if (line == null) break;
				sourceFileParsed.add(line);
			}
		} catch (Exception e) {

		}

		return sourceFileParsed;
	}

	public static void openWorkers(int howManyWorkersToOpen, String localAppID,AmazonEC2 ec2 ) {

		for(int i = 0; i < howManyWorkersToOpen; i++) {
			RunInstancesRequest request = new RunInstancesRequest("ami-0080e4c5bc078760e", 1, 1);
			IamInstanceProfileSpecification iamProfile = new IamInstanceProfileSpecification();
			iamProfile.setName("Manager_Access_Role");//FIXME give real name
			request.setIamInstanceProfile(iamProfile); // attach IAM access role to every worker instance

			// set user data and attach it to workers
			request.setInstanceType(InstanceType.T2Micro.toString());
			String userData = "#!/bin/bash\n"//FIXME give path to jar file
					+ "aws s3 cp s3://konamanswer/worker.jar worker.jar\n"
					+ "java -jar worker.jar " + localAppID + "\n"; 
			String base64UserData = null;
			try {
				base64UserData = new String( Base64.encodeBase64Chunked(userData.getBytes( "UTF-8" )));
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
			request.setUserData(base64UserData); // attach init script

			// make tags
			List<Tag> tagsList = new ArrayList<Tag>();
			tagsList.add(new Tag().withKey("Id").withValue(localAppID));
			tagsList.add(new Tag().withKey("Type").withValue("Worker"));
			tagsList.add(new Tag().withKey("Name").withValue("Worker_"+localAppID));
			TagSpecification tagSpec = new TagSpecification().withTags(tagsList).withResourceType("instance");
			request.setTagSpecifications(Arrays.asList(tagSpec));
			ec2.runInstances(request).getReservation().getInstances();
			System.out.println("new worker was created!!" );
		}
	}


	public static int getNumOfInstances(AmazonEC2 ec2) {
		List<Reservation> reservationsList = ec2.describeInstances().getReservations();
		int numOfinstances = 0;
		for(Reservation reservation: reservationsList) {
			List<Instance> instancesList = reservation.getInstances();
			for(Instance instance : instancesList) {
				if(instance.getState().getCode() == 16 || instance.getState().getCode() == 0) 
					numOfinstances++;
			}
		}

		return numOfinstances;
	}
}

