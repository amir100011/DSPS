package com.amazonaws.samples;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TagSpecification;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import java.io.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.codec.binary.Base64;

public class manager {


	private static final String JAR_BUCKET_NAME = "jarbucketkonam";
	private static final String TASK_BUCKET_NAME = "taskbucketkonam";
	private static final String COMPLETED_TASK_BUCKET_NAME = "completedtaskskonam";
	private static final String MANAGER_WORKER_TASK_QUEUE_URL = "konamMANAGER_WORKER_TASK_QUEUE_URL" ;
	private static final String MANAGER_WORKER_DONE_QUEUE_URL = 	 "konamMANAGER_WORKER_DONE_QUEUE_URL";
	private static final String MANAGER_APP_TASK_QUEUE_URL = 	 "konamMANAGER_APP_QUEUE_URL";
	private static final String MANAGER_APP_DONE_QUEUE_URL = "konamMANAGER_APP_DONE_QUEUE_URL";
	private static final String INPUT_THREAD_QUEUE_NAME = "konamINPUT_THREAD_QUEUE_URL";
	private static final int numOfNeededAttributesInAppMessage = 4;
	private static boolean terminate = true;
	private	static List<String> urlsList;
	private static final String outputFileName = "outputFile.txt";


	private static String outputFiletURL;

	private static ArrayList<String> instancesId = new ArrayList<String>();
	private static int numOfRunningWorkers = 0;

	public static void main(String[] args){
		try {
			final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
			List<Message> messages = new ArrayList<Message>();
			Message message;

			AmazonS3ClientBuilder.standard()
			.withRegion("us-east-1")
			.build();

			AmazonSQS sqs = AmazonSQSClientBuilder.standard()
					.withRegion("us-east-1")
					.build();

			AmazonEC2ClientBuilder.standard()
			.withRegion("us-east-1")
			.build();

			while(terminate) {

				while (messages.isEmpty()) {

					ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(MANAGER_APP_TASK_QUEUE_URL)
							.withMaxNumberOfMessages(1)
							.withVisibilityTimeout(0);
					messages = sqs.receiveMessage(receiveMessageRequest.withMessageAttributeNames("All")).getMessages();

					if (!terminate){
						break;
					}
					try {Thread.sleep(2000);}
					catch (InterruptedException e){System.out.println(e.getMessage());}
				}
				if (!terminate) {
					break;
				}

				message = messages.get(0);
				Map<String, MessageAttributeValue> messageAttributes = message.getMessageAttributes();

				// transfer this message from the input queue to the threads queue
				sqs.deleteMessage(new DeleteMessageRequest(MANAGER_APP_TASK_QUEUE_URL,message.getReceiptHandle()));
				SendMessageRequest sendMessageRequest = new SendMessageRequest()
						.withMessageBody(message.getBody())
						.withMessageAttributes(messageAttributes)
						.withQueueUrl(INPUT_THREAD_QUEUE_NAME);;

						sqs.sendMessage(sendMessageRequest);
						messages.clear();//FIXME why clear all massages and not only the first message?

						Runnable newTask = new Runnable() {
							@Override
							public void run() {
								try{
									AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard()
											.withRegion("us-east-1")
											.build();
									AmazonSQS sqs = AmazonSQSClientBuilder.standard()
											.withRegion("us-east-1")
											.build();
									AmazonS3 s3 = AmazonS3ClientBuilder.standard()
											.withRegion("us-east-1")
											.build();

									List<Message> threadMesseges = new ArrayList<Message>();
									while (threadMesseges.isEmpty()) {
										ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(INPUT_THREAD_QUEUE_NAME)
												.withMaxNumberOfMessages(1)
												.withVisibilityTimeout(600);
										threadMesseges = sqs.receiveMessage(receiveMessageRequest.withMessageAttributeNames("All")).getMessages();


										try {Thread.sleep(1000);}
										catch (InterruptedException e){System.out.println(e.getMessage());}
									}
									Message currentMessege = threadMesseges.get(0);

									if(currentMessege != null)
										threadMesseges.clear();

									/*
									 * message = LocalAppID + " " + terminate + " " + n + " " + uploadedFileURL
									 * parsedMessage[0] = localAppID - first 12 is shortLocalAppID
									 * parsedMessage[1] = terminate - true/false
									 * parsedMessage[2] = n - number of workers
									 * parsedMessage[3] = uploadedFileURL - input file URL in S3 
									 */
									//	String[] parsedMessage = currentMessege.getBody().split(" ");
									String[] parsedMessage = getAppMessage(currentMessege); 

									// check how many workers is currently running under the tag workers
									numOfRunningWorkers = getNumOfInstances(ec2);
									int n;
									try {
										n = Integer.parseInt(parsedMessage[2]);
									}
									catch (NumberFormatException e) {
										n = 1;//FIXME changed from 0 to 1
									}

									if(n > 19) {//FIXME replace to 19 
										System.out.println("Number of workers per Input file tasks exceeds the upper limit which is 19, Setting N to 19");
										n = 19;
									}

									// create the (n-numOfRunningWorkers) instances of Workers with the tag Worker
										openWorkers(n, numOfRunningWorkers, ec2);
										
										
										//while (n - numOfRunningWorkers > 0){
//										RunInstancesRequest request = new RunInstancesRequest("ami-0080e4c5bc078760e", 1, 1)
//												.withKeyName("amazonKey");
//										IamInstanceProfileSpecification iamProfile = new IamInstanceProfileSpecification();
//										iamProfile.setName("accessRoleForManager");//FIXME give real name
//										request.setIamInstanceProfile(iamProfile); // attach IAM access role to every worker instance
//
//										// set user data and attach it to workers
//										request.setInstanceType(InstanceType.T2Micro.toString());
//										String userData = "#!/bin/bash\n"
//												+ "aws s3 cp s3://" + JAR_BUCKET_NAME + "/worker.jar worker.jar\n"
//												+ "java -jar worker.jar\n"; 
//										String base64UserData = null;
//										try {
//											base64UserData = new String( Base64.encodeBase64Chunked(userData.getBytes( "UTF-8" )));
//										} catch (UnsupportedEncodingException e) {
//											e.printStackTrace();
//										}
//										request.setUserData(base64UserData); // attach init script
//
//										// make tags
//										List<Tag> tagsList = new ArrayList<Tag>();
//										tagsList.add(new Tag().withKey("Type").withValue("Worker"));
//										TagSpecification tagSpec = new TagSpecification().withTags(tagsList).withResourceType("instance");
//										request.setTagSpecifications(Arrays.asList(tagSpec));
//										ec2.runInstances(request).getReservation().getInstances();
//										System.out.println("worker_" + n + " was created!!");
//										numOfRunningWorkers = getNumOfInstances(ec2);
//									}

									//sends source file tasks to the treads
									int numOfTasksInFile = analyzeTextFile(s3, sqs, ec2, parsedMessage[3], parsedMessage[0]);

									outputFiletURL = waitUntilWorkersDoneAllRequests(s3, sqs, ec2, numOfTasksInFile, parsedMessage[3], parsedMessage[0]);

									if(outputFiletURL == null)
										throw new RuntimeException("output file's url is null");

									sendNewOutputFileMessage(sqs, outputFiletURL, parsedMessage[3]);
									deleteOutputFileMessageFromThreadQueue(sqs,currentMessege.getReceiptHandle());

									//TODO Check if terminate message has been received
									if (Boolean.parseBoolean(parsedMessage[1])){


										// Stop retrieving messages from the input queue, and wait for stopping the running
										terminate = false;

										// Terminate all workers instances start by this Manager
										for(String instance : instancesId )
											TerminateInstance(instance, ec2);


										// Stop the thread pool executor
										executor.shutdown();

										// Terminate the worker instances
										try {Thread.sleep(2000);}
										catch (InterruptedException e){System.out.println(e.getMessage());}

										while (getNumOfInstances(ec2) > 0){
											List<Reservation> reservationsList = ec2.describeInstances().getReservations();
											for(Reservation reservation: reservationsList) {
												List<Instance> instancesList = reservation.getInstances();
												for(Instance instance : instancesList) {
													for(Tag tag : instance.getTags()) {
														if(tag.getKey().equals("Type") && tag.getValue().equals("Worker")){
															if(instance.getState().getCode() == 16 || instance.getState().getCode() == 0) {
																TerminateInstance(instance.getInstanceId(), ec2);
															}
														}    				
													}
												}
											}

											try {Thread.sleep(2000);}
											catch (InterruptedException e){System.out.println(e.getMessage());}
										}




									}

								}catch (AmazonServiceException ase) {
									System.out.println("Caught an AmazonServiceException, which means your request made it "
											+ "to Amazon S3, but was rejected with an error response for some reason.");
									System.out.println("Error Message:    " + ase.getMessage());
									System.out.println("HTTP Status Code: " + ase.getStatusCode());
									System.out.println("AWS Error Code:   " + ase.getErrorCode());
									System.out.println("Error Type:       " + ase.getErrorType());
									System.out.println("Request ID:       " + ase.getRequestId());

								} catch (AmazonClientException ace) {
									System.out.println("Caught an AmazonClientException, which means the client encountered "
											+ "a serious internal problem while trying to communicate with S3, "
											+ "such as not being able to access the network.");
									System.out.println("Error Message: " + ace.getMessage());	
								} catch (Exception e){
									System.out.println(e.toString());
									System.out.println(e.getMessage());
								}
								//								}finally {
								//									try{
								//		                                myAWS.mUploadS3(Header.PRE_UPLOAD_BUCKET_NAME, null, Header.LOGGER_FILE_NAME, new File("ManagerLogger.txt"));
								//		                            }catch (Exception e){
								//		                                System.out.println("Error while uploading Manager logger + " + e.toString());
								//		}
								//								}
							}

							private void deleteOutputFileMessageFromThreadQueue(AmazonSQS sqs, String reciptHandler) {
								DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest(INPUT_THREAD_QUEUE_NAME, reciptHandler);
								sqs.deleteMessage(deleteMessageRequest);

							}

							private void sendNewOutputFileMessage(AmazonSQS sqs, String outputFileURL, String localAppID) {
								SendMessageRequest request = new SendMessageRequest(MANAGER_APP_DONE_QUEUE_URL,"done task-" + localAppID);
								Map<String,MessageAttributeValue> messageAttributes = new HashMap<String,MessageAttributeValue>();
								messageAttributes.put("Type",new MessageAttributeValue()
										.withDataType("String").withStringValue("done task")
										);
								messageAttributes.put("BucketName",new MessageAttributeValue()
										.withDataType("String").withStringValue(COMPLETED_TASK_BUCKET_NAME)
										);
								messageAttributes.put("FileName",new MessageAttributeValue()
										.withDataType("String").withStringValue(outputFileURL)
										);
								request.withMessageAttributes(messageAttributes);				
								sqs.sendMessage(request);

							}

							private String[] getAppMessage(Message currentMessege) {
								String[] splittedMessage = new String[numOfNeededAttributesInAppMessage];
								Map<String,MessageAttributeValue> messageAttributes = currentMessege.getMessageAttributes();
								boolean validMessage = checkForCorrectAttributesInAppMessage(messageAttributes);
								if(validMessage) {
									splittedMessage[0] = messageAttributes.get("localAppID").getStringValue();
									splittedMessage[1] = messageAttributes.get("shouldTerminate").getStringValue();
									splittedMessage[2] = messageAttributes.get("numOfWorkersPerInputFile").getStringValue();
									splittedMessage[3] = messageAttributes.get("FileName").getStringValue();
								}
								return splittedMessage;
							}

							private boolean checkForCorrectAttributesInAppMessage(
									Map<String, MessageAttributeValue> messageAttributes) {
								return messageAttributes.containsKey("Type") && messageAttributes.get("Type").getStringValue().equals("new PDF task")
										&& messageAttributes.containsKey("FileName") && messageAttributes.containsKey("shouldTerminate")
										&& messageAttributes.containsKey("localAppID")&& messageAttributes.containsKey("numOfWorkersPerInputFile"); 

							}
						};

						// Attach a thread to handle this task
						executor.execute(newTask);

						// Wait a little - to enable AWS updating instances status
						try {Thread.sleep(250);}
						catch (InterruptedException e){System.out.println(e.getMessage());}
			}

		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it "
					+ "to Amazon S3, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());

		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());	
		} catch (Exception e){
			System.out.println(e.toString());
			System.out.println(e.getMessage());

		}
	}

	/**
	 * analyzeTextFile - Manager creates an SQS message for each URL and operation from the input list.
	 * Then, the thread that handles this task is waiting for the workers to response with the results
	 * of those requests
	 * ( Manager reads all Workers messages from SQS and creates one summary file,
	 * once all URLs in the input file have been processed).
	 * @param localAppID 
	 *
	 * @param myAWS mAWS amazon web service object with EC2, S3 & SQS
	 * @param shortLocalAppID the LocalApp ID that request this input-file
	 * @param inputFileURL the input-file URL in S3 LocalApp bucket
	 * @return outputURL, the result-file URL on S3 LocalApp bucket
	 */
	private static int analyzeTextFile(final AmazonS3 s3, final AmazonSQS sqs, final AmazonEC2 ec2, String inputFileName, String localAppID){
		int numOfTasksInFile = 0;
		try {
			urlsList = downloadTasks(s3,inputFileName); // updates the urls for each file

			if(urlsList == null)
				throw new RuntimeException("urlList is Null");


			parseFileIntoMassagesAndSendToSQS(sqs,localAppID);
			numOfTasksInFile = urlsList.size();

				int period = 20*(numOfTasksInFile+1)/(numOfRunningWorkers+1)+1;
				int delay = 2*(numOfTasksInFile+1)/(numOfRunningWorkers+1)+1;

				System.out.println("delay: " + delay + "\t" + "period:" + period);
				//
				//			// Schedule checking for sudden termination
				//			new java.util.Timer().schedule(new TimerTask(){
				//				@Override
				//				public void run() {
				//					if (terminate){
				//						System.out.println("Schedule checking for sudden termination");
				//						int currentNumOfRunningWorkers = getNumOfInstances(ec2);
				//						if (currentNumOfRunningWorkers < numOfRunningWorkers){
				//							int workersToOpen = (numOfRunningWorkers-currentNumOfRunningWorkers);
				//							System.out.println("Adding " + workersToOpen + " instances of Workers to total of " + numOfRunningWorkers +" Workers \n");
				//							openWorkers(workersToOpen, currentNumOfRunningWorkers, ec2);
				//							numOfRunningWorkers = getNumOfInstances(ec2);
				//
				//							//							instancesId.addAll(myAWS.initEC2instance(Header.imageID,
				//							//									1,
				//							//									(numOfRunningWorkers-currentNumOfRunningWorkers),
				//							//									InstanceType.T2Micro.toString(),
				//							//									Header.PRE_UPLOAD_BUCKET_NAME,
				//							//									Header.WORKER_SCRIPT,
				//							//									Header.INSTANCE_WORKER_KEY_NAME,
				//							//									TAG_WORKER));
				//
				//						}
				//					}
				//					// after *num_of_lines x 1s / num_of_workers* we are checking if there is still active worker to detect sudden termination of nodes.
				//				}
				//			},delay,period);			
			}catch (Exception e){
				System.out.println(e.getMessage());
			}
			return numOfTasksInFile;
		}



		private static String waitUntilWorkersDoneAllRequests(AmazonS3 s3, AmazonSQS sqs, AmazonEC2 ec2, int numOfTasksInFile, String inputFileName, String localAppID){

			//create output file if doesn't exists, else overwrite.
			File outputFile = new File(outputFileName);
			try {
				PrintWriter out = new PrintWriter(outputFile, "UTF-8");

				while(numOfTasksInFile > 0){
					ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(MANAGER_WORKER_DONE_QUEUE_URL);
					List<Message> workersComplitionMessages = sqs.receiveMessage(receiveMessageRequest.withMessageAttributeNames("All")).getMessages();
					for(Message complitionMessage : workersComplitionMessages){

						String textToAddToOutputFile = getComplitionMessageText(complitionMessage);
						// add this result from the worker to the Result-file
						out.println(textToAddToOutputFile);
						deleteComplitionMessageFromSQS(sqs, complitionMessage);

						// decrease the count - when we done processing enough messages from the workers we exit
						numOfTasksInFile--;
						System.out.println("numOfTasksToProcess = " + numOfTasksInFile + "\n");
					}
					// "busy"-wait for 0.5 second while workers keep completing other requests
					try {Thread.sleep(250);}
					catch (InterruptedException e){
						System.out.println(e.getMessage());
					}
				}
				out.close();
				// Upload File file to app_bucket+LocalID S3 and return the URL
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (UnsupportedEncodingException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			return uploadOutputFileToS3(s3,outputFile, inputFileName, localAppID);

		}

		private static void deleteComplitionMessageFromSQS(AmazonSQS sqs, Message complitionMessage) {
			DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest(MANAGER_WORKER_DONE_QUEUE_URL, complitionMessage.getReceiptHandle());
			sqs.deleteMessage(deleteMessageRequest);

		}
		private static String uploadOutputFileToS3(AmazonS3 s3, File outputFile, String inputFileName, String folderName ) {
			if(inputFileName != null && outputFile != null ) {
				if (folderName != null){//TODO crerate localID
					s3.putObject(new PutObjectRequest(COMPLETED_TASK_BUCKET_NAME, folderName + "/" + inputFileName, outputFile)); // upload the file to the bucket
					return "https://s3.amazonaws.com/" + COMPLETED_TASK_BUCKET_NAME + "/" + folderName + "/" + inputFileName; // return the url of the uploaded file
				} else{
					s3.putObject(new PutObjectRequest(COMPLETED_TASK_BUCKET_NAME, inputFileName, outputFile)); // upload the file to the bucket
					return "https://s3.amazonaws.com/" + COMPLETED_TASK_BUCKET_NAME + "/" + inputFileName; // return the url of the uploaded file
				}
			}
			return "";
		}

		private static String getComplitionMessageText(Message complitionMessage) {
			String messageTextToReturn = "";
			Map<String,MessageAttributeValue> messageAttributes = complitionMessage.getMessageAttributes();
			boolean validWorkerComplitionMessage = checkForCorrectAttributesInWorkerComplitionMessage(messageAttributes);
			if(validWorkerComplitionMessage) {
				if(messageAttributes.get("Type").getStringValue().equals("error messege")) {
					//				String localAppID = messageAttributes.get("localAppID").getStringValue();
					messageTextToReturn = messageAttributes.get("error_messege").getStringValue();

				}else {
					//				String localAppID = messageAttributes.get("localAppID").getStringValue();
					String commandToExecute = messageAttributes.get("commandToExecute").getStringValue();
					String originalURL = messageAttributes.get("originalURL").getStringValue();
					String proccessedFileURL = messageAttributes.get("proccessedFileURL").getStringValue();
					messageTextToReturn = commandToExecute +": " + originalURL + " " + proccessedFileURL;
				}
			}
			return messageTextToReturn;
		}

		private static List<String> downloadTasks(AmazonS3 s3, String inputFileName) {
			if(inputFileName != null) {
				System.out.println("Downloading an object");
				S3Object sourceFile = s3.getObject(new GetObjectRequest(TASK_BUCKET_NAME, inputFileName));
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
			return null;
		}

		public static void TerminateInstance(String managerInstance, AmazonEC2 ec2) {
			TerminateInstancesRequest deleteRequest = new TerminateInstancesRequest().withInstanceIds(managerInstance);
			ec2.terminateInstances(deleteRequest);
		}


		public static void parseFileIntoMassagesAndSendToSQS(AmazonSQS sqs,String localAppID) {

			for (Iterator<String> iterator = urlsList.iterator(); iterator.hasNext();) {
				String commandLine = iterator.next();
				if (commandLine.isEmpty()) {
					// Remove the current element from the iterator and the list.
					iterator.remove();
				}
				else {
					String[] commandLineSplitted = commandLine.split("\t");
					if(commandLineSplitted.length == 1) {
						commandLine.replaceAll("\t"," ");
						commandLineSplitted = commandLine.split(" ");
					}
					System.out.println(commandLineSplitted[0] + " "+ commandLineSplitted[1]);
					SendMessageRequest msgRequest = new SendMessageRequest(MANAGER_WORKER_TASK_QUEUE_URL,"new PDF task");
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
					messageAttributes.put("localAppID",new MessageAttributeValue()
							.withDataType("String")
							.withStringValue(localAppID)
							);
					msgRequest.withMessageAttributes(messageAttributes);				
					sqs.sendMessage(msgRequest); // send "new task image" msg 
				}
			}
		}

		public static int getNumOfInstances(AmazonEC2 ec2) {
			List<Reservation> reservationsList = ec2.describeInstances().getReservations();
			int numOfinstances = 0;
			for(Reservation reservation: reservationsList) {
				List<Instance> instancesList = reservation.getInstances();
				for(Instance instance : instancesList) {
					for(Tag tag : instance.getTags()) {
						if(tag.getKey().equals("Type") && !tag.getValue().equals("Manager")){
							if(instance.getState().getCode() == 16 || instance.getState().getCode() == 0) 
								numOfinstances++;
						}
					}
				}
			}

			return numOfinstances;
		}


		public static void openWorkers(int howManyWorkersToOpen,int numOfRunningWorkers , AmazonEC2 ec2 ) {

			for(int i = 0; i < howManyWorkersToOpen; i++) {
				RunInstancesRequest request = new RunInstancesRequest("ami-0080e4c5bc078760e", 1, 1);
				IamInstanceProfileSpecification iamProfile = new IamInstanceProfileSpecification();
				iamProfile.setName("accessRoleForManager");//FIXME give real name
				request.setIamInstanceProfile(iamProfile); // attach IAM access role to every worker instance

				// set user data and attach it to workers
				request.setInstanceType(InstanceType.T2Micro.toString());
				String userData = "#!/bin/bash\n"//FIXME give path to jar file
						+ "aws s3 cp s3://" + JAR_BUCKET_NAME + "/worker.jar worker.jar\n"
						+ "java -jar worker.jar " + "\n"; 
				String base64UserData = null;
				try {
					base64UserData = new String( Base64.encodeBase64Chunked(userData.getBytes( "UTF-8" )));
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
				request.setUserData(base64UserData); // attach init script

				// make tags
				List<Tag> tagsList = new ArrayList<Tag>();
				tagsList.add(new Tag().withKey("Type").withValue("Worker"));
				tagsList.add(new Tag().withKey("Name").withValue("Worker_"+numOfRunningWorkers));
				TagSpecification tagSpec = new TagSpecification().withTags(tagsList).withResourceType("instance");
				request.setTagSpecifications(Arrays.asList(tagSpec));
				ec2.runInstances(request).getReservation().getInstances();
				System.out.println("new worker_" + i + " was created!!" );
				numOfRunningWorkers++;
			}
		}

		private static boolean checkForCorrectAttributesInWorkerComplitionMessage(
				Map<String, MessageAttributeValue> messageAttributes) {

			boolean isDoneMessage = messageAttributes.containsKey("Type") && messageAttributes.get("Type").getStringValue().equals("done PDF task")
					&& messageAttributes.containsKey("proccessedFileURL");
			boolean isErrorMessage = messageAttributes.containsKey("Type") && messageAttributes.get("Type").getStringValue().equals("error messege") 
					&& messageAttributes.containsKey("error_messege");


			return  (isDoneMessage || isErrorMessage) 
					&& messageAttributes.containsKey("originalURL") 
					&& messageAttributes.containsKey("localAppID")
					&& messageAttributes.containsKey("commandToExecute");
		}

	}




