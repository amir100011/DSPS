package com.amazonaws.samples;

import java.awt.HeadlessException;
import java.awt.Toolkit;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.concurrent.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFTextStripper;
import org.apache.pdfbox.util.PDFImageWriter;
import org.apache.pdfbox.util.PDFText2HTML;


public class Worker {
	

	private static final String MANAGER_WORKER_TASK_QUEUE_URL = "konamMANAGER_WORKER_TASK_QUEUE_URL" ;//TODO move it to manager file
	private static final String MANAGER_WORKER_DONE_QUEUE_URL = 	 "konamMANAGER_WORKER_DONE_QUEUE_URL";
	private static final String CONVERTED_FILES_BUCKET_NAME = "convertedfileskonam";


	static boolean Done = false;
	static String commandToExecute = "",originalURL = "", localAppID="",  messagereceipt = null, proccessedFileURL = "";

	public static void main(String[] args) throws Exception {
		doYourThing(AmazonS3ClientBuilder.standard()
				.withRegion("us-east-1")
				.build(),
				AmazonSQSClientBuilder.standard()
				.withRegion("us-east-1")
				.build());	
		System.out.print("finally!!!!");
	}


	static void doYourThing(AmazonS3 s3,AmazonSQS sqs) throws Exception{			
		while(true) {
			try {
				messagereceipt = null;
				Done = false;
				ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(MANAGER_WORKER_TASK_QUEUE_URL)
						.withMaxNumberOfMessages(1)
						.withVisibilityTimeout(5);
				List<Message> messages = sqs.receiveMessage(receiveMessageRequest.withMessageAttributeNames("All")).getMessages();

				if(messages.isEmpty()) {
					System.out.println("sleeping, no new message");
					TimeUnit.SECONDS.sleep(5);
					continue;
				}

				for (Message message : messages) {
					messagereceipt = message.getReceiptHandle();
					//TODO sqs.changeMessageVisibility(MANAGER_WORKER_TASK_QUEUE_URL,receipt,10000);
					Map<String,MessageAttributeValue> messageAttributes = message.getMessageAttributes();
					if(messageAttributes.containsKey("Type") && messageAttributes.get("Type").getStringValue().equals("new PDF task")
							&& messageAttributes.containsKey("whatToDo") && messageAttributes.containsKey("URL")
							&& messageAttributes.containsKey("localAppID"))
					{ // TODO add checks to the if 
						commandToExecute = messageAttributes.get("whatToDo").getStringValue();
						originalURL = messageAttributes.get("URL").getStringValue();
						localAppID = messageAttributes.get("localAppID").getStringValue();						
						//doneQueueUrl = sqs.getQueueUrl(new GetQueueUrlRequest().withQueueName("img_done_queue_" + localAppID)).getQueueUrl();
						//TODO decide about : message id, queue of result url, result bucket name
						System.out.println("whatToDO: " + commandToExecute + "\n");
						System.out.println("URL: " + originalURL + "\n");
						proccessedFileURL = processMSG(originalURL, commandToExecute, CONVERTED_FILES_BUCKET_NAME, MANAGER_WORKER_DONE_QUEUE_URL,s3, sqs );
						System.out.println("worker finish task: Operation " + commandToExecute + " URL: " + originalURL + ", s3url: " + proccessedFileURL + "\n");
						sendComplitionMassage( MANAGER_WORKER_DONE_QUEUE_URL, originalURL , commandToExecute,  proccessedFileURL, sqs);
						Done = true;
						sqs.deleteMessage(new DeleteMessageRequest(MANAGER_WORKER_TASK_QUEUE_URL,messagereceipt));
					}else {
						throw new Exception("task missing attributes: Operation " + commandToExecute + " URL: " + originalURL + "\n");
					}
					System.out.println("Done!!");
				}
			}catch (Exception e) {
				System.out.println("error " + e.getMessage());
				sendErrorMassage(MANAGER_WORKER_DONE_QUEUE_URL,originalURL, commandToExecute, e.getMessage(),sqs);
				if(messagereceipt != null && !Done) {
					System.out.println("delete Message from sqs queue");
					sqs.deleteMessage(new DeleteMessageRequest(MANAGER_WORKER_TASK_QUEUE_URL, messagereceipt));
					//TODO delete files if excists
				}
			}
		}
	}

	private static void DeleteFile(File file) {
			// delete file
				if(file.delete()) 
		        { 
		            System.out.println("File " + file.getName() + " deleted successfully"); 
		        } 
		        else
		        { 
		            System.out.println("Failed to delete the file: " + file.getName()); 
		        }  			
	}


	private static String processMSG( String originalURL,  String whatToDo,  String bucketName, String queueURL,AmazonS3 s3, AmazonSQS sqs ) throws Exception {

		String[] urlSplit = originalURL.split("/");
		String FileUniqueName = urlSplit[urlSplit.length-1];// + UUID.randomUUID().toString()

		File resultFile = new File(FileUniqueName);
		
		if(FileUniqueName == null)
			System.out.println("null File " + originalURL);
		
		try {
			URL url = new URL(originalURL);
			//download img to computer
			InputStream inputStream = url.openStream();
			OutputStream outputStream = new FileOutputStream(resultFile);

			byte[] b = new byte[2048*2];
			int length;

			while ((length = inputStream.read(b)) != -1) {
				outputStream.write(b, 0, length);
			}

			inputStream.close();
			outputStream.close();
			
		if(whatToDo.equals("ToImage")){ 
			//String outputFileName = "toImage - " + FileUniqueName;
			File convertedFile = convertPDFToImage(resultFile); 
			String key = (convertedFile.getName()+localAppID).replace('\\', '_').replace('/','_').replace(':', '_');
			PutObjectRequest req = new PutObjectRequest(bucketName, key, convertedFile);
			s3.putObject(req.withCannedAcl(CannedAccessControlList.PublicRead));
			DeleteFile(resultFile);
			DeleteFile(convertedFile);
			return s3.getUrl(bucketName, key).toString();
		}		
		if(whatToDo.equals("ToHTML")) {
			//String outputFileName = "toImage - " + FileUniqueName;
			File convertedFile = convertPDFToHTML(resultFile);
			String key = (convertedFile.getName()+localAppID).replace('\\', '_').replace('/','_').replace(':', '_');
			PutObjectRequest req = new PutObjectRequest(bucketName, key, convertedFile);
			s3.putObject(req.withCannedAcl(CannedAccessControlList.PublicRead)); 
			return s3.getUrl(bucketName, key).toString();
		}
		if(whatToDo.equals("ToText")) {
			//String outputFileName = "toImage - " + FileUniqueName;
			File convertedFile = convertPDFToText(resultFile); 
			String key = (convertedFile.getName()+localAppID).replace('\\', '_').replace('/','_').replace(':', '_');
			PutObjectRequest req = new PutObjectRequest(bucketName, key, convertedFile);
			try {
				s3.putObject(req.withCannedAcl(CannedAccessControlList.PublicRead)); 
			}catch(Exception e){
				System.out.println(e.getMessage());
			}
			return s3.getUrl(bucketName, key).toString();
		}
		}catch (Exception e) {
			throw new Exception("bad Operation: Operation " + whatToDo + " URL: " + originalURL);
		}
		System.out.println("bad Operation: Operation " + whatToDo + " URL: " + originalURL + "\n");
		throw new Exception("bad Operation: Operation " + whatToDo + " URL: " + originalURL);

	}


	private static File convertPDFToText(File myPDFWorkingFile) throws Exception {
		try {
			PDDocument myPDF = PDDocument.load(myPDFWorkingFile);
			String text = new PDFTextStripper().getText(myPDF);
			myPDF.close();
			//String fileName = "TextFile.txt";
			String convertedFilePath = myPDFWorkingFile.getAbsolutePath();
			convertedFilePath = convertedFilePath.substring(0,convertedFilePath.length()-4) + ".txt";
			PrintWriter pw = new PrintWriter(convertedFilePath);
			pw.println(text);
			pw.close();
			File convertedFile = new File(convertedFilePath);
			return convertedFile;
		} catch (IOException e) {
			e.printStackTrace();
			throw new Exception("error\n");
			
		}catch (Exception e) {
			System.out.println("An exception occured in writing the pdf text to file.");
			e.printStackTrace();
			throw new Exception("error\n");
		}
	}

	private static File convertPDFToHTML(File myPDFWorkingFile) throws Exception {
		String convertedFilePath = myPDFWorkingFile.getAbsolutePath();
		convertedFilePath = convertedFilePath.substring(0,convertedFilePath.length()-4) + ".html";
		try {
			PDDocument myPDF = PDDocument.load(myPDFWorkingFile);
			PDFText2HTML stripper = new PDFText2HTML("UTF-8");
			stripper.setStartPage(1);
			stripper.setEndPage(1);
			File convertedFile = new File(convertedFilePath);
			String text = stripper.getText(myPDF);
			Writer writer = new BufferedWriter(new FileWriter(convertedFilePath));
			writer.write(text);		      
			if (writer != null) 
				try { writer.close(); } catch (IOException ignore) {}


			return convertedFile;
		} catch (IOException e) {
			e.printStackTrace();
			throw new Exception("error\n");
		}

	}



	private static File convertPDFToImage(File myPDFWorkingFile) throws Exception {

		String password = "";
		String imageFormat = "png";
		int startPage = 1;
		int endPage = 1;
		String color = "rgb";
		int resolution = 56;
		String savedImageFileName = myPDFWorkingFile.getAbsolutePath();
		savedImageFileName = savedImageFileName.substring(0, savedImageFileName.length()-4);
		PDDocument myPDF;
		try {
			myPDF = PDDocument.load(myPDFWorkingFile);
			int imageType = 24;
			if ("bilevel".equalsIgnoreCase(color)) {
				imageType = BufferedImage.TYPE_BYTE_BINARY;
			} else if ("indexed".equalsIgnoreCase(color)) {
				imageType = BufferedImage.TYPE_BYTE_INDEXED;
			} else if ("gray".equalsIgnoreCase(color)) {
				imageType = BufferedImage.TYPE_BYTE_GRAY;
			} else if ("rgb".equalsIgnoreCase(color)) {
				imageType = BufferedImage.TYPE_INT_RGB;
			} else if ("rgba".equalsIgnoreCase(color)) {
				imageType = BufferedImage.TYPE_INT_ARGB;
			} else {
				System.err.println("Error: the number of bits per pixel must be 1, 8 or 24.");
				System.exit(2);
			}
			try
			{
				resolution = Toolkit.getDefaultToolkit().getScreenResolution();
			}
			catch( HeadlessException e )
			{
				resolution = 96;
			}
			PDFImageWriter imageWriter = new PDFImageWriter();
			boolean conversionToImageSuccedded = imageWriter.writeImage(myPDF, imageFormat, password, startPage, endPage, savedImageFileName, imageType, resolution);
			File convertedFile = new File(savedImageFileName +"1.png");
			if (!conversionToImageSuccedded) {
				System.err.println("Error: no writer found for image format '" + "png" + "'");
				System.exit(1);
			} 
			if (myPDF != null) {
				myPDF.close();
			}
			return convertedFile;
		} catch (IOException e) {
			e.printStackTrace();
			throw new Exception("error\n");
		}
	}     

 
	private static void sendComplitionMassage(String doneQueueUrl,String originalURL ,String commandToExecute, String proccessedFileURL,AmazonSQS sqs) {
		if(checkIfQueueIsOpen(sqs,doneQueueUrl)) {
			Map<String, MessageAttributeValue> messageAttributes = new HashMap<String, MessageAttributeValue>();
			messageAttributes.put("Type", new MessageAttributeValue()
					.withDataType("String")
					.withStringValue("done PDF task"));
			messageAttributes.put("localAppID", new MessageAttributeValue()
					.withDataType("String")
					.withStringValue(localAppID));
			messageAttributes.put("originalURL",new MessageAttributeValue()
					.withDataType("String")
					.withStringValue(originalURL));
			messageAttributes.put("commandToExecute",new MessageAttributeValue()
					.withDataType("String")
					.withStringValue(commandToExecute));
			messageAttributes.put("proccessedFileURL",new MessageAttributeValue()
					.withDataType("String")
					.withStringValue(proccessedFileURL));
//			messageAttributes.put("FileName",new MessageAttributeValue()
//					.withDataType("String")
//					.withStringValue(inputFileName));
			

			SendMessageRequest sendMessageRequest = new SendMessageRequest();
			sendMessageRequest.withMessageBody("done PDF task");
			sendMessageRequest.withQueueUrl(doneQueueUrl);
			sendMessageRequest.withMessageAttributes(messageAttributes);
			sqs.sendMessage(sendMessageRequest);
			return;
		}
		System.out.println("queue " + doneQueueUrl + " is closed");
	}

	private static void sendErrorMassage(String doneQueueUrl,String originalURL ,String commandToExecute, String error_messege, AmazonSQS sqs) {
		if(checkIfQueueIsOpen(sqs,doneQueueUrl )) {
			Map<String, MessageAttributeValue> messageAttributes = new HashMap<String, MessageAttributeValue>();
			messageAttributes.put("Type", new MessageAttributeValue()
					.withDataType("String")
					.withStringValue("error messege"));
			messageAttributes.put("localAppID", new MessageAttributeValue()
					.withDataType("String")
					.withStringValue(localAppID));
			messageAttributes.put("originalURL",new MessageAttributeValue()
					.withDataType("String")
					.withStringValue(originalURL));
			messageAttributes.put("commandToExecute",new MessageAttributeValue()
					.withDataType("String")
					.withStringValue(commandToExecute));
			messageAttributes.put("error_messege",new MessageAttributeValue()
					.withDataType("String")
					.withStringValue(error_messege));
//			messageAttributes.put("FileName",new MessageAttributeValue()
//					.withDataType("String")
//					.withStringValue(inputFileName));
			

			SendMessageRequest sendMessageRequest = new SendMessageRequest();
			sendMessageRequest.withMessageBody("error messege");
			sendMessageRequest.withQueueUrl(doneQueueUrl);
			sendMessageRequest.withMessageAttributes(messageAttributes);
			sqs.sendMessage(sendMessageRequest);
			return;
		}
		System.out.println("queue " + doneQueueUrl + " is closed");
	}

	private static Boolean checkIfQueueIsOpen(AmazonSQS sqs, String queueUrl) {
		List<String> tmp= sqs.listQueues().getQueueUrls();
		for(String delMe : tmp) {
			System.out.println(delMe.substring(delMe.length()-queueUrl.length(), delMe.length()));
			if(delMe.substring(delMe.length()-queueUrl.length(), delMe.length()).equals(queueUrl))
				return true;
		}
		return false;
	}


}










