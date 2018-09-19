package com.amazonaws.samples;

import java.util.List;

import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.CelebrityDetail;
import com.amazonaws.services.rekognition.model.CelebrityRecognition;
import com.amazonaws.services.rekognition.model.CelebrityRecognitionSortBy;
import com.amazonaws.services.rekognition.model.GetCelebrityRecognitionRequest;
import com.amazonaws.services.rekognition.model.GetCelebrityRecognitionResult;
import com.amazonaws.services.rekognition.model.NotificationChannel;
import com.amazonaws.services.rekognition.model.S3Object;
import com.amazonaws.services.rekognition.model.StartCelebrityRecognitionRequest;
import com.amazonaws.services.rekognition.model.StartCelebrityRecognitionResult;
import com.amazonaws.services.rekognition.model.Video;
import com.amazonaws.services.rekognition.model.VideoMetadata;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class VideoDetectCelebrity {


	private static String bucket = "rekognition-demo-bucket";
	private static String video = "input2.mp4"; 
	private static String queueUrl =  "https://sqs.ap-south-1.amazonaws.com/895240857861/rekognitionQueue";
	private static String topicArn="arn:aws:sns:ap-south-1:895240857861:AmazonRekognitionTopic";
	private static String roleArn="arn:aws:iam::895240857861:role/serviceRekognition";
	private static AmazonSQS sqs = null;
	private static AmazonRekognition rek = null;

	private static NotificationChannel channel= new NotificationChannel()
			.withSNSTopicArn(topicArn)
			.withRoleArn(roleArn);


	private static String startJobId = null;


	public static void main(String[] args)  throws Exception{


		sqs = AmazonSQSClientBuilder.standard().withRegion("ap-south-1").build();
		rek = AmazonRekognitionClientBuilder.standard().withRegion("ap-south-1").build();

		//=================================================
		StartCelebrities(bucket, video);
		//=================================================
		System.out.println("Waiting for job: " + startJobId);
		//Poll queue for messages
		List<Message> messages=null;
		int dotLine=0;
		boolean jobFound=false;

		//loop until the job status is published. Ignore other messages in queue.
		do{
			messages = sqs.receiveMessage(queueUrl).getMessages();
			if (dotLine++<20){
				System.out.print(".");
			}else{
				System.out.println();
				dotLine=0;
			}

			if (!messages.isEmpty()) {
				//Loop through messages received.
				for (Message message: messages) {
					String notification = message.getBody();

					// Get status and job id from notification.
					ObjectMapper mapper = new ObjectMapper();
					JsonNode jsonMessageTree = mapper.readTree(notification);
					JsonNode messageBodyText = jsonMessageTree.get("Message");
					ObjectMapper operationResultMapper = new ObjectMapper();
					JsonNode jsonResultTree = operationResultMapper.readTree(messageBodyText.textValue());
					JsonNode operationJobId = jsonResultTree.get("JobId");
					JsonNode operationStatus = jsonResultTree.get("Status");
					System.out.println("Job found was " + operationJobId);
					// Found job. Get the results and display.
					if(operationJobId.asText().equals(startJobId)){
						jobFound=true;
						System.out.println("Job id: " + operationJobId );
						System.out.println("Status : " + operationStatus.toString());
						if (operationStatus.asText().equals("SUCCEEDED")){
							//============================================
							GetResultsCelebrities();
							//============================================
						}
						else{
							System.out.println("Video analysis failed");
						}

						sqs.deleteMessage(queueUrl,message.getReceiptHandle());
					}

					else{
						System.out.println("Job received was not job " +  startJobId);
						//Delete unknown message. Consider moving message to dead letter queue
						sqs.deleteMessage(queueUrl,message.getReceiptHandle());
					}
				}
			}
		} while (!jobFound);


		System.out.println("Done!");
	}


	// Celebrities=====================================================================
	private static void StartCelebrities(String bucket, String video) throws Exception{

		StartCelebrityRecognitionRequest req = new StartCelebrityRecognitionRequest()
				.withVideo(new Video()
						.withS3Object(new S3Object()
								.withBucket(bucket)
								.withName(video)))
				.withNotificationChannel(channel);



		StartCelebrityRecognitionResult startCelebrityRecognitionResult = rek.startCelebrityRecognition(req);
		startJobId=startCelebrityRecognitionResult.getJobId();

	} 

	private static void GetResultsCelebrities() throws Exception{

		int maxResults=10;
		String paginationToken=null;
		GetCelebrityRecognitionResult celebrityRecognitionResult=null;

		do{
			if (celebrityRecognitionResult !=null){
				paginationToken = celebrityRecognitionResult.getNextToken();
			}
			celebrityRecognitionResult = rek.getCelebrityRecognition(new GetCelebrityRecognitionRequest()
					.withJobId(startJobId)
					.withNextToken(paginationToken)
					.withSortBy(CelebrityRecognitionSortBy.TIMESTAMP)
					.withMaxResults(maxResults));


			System.out.println("File info for page");
			VideoMetadata videoMetaData=celebrityRecognitionResult.getVideoMetadata();

			System.out.println("Format: " + videoMetaData.getFormat());
			System.out.println("Codec: " + videoMetaData.getCodec());
			System.out.println("Duration: " + videoMetaData.getDurationMillis());
			System.out.println("FrameRate: " + videoMetaData.getFrameRate());

			System.out.println("Job");

			System.out.println("Job status: " + celebrityRecognitionResult.getJobStatus());


			//Show celebrities
			List<CelebrityRecognition> celebs= celebrityRecognitionResult.getCelebrities();

			for (CelebrityRecognition celeb: celebs) { 
				long seconds=celeb.getTimestamp()/1000;
				System.out.print("Sec: " + Long.toString(seconds) + " ");
				CelebrityDetail details=celeb.getCelebrity();
				System.out.println("Name: " + details.getName());
				System.out.println("Id: " + details.getId());
				System.out.println(); 
			}
		} while (celebrityRecognitionResult !=null && celebrityRecognitionResult.getNextToken() != null);

	}

}
