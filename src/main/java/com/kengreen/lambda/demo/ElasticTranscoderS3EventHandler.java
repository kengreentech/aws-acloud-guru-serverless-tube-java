package com.kengreen.lambda.demo;

import static com.amazonaws.regions.Regions.US_EAST_1;
import static com.amazonaws.services.elastictranscoder.AmazonElasticTranscoderClient.builder;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import com.amazonaws.services.elastictranscoder.AmazonElasticTranscoder;
import com.amazonaws.services.elastictranscoder.model.CreateJobOutput;
import com.amazonaws.services.elastictranscoder.model.CreateJobRequest;
import com.amazonaws.services.elastictranscoder.model.JobInput;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;

public class ElasticTranscoderS3EventHandler implements RequestHandler<S3Event, String> {

	private static final String INPUT_KEY = System.getenv("INPUT_KEY");
	private static final String PIPELINE_ID = System.getenv("PIPELINE_ID");
	
	private Consumer<String> logMethod;
	
	@Override
	public String handleRequest(S3Event s3Event, Context context) {

		logMethod = context.getLogger()::log;
				
		log("S3 Event: " + s3Event.toJson());
		log("Context: " + context);
		log("INPUT_KEY: " + INPUT_KEY);
		log("Pipleine ID: " + PIPELINE_ID);
		
		AmazonElasticTranscoder transcoder = builder().withRegion(US_EAST_1).build();

		Consumer<S3EventNotificationRecord> transcodingS3EventConsumer = (record) -> transcode(transcoder, logMethod, record);
		
		// the original JavaScript code only processed the first record
		s3Event.getRecords().forEach(transcodingS3EventConsumer);
		
		// TODO: AWS Lamdba docs suggest that asynchronous event handlers should have a void return value but the
		// Request Handler requires an output object.  Investigate this.
		return null;
	}

	private void log(String logContents) {
		
		logMethod.accept(logContents);
	}

	static final String ETC_PRESET_GENERIC_780P  = "1351620000001-000001";
	static final String ETC_PRESET_GENERIC_1080P = "1351620000001-000010";
	static final String ETC_PRESET_WEB           = "1351620000001-100070";

	private static void transcode(AmazonElasticTranscoder transcoder, Consumer<String> log, S3EventNotificationRecord record) {
		
		String key = record.getS3().getObject().getKey();
		log.accept("Key: " + key);
		
		String urlDecodedKey = record.getS3().getObject().getUrlDecodedKey();
		log.accept("Decoded Key: " + urlDecodedKey);
		
		// replace spaces in URL with +
		String sourceKey = urlDecodedKey.replace(" ", "+");
		log.accept("Source Key: " + sourceKey);

		// remove the extension
		String outputKey = sourceKey.split("[.]")[0];
		log.accept("Output Key: " + outputKey);

		
        // Setup the job input using the provided input key.
        JobInput input = new JobInput()
            .withKey(sourceKey);
        
        List<CreateJobOutput> outputs = Arrays.asList(
            	new CreateJobOutput()
                .withKey(outputKey + "-1080p" + ".mp4")
                .withPresetId(ETC_PRESET_GENERIC_1080P), 
            	new CreateJobOutput()
                .withKey(outputKey + "-720p" + ".mp4")
                .withPresetId(ETC_PRESET_GENERIC_780P), 
            	new CreateJobOutput()
                .withKey(outputKey + "-web-720p" + ".mp4")
                .withPresetId(ETC_PRESET_WEB));
        
        // Create the job.
        CreateJobRequest createJobRequest = new CreateJobRequest()
            .withPipelineId(PIPELINE_ID)
            .withInput(input)
            .withOutputKeyPrefix(outputKey + "/")
            .withOutputs(outputs);
        
        transcoder.createJob(createJobRequest).getJob();

		
	}
	
}
