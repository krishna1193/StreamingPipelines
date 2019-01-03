package com.krishna.learning.streamingPipelines;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WatchingFiles {
	private static final Logger LOG = LoggerFactory.getLogger(WatchingFiles.class);
	public static interface MyOptions extends DataflowPipelineOptions {
		@Description("The Cloud Pub/Sub subscription to consume from. " + "The name should be in the format of "
				+ "projects/<project-id>/subscriptions/<subscription-name>.")
		@Validation.Required
		ValueProvider<String> getFirstSubscription();

		void setFirstSubscription(ValueProvider<String> firstSubscription);
		

		@Description("The Cloud Pub/Sub topic to publish to. " + "The name should be in the format of "
				+ "projects/<project-id>/topics/<topic-name>.")
		@Validation.Required
		ValueProvider<String> getOutputTopic();

		void setOutputTopic(ValueProvider<String> outputTopic);
	}

	public static void main(String[] args) {
		LOG.info("starting dataflow pipeline");
		System.out.println("starting dataflow pipeline");
		MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
		options.setStreaming(true);
		Pipeline p = Pipeline.create(options);
		LOG.info("creating pipeline");
		System.out.println("creating pipeline");
		

		PCollection<String> lines = p.apply(TextIO.read().from("gs://dataflow-aug-4/LandingFolder/*.json").watchForNewFiles(
				// Check for new files every 30 seconds
				Duration.standardSeconds(3),
				// Never stop checking for new files
				Watch.Growth.<String>never()));
		System.out.println("is the lines getting captured or not?");
		//lines.apply(TextIO.write().to("gs://dataflow-aug-4/outputFolder/output"));
		lines.apply("publish to output topic", PubsubIO.writeStrings().to(options.getOutputTopic()));
		System.out.println("does this writes to storage?");
		
	
		p.run().waitUntilFinish();
		System.out.println("does this pipeline run?");
		
		
	}

}
