package com.krishna.learning.streamingPipelines;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Subscriber {
	private static final Logger LOG = LoggerFactory.getLogger(Subscriber.class);

	public interface Options extends PipelineOptions, StreamingOptions {
		@Description("The Cloud Pub/Sub subscription to consume from. " + "The name should be in the format of "
				+ "projects/<project-id>/subscriptions/<subscription-name>.")
		@Validation.Required
		ValueProvider<String> getFirstSubscription();

		void setFirstSubscription(ValueProvider<String> firstSubscription);
	}

	public static void main(String[] args) {

		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		options.setStreaming(true);
		Pipeline p = Pipeline.create(options);

		p.apply("messages from First Subscription",
				PubsubIO.readStrings().fromSubscription(options.getFirstSubscription()))
				.apply("fixed window", Window.<String>into(FixedWindows.of(Duration.standardSeconds(5))))
				.apply("First DoFn", ParDo.of(new DoFn<String, String>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						String line = c.element().toString();
						LOG.info("this message is from first data");
						LOG.info(line);
						c.output(line);
					}
				})).apply("storage",
						TextIO.write().to("gs://dataflow-aug-4/output").withWindowedWrites().withNumShards(2));
		//p.run().waitUntilFinish();
		//PipelineResult result = p.run();
		  // System.out.println(result.getState().hasReplacementJob());
		   //result.waitUntilFinish();
		
		PipelineResult result = p.run();
	    try {
	        result.getState();
	        result.waitUntilFinish();
	    } catch (UnsupportedOperationException e) {
	       // do nothing
	    	System.out.println("exception occured");
	    } catch (Exception e) {
	        e.printStackTrace();
	        System.out.println(e.getMessage());
	    }
	}
}
