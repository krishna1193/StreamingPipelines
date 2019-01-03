package com.krishna.learning.streamingPipelines;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class StreamingPipelines {
	private static final Logger LOG = LoggerFactory.getLogger(StreamingPipelines.class);

	public interface Options extends PipelineOptions, StreamingOptions {
		@Description("The Cloud Pub/Sub subscription to consume from. " + "The name should be in the format of "
				+ "projects/<project-id>/subscriptions/<subscription-name>.")
		@Validation.Required
		ValueProvider<String> getFirstSubscription();

		void setFirstSubscription(ValueProvider<String> firstSubscription);

		@Description("The Cloud Pub/Sub subscription to consume from. " + "The name should be in the format of "
				+ "projects/<project-id>/subscriptions/<subscription-name>.")
		@Validation.Required
		ValueProvider<String> getSecondSubscription();

		void setSecondSubscription(ValueProvider<String> secondSubscription);

		@Description("The Cloud Pub/Sub subscription to consume from. " + "The name should be in the format of "
				+ "projects/<project-id>/subscriptions/<subscription-name>.")
		@Validation.Required
		ValueProvider<String> getThirdSubscription();

		void setThirdSubscription(ValueProvider<String> thirdSubscription);

		@Description("The Cloud Pub/Sub subscription to consume from. " + "The name should be in the format of "
				+ "projects/<project-id>/subscriptions/<subscription-name>.")
		@Validation.Required
		ValueProvider<String> getFourthSubscription();

		void setFourthSubscription(ValueProvider<String> fourthSubscription);

		@Description("The Cloud Pub/Sub topic to publish to. " + "The name should be in the format of "
				+ "projects/<project-id>/topics/<topic-name>.")
		@Validation.Required
		ValueProvider<String> getOutputTopic();

		void setOutputTopic(ValueProvider<String> outputTopic);

	}

	@SuppressWarnings("serial")
	public static void main(String[] args) {
		final Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		options.setStreaming(true);
		Pipeline p = Pipeline.create(options);

		PCollection<String> firstData = p
				.apply("messages from First Subscription",
						PubsubIO.readStrings().fromSubscription(options.getFirstSubscription()))
				.apply("fixed window", Window.<String>into(FixedWindows.of(Duration.standardSeconds(5))))
				.apply("words per line", ParDo.of(new DoFn<String, String>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						String line = c.element().toString();
						LOG.info("this message is from first data");
						LOG.info(line);
						c.output(line);
					}
				}));

		PCollection<String> secondData = p
				.apply("messages from Second Subscription",
						PubsubIO.readStrings().fromSubscription(options.getSecondSubscription()))
				.apply("fixed window", Window.<String>into(FixedWindows.of(Duration.standardSeconds(5))))
				.apply("words per line", ParDo.of(new DoFn<String, String>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						String line = c.element().toString();
						LOG.info("this message is from second data");
						LOG.info(line);
						c.output(line);
					}
				}));

		PCollection<String> thirdData = p
				.apply("messages from third Subscription",
						PubsubIO.readStrings().fromSubscription(options.getThirdSubscription()))
				.apply("fixed window", Window.<String>into(FixedWindows.of(Duration.standardSeconds(5))))
				.apply("words per line", ParDo.of(new DoFn<String, String>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						String line = c.element().toString();
						LOG.info("this message is from third data");
						LOG.info(line);
						c.output(line);
					}
				}));

		PCollection<String> fourthData = p
				.apply("messages from fourth Subscription",
						PubsubIO.readStrings().fromSubscription(options.getFourthSubscription()))
				.apply("fixed window", Window.<String>into(FixedWindows.of(Duration.standardSeconds(5))))
				.apply("words per line", ParDo.of(new DoFn<String, String>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						String line = c.element().toString();
						LOG.info("this message is from fourth data");
						LOG.info(line);
						c.output(line);
					}
				}));

		PCollectionList<String> pcs = PCollectionList.of(firstData).and(secondData).and(thirdData).and(fourthData);
		final PCollection<String> mergeddata = pcs.apply(Flatten.<String>pCollections());
		LOG.info("merged data");

		mergeddata.apply(TextIO.write().to("gs://dataflow-aug-4/output").withWindowedWrites().withNumShards(1));

		p.run();
	}
}
