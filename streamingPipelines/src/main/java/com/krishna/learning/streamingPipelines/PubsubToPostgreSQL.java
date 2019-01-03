package com.krishna.learning.streamingPipelines;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

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


public class PubsubToPostgreSQL {
	private static transient Connection connection = null;
	
	
	public interface Options extends PipelineOptions, StreamingOptions {
		@Description("The Cloud Pub/Sub subscription to consume from. " + "The name should be in the format of "
				+ "projects/<project-id>/subscriptions/<subscription-name>.")
		@Validation.Required
		ValueProvider<String> getFirstSubscription();

		void setFirstSubscription(ValueProvider<String> firstSubscription);
	}

	private static Connection getCloudSqlConnection() throws SQLException {

		try {
			if (connection == null || connection.isClosed()) {
				Class.forName("org.postgresql.Driver");
				connection = DriverManager.getConnection("jdbc:postgresql://104.154.28.212:5432/airflow",
						"airflow-user", "airflow");

			}
		} catch (ClassNotFoundException | SQLException e) {
			throw new SQLException("Exception occured while connecting to PostgresSQL");
		}
		return connection;
	}

	static class DatabaseCon extends DoFn<String, String> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private static transient Connection connection;

		@StartBundle
		public void startBundle(DoFn<String, String>.StartBundleContext context) {
			synchronized (context) {
				try {
					if (connection == null || connection.isClosed()) {
						connection = getCloudSqlConnection();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		@ProcessElement
		public void processElement(ProcessContext c) throws Exception {

			PreparedStatement statement = connection.prepareStatement("SELECT state from dag_stats where dag_id = ?");
			statement.setString(1, c.element());
			ResultSet rs = statement.executeQuery();
			System.out.println("is the compiler coming here");
			c.output(c.element());
			while (rs.next()) {
				String state = rs.getString("state");
				System.out.println("connection establoshed successfully!");
				c.output(state);
			}
		}

	}

	public static void main(String[] args) {

		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		options.setStreaming(true);
		Pipeline p = Pipeline.create(options);

		p.apply("messages from First Subscription",
				PubsubIO.readStrings().fromSubscription(options.getFirstSubscription()))
				.apply("fixed window", Window.<String>into(FixedWindows.of(Duration.standardSeconds(5))))
				.apply("Database connection test", ParDo.of(new DatabaseCon())).apply("storage",
						TextIO.write().to("gs://dataflow-aug-4/output").withWindowedWrites().withNumShards(2));
		// p.run().waitUntilFinish();
		PipelineResult result = p.run();
		try {
			result.getState();
			result.waitUntilFinish();
		} catch (UnsupportedOperationException e) {
			// do nothing
		} catch (Exception e) {
			e.printStackTrace();

		}
	}

}
