package workers;

import java.io.IOException;
import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class LicenseWorker {

	private static final String QUEUE = "CLOUDAMQP_URL";
	private static Logger logger = LoggerFactory.getLogger(LicenseWorker.class);
	private static final String QUEUE_NAME = "get_license_jobs";
	
	public static void main(String[] args) {

		logger.info("Running with queue at {}", System.getenv(QUEUE));
		
		LicenseWorker w = new LicenseWorker();
		w.run();

 		// Register shutdown hook with the JVM
 		// TODO - put the search job back on the queue if it exists..
 		Runtime.getRuntime().addShutdownHook(new Thread() {
 			@Override
 			public void run() {
 				
 				logger.info("Sutting down the LicenseWorker..");
 				logger.warn("TODO : Ensure unprocessed job gets sent back to the queue");
 			}
 		});
	}
	
	private void run() {
		try {
			final URI rabbitMqUrl = new URI(System.getenv(QUEUE));
		
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(rabbitMqUrl.getHost());
			factory.setUsername(rabbitMqUrl.getUserInfo().split(":")[0]);
			factory.setPassword(rabbitMqUrl.getUserInfo().split(":")[1]);
	 		factory.setPort(rabbitMqUrl.getPort());
	 		factory.setVirtualHost(rabbitMqUrl.getPath().substring(1));
	 		
	 		
	 		Connection conn = factory.newConnection();
	 		Channel channel = conn.createChannel();
	 		channel.queueDeclare(QUEUE_NAME, false, false, false, null);
	 		
	 		Consumer consumer = new DefaultConsumer(channel) {
	 			@Override
	 			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
	 				handle(consumerTag, envelope, properties, body);
	 			}
	 		};
	 		
	 		channel.basicConsume(QUEUE_NAME, true, consumer);
	 		logger.info("We have started listening..");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	private void handle(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException  {
		try {
			String message = new String(body, "UTF-8");
			logger.info("Received [{}]", message);
			//TODO - implement the "do work" bit here
		} finally {
			logger.info("Finished job {}", consumerTag);
		}
	}
}
