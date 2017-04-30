package generator;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.*;

public class Generator {

	private static Logger logger = LoggerFactory.getLogger(Generator.class);
	private static final String QUEUE = "CLOUDAMQP_URL";
	private static final String QUEUE_NAME = "get_license_jobs";
	
	private static int count = 0;
	
	public static void main(String[] args) {

		logger.info("Running with queue at {}", System.getenv(QUEUE));
		
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
	 		
	 		final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
	 		executorService.scheduleAtFixedRate(new Runnable(){
	 			@Override
	 			public void run() {
	 				task(channel);
	 			}
	 		}, 0, 1, TimeUnit.SECONDS);
	 		
	 		// Register shutdown hook with the JVM
	 		// TODO - put the search job back on the queue if it exists..
	 		Runtime.getRuntime().addShutdownHook(new Thread() {
	 			@Override
	 			public void run() {
	 				
	 				logger.info("Sutting down the Generator..");
	 				try {
	 					channel.close();
	 					conn.close();
	 				} catch (Exception e) {
	 					e.printStackTrace();
	 				}
	 			}
	 		});	 		
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	} 
	
	private static void task(Channel channel) {
		try {
			String message = "Hello.. message " + count++;
			channel.basicPublish("", QUEUE_NAME, null, message.getBytes("UTF-8"));
			logger.info("Sent message [{}]", message);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

}
