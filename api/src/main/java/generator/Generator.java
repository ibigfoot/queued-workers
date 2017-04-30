package generator;

import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Generator {

	private static Logger logger = LoggerFactory.getLogger(Generator.class);
	private static final String QUEUE = "CLOUDAMQP_URL";
	private static final String QUEUE_NAME = "get_license_jobs";
	
	private static int count = 0;
	
	public static void main(String[] args) {

		logger.info("Running with queue at {}", System.getenv(QUEUE));
		
		try {
	 		final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
	 		executorService.scheduleAtFixedRate(new Runnable(){
	 			@Override
	 			public void run() {
	 				task();
	 			}
	 		}, 0, 1, TimeUnit.SECONDS);
	 		// Register shutdown hook with the JVM
	 		// TODO - put the search job back on the queue if it exists..
	 		Runtime.getRuntime().addShutdownHook(new Thread() {
	 			@Override
	 			public void run() {
	 				logger.info("Sutting down the Generator..");
	 			}
	 		});	
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	} 
	
	private static void task() {
		
		Connection conn = null;
		
		try {

			final URI rabbitMqUrl = new URI(System.getenv(QUEUE));
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(rabbitMqUrl.getHost());
			factory.setUsername(rabbitMqUrl.getUserInfo().split(":")[0]);
			factory.setPassword(rabbitMqUrl.getUserInfo().split(":")[1]);
	 		factory.setPort(rabbitMqUrl.getPort());
	 		factory.setVirtualHost(rabbitMqUrl.getPath().substring(1));
	 		
	 		conn = factory.newConnection();
	 		Channel channel = conn.createChannel();
	 		
	 		channel.queueDeclare(QUEUE_NAME, false, false, false, null);			
			String message = "Hello.. message " + count++;
			channel.basicPublish("", QUEUE_NAME, null, message.getBytes("UTF-8"));
			logger.info("Sent message [{}]", message);			
			
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			try {
				conn.close();
			} catch (Exception e) {
				logger.warn(e.getMessage(), e);
			}
		}
	}

}
