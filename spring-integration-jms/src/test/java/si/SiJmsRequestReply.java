package si;

import java.util.Random;

import junit.framework.Assert;

import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.integration.gateway.RequestReplyExchanger;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.util.StopWatch;

public class SiJmsRequestReply {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
//		testPerformance();
		//testCorrelation();
		//testPerformanceTempReply();
		testCorrelationWithDelayedReply();
		//testPerformanceTempReplyWithCustomUUID();
		//testPerformanceReplyWithCustomUUID();
	}

	public static void testPerformance() throws Exception{
		new ClassPathXmlApplicationContext("consumer.xml", SiJmsRequestReply.class);
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("producer.xml", SiJmsRequestReply.class);
		RequestReplyExchanger gateway = context.getBean(RequestReplyExchanger.class);
		for (int i = 0; i < 20; i++) {
			testBatchOfMessagesSync(gateway, 10000);
		}
	}

	public static void testPerformanceTempReply() throws Exception{
		//new ClassPathXmlApplicationContext("consumer.xml", SiJmsRequestReply.class);
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("producer-temp-reply.xml", SiJmsRequestReply.class);
		RequestReplyExchanger gateway = context.getBean(RequestReplyExchanger.class);
		for (int i = 0; i < 100; i++) {
			testBatchOfMessagesSync(gateway, 10000);
		}
	}

	public static void testPerformanceTempReplyWithCustomUUID() throws Exception{
		new ClassPathXmlApplicationContext("consumer-uuid.xml", SiJmsRequestReply.class);
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("producer-temp-reply-uuid.xml", SiJmsRequestReply.class);
		RequestReplyExchanger gateway = context.getBean(RequestReplyExchanger.class);
		for (int i = 0; i < 20; i++) {
			testBatchOfMessagesSync(gateway, 10000);
		}
	}

	public static void testPerformanceReplyWithCustomUUID() throws Exception{
		new ClassPathXmlApplicationContext("consumer-uuid.xml", SiJmsRequestReply.class);
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("producer-uuid.xml", SiJmsRequestReply.class);
		RequestReplyExchanger gateway = context.getBean(RequestReplyExchanger.class);
		for (int i = 0; i < 20; i++) {
			testBatchOfMessagesSync(gateway, 10000);
		}
	}

	public static void testCorrelation() throws Exception{
		new ClassPathXmlApplicationContext("random-delay-consumer.xml", SiJmsRequestReply.class);
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("async-producer.xml", SiJmsRequestReply.class);
		RequestReplyExchanger gateway = context.getBean(RequestReplyExchanger.class);
		for (int i = 0; i < 20; i++) {
			testBatchOfMessagesAsync(gateway, 20);
		}
	}

	public static void testCorrelationWithDelayedReply() throws Exception{
		new ClassPathXmlApplicationContext("delayed-consumer.xml", SiJmsRequestReply.class);
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("producer.xml", SiJmsRequestReply.class);
		RequestReplyExchanger gateway = context.getBean(RequestReplyExchanger.class);
		try {
			System.out.println("REPLY: " + gateway.exchange(MessageBuilder.withPayload(1).build()).getPayload());
		} catch (Exception e) {
			e.printStackTrace();
		}
		Thread.sleep(2000);// ensure the consumer does send a reply for the firts one
		System.out.println("REPLY: " + gateway.exchange(MessageBuilder.withPayload(2).build()).getPayload());
	}

	private static void testBatchOfMessagesAsync(final RequestReplyExchanger gateway, int number) throws Exception {
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		for (int i = 0; i < number; i++) {
			Assert.assertEquals(i, gateway.exchange(MessageBuilder.withPayload(i).build()).getPayload());
		}
		stopWatch.stop();
		System.out.println("Exchanged " + number + "  messages with random delay, async in " + stopWatch.getTotalTimeMillis());
	}

	private static void testBatchOfMessagesSync(final RequestReplyExchanger gateway, int number) throws Exception{
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		for (int i = 0; i < number; i++) {
			gateway.exchange(MessageBuilder.withPayload(String.valueOf(i)).build()).getPayload();
		}
		stopWatch.stop();
		System.out.println("Exchanged " + number + "  messages in " + stopWatch.getTotalTimeMillis());

	}

	public static class MyService{
		Random random = new Random();
    	public Integer echo(Integer value) throws Exception{
    		Thread.sleep(random.nextInt(1000));
    		return value;
    	}
    }

	public static class SlowService{
    	public Integer echo(Integer value) throws Exception{
    		System.out.println("Received value: " + value);
    		if (value == 1){
    			Thread.sleep(6000);
    		}
    		return value;
    	}
    }

}
