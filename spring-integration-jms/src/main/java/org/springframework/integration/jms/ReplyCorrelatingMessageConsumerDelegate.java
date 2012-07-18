package org.springframework.integration.jms;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.springframework.integration.MessagingException;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.jms.listener.SessionAwareMessageListener;

public class ReplyCorrelatingMessageConsumerDelegate {

	private final Executor replyReceivingExecutor = Executors.newFixedThreadPool(10);
	private volatile MessageConsumer messageConsumer;
	private volatile long receiveTimeout = 5000;

	private volatile Map<String, LinkedBlockingQueue<Object>> correlatorQueues = new HashMap<String, LinkedBlockingQueue<Object>>();

	public ReplyCorrelatingMessageConsumerDelegate(Session session,
												   Destination replyTo,
												   String messageSelector,
												   long receiveTimeout, ConnectionFactory connectionFactory) throws JMSException{
		//this.messageConsumer = session.createConsumer(replyTo, messageSelector);

		DefaultMessageListenerContainer container = new DefaultMessageListenerContainer();
		container.setDestination(replyTo);
		container.setConcurrentConsumers(10);
		container.setMaxConcurrentConsumers(10);
		container.setConcurrency("10");
		//container.set
		//container.setIdleConsumerLimit(20);
//		container.setErrorHandler(new ErrorHandler() {
//
//			public void handleError(Throwable t) {
//				t.printStackTrace();
//			}
//		});
//		container.setExceptionListener(new ExceptionListener() {
//
//			public void onException(JMSException exception) {
//				exception.printStackTrace();
//			}
//		});
		//container.setIdleTaskExecutionLimit(50);
		container.setMaxMessagesPerTask(500);
		container.setConnectionFactory(connectionFactory);
		container.setMessageSelector(messageSelector);
		container.setTaskExecutor(replyReceivingExecutor);
		container.setMessageListener(new SessionAwareMessageListener<Message>() {

			public void onMessage(Message replyMessage, Session session)
					throws JMSException {

				//System.out.println("Received: " + replyMessage);
				try {
					//Thread.sleep(1);
					String replyCorrelationId = replyMessage.getStringProperty("replyCorrelationId");
					LinkedBlockingQueue<Object> replyCorrelatedQueue = correlatorQueues.get(replyCorrelationId);
					if (replyCorrelatedQueue != null){
						replyCorrelatedQueue.offer(replyMessage);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		container.afterPropertiesSet();
		container.start();

		//this.receiveTimeout = receiveTimeout;
//		this.replyReceivingExecutor.execute(new Runnable() {
//
//			public void run() {
//				while (true) {
//					//System.out.println("Looking");
//
//					try {
//						Thread.sleep(100);
//						javax.jms.Message replyMessage = receiveReplyMessage(messageConsumer);
//						if (replyMessage != null){
//							String replyCorrelationId = replyMessage.getStringProperty("replyCorrelationId");
////							System.out.println("Received: " + replyCorrelationId);
//							LinkedBlockingQueue<Object> replyCorrelatedQueue = correlatorQueues.get(replyCorrelationId);
//							if (replyCorrelatedQueue != null){
//								replyCorrelatedQueue.offer(replyMessage);
//							}
//						}
//					} catch (Exception e) {
//						e.printStackTrace();
//					}
//				}
//			}
//		});
	}



	public Message receive(String messageId){
		//System.out.println("Sent: " + messageId);
		this.correlatorQueues.put(messageId, new LinkedBlockingQueue<Object>(1));

//		this.replyReceivingExecutor.execute(new Runnable() {
//			public void run() {
//				String replyCorrelationId = null;
//				LinkedBlockingQueue<Object> replyCorrelatedQueue = null;
//				try {
//					//Thread.sleep(100);
//					boolean found = false;
//					while (!found) {
//						System.out.println("Looking");
//						javax.jms.Message replyMessage = receiveReplyMessage(messageConsumer);
//						replyCorrelationId = replyMessage.getStringProperty("replyCorrelationId");
//						System.out.println("Received: " + replyCorrelationId);
//						replyCorrelatedQueue = correlatorQueues.get(replyCorrelationId);
//						if (replyCorrelatedQueue != null){
//							replyCorrelatedQueue.offer(replyMessage);
//							found = true;
//						}
//					}
//				}
//				catch (Exception e) {
//					if (replyCorrelatedQueue != null){
//						replyCorrelatedQueue.offer(e);
//					}
//				}
//				finally {
//					JmsUtils.closeMessageConsumer(messageConsumer);
//				}
//			}
//		});
		try {
			Object reply = this.correlatorQueues.get(messageId).poll(60000*60, TimeUnit.MILLISECONDS);

			if (reply instanceof Exception){
				throw new MessagingException("Failed to receive reply message", (Throwable) reply);
			}
			else {
				//System.out.println("Returning: " + reply);
				this.correlatorQueues.remove(messageId);
				return (javax.jms.Message)reply;
			}
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
			throw new MessagingException("Message receive was interrupted");
		}
	}

	private javax.jms.Message receiveReplyMessage(MessageConsumer messageConsumer) throws JMSException {
		return (this.receiveTimeout >= 0) ? messageConsumer.receive(receiveTimeout) : messageConsumer.receive();
	}

}
