package org.springframework.integration.jms;

import java.util.Map;
import java.util.WeakHashMap;
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
import org.springframework.jms.support.JmsUtils;

public class ReplyCorrelatingMessageConsumerDelegate2 {

	private final Executor replyReceivingExecutor = Executors.newFixedThreadPool(5);
	private volatile MessageConsumer messageConsumer;
	private volatile long receiveTimeout = 5000;
	private final Session session;
	private final Destination replyTo;
	private final String messageSelector;

	private volatile Map<String, LinkedBlockingQueue<Object>> correlatorQueues = new WeakHashMap<String, LinkedBlockingQueue<Object>>(50);

	public ReplyCorrelatingMessageConsumerDelegate2(Session session,
												   Destination replyTo,
												   String messageSelector,
												   long receiveTimeout, ConnectionFactory connectionFactory) throws JMSException{
		this.session = session;
		this.replyTo = replyTo;
		this.messageSelector = messageSelector;
	}



	public Message receive(String messageId){
		//System.out.println("Sent: " + messageId);
		this.correlatorQueues.put(messageId, new LinkedBlockingQueue<Object>(1));

		this.replyReceivingExecutor.execute(new Runnable() {
			public void run() {

				String replyCorrelationId = null;
				LinkedBlockingQueue<Object> replyCorrelatedQueue = null;
				try {
					//Thread.sleep(100);
					//System.out.println("Looking");
					MessageConsumer messageConsumer = session.createConsumer(replyTo, messageSelector);
					javax.jms.Message replyMessage = receiveReplyMessage(messageConsumer);
					replyCorrelationId = replyMessage.getStringProperty("replyCorrelationId");
					//System.out.println("Received: " + replyCorrelationId);
					replyCorrelatedQueue = correlatorQueues.get(replyCorrelationId);
					if (replyCorrelatedQueue != null){
						replyCorrelatedQueue.offer(replyMessage);
					}
				}
				catch (Exception e) {
					if (replyCorrelatedQueue != null){
						replyCorrelatedQueue.offer(e);
					}
				}
				finally {
					JmsUtils.closeMessageConsumer(messageConsumer);
				}
			}
		});
		try {
			Object reply = this.correlatorQueues.get(messageId).poll(60000*60, TimeUnit.MILLISECONDS);

			if (reply instanceof Exception){
				throw new MessagingException("Failed to receive reply message", (Throwable) reply);
			}
			else {
				//System.out.println("Returning: " + reply);
//				this.correlatorQueues.remove(messageId);
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
