/*
 * Copyright 2002-2011 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.integration.jms;

import java.util.Map;
import java.util.UUID;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.expression.Expression;
import org.springframework.integration.Message;
import org.springframework.integration.MessageChannel;
import org.springframework.integration.MessageDeliveryException;
import org.springframework.integration.MessageHandlingException;
import org.springframework.integration.MessageTimeoutException;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.integration.handler.ExpressionEvaluatingMessageProcessor;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.jms.connection.ConnectionFactoryUtils;
import org.springframework.jms.support.JmsUtils;
import org.springframework.jms.support.converter.MessageConverter;
import org.springframework.jms.support.converter.SimpleMessageConverter;
import org.springframework.jms.support.destination.DestinationResolver;
import org.springframework.jms.support.destination.DynamicDestinationResolver;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * An outbound Messaging Gateway for request/reply JMS.
 *
 * @author Mark Fisher
 * @author Arjen Poutsma
 * @author Juergen Hoeller
 * @author Oleg Zhurakousky
 */
public class JmsOutboundGateway extends AbstractReplyProducingMessageHandler implements DisposableBean{

	private final ThreadLocal<MessageConsumer> tempQueueConsumerCache = new ThreadLocal<MessageConsumer>();

	private final String gatewayId = UUID.randomUUID().toString();

	private volatile Destination requestDestination;

	private volatile String requestDestinationName;

	private volatile ExpressionEvaluatingMessageProcessor<?> requestDestinationExpressionProcessor;

	private volatile Destination replyDestination;

	private volatile String replyDestinationName;

	private volatile DestinationResolver destinationResolver = new DynamicDestinationResolver();

	private volatile boolean requestPubSubDomain;

	private volatile boolean replyPubSubDomain;

	private volatile long receiveTimeout = 5000;

	private volatile int deliveryMode = javax.jms.Message.DEFAULT_DELIVERY_MODE;

	private volatile long timeToLive = javax.jms.Message.DEFAULT_TIME_TO_LIVE;

	private volatile int priority = javax.jms.Message.DEFAULT_PRIORITY;

	private volatile boolean explicitQosEnabled;

	private ConnectionFactory connectionFactory;

	private volatile MessageConverter messageConverter = new SimpleMessageConverter();

	private volatile JmsHeaderMapper headerMapper = new DefaultJmsHeaderMapper();

	private volatile String correlationKey;

	private volatile boolean extractRequestPayload = true;

	private volatile boolean extractReplyPayload = true;

	private volatile boolean initialized;

	private final Object initializationMonitor = new Object();


	/**
	 * Set whether message delivery should be persistent or non-persistent,
	 * specified as a boolean value ("true" or "false"). This will set the delivery
	 * mode accordingly to either "PERSISTENT" (1) or "NON_PERSISTENT" (2).
	 * <p>The default is "true", i.e. delivery mode "PERSISTENT".
	 * @see #setDeliveryMode(int)
	 * @see javax.jms.DeliveryMode#PERSISTENT
	 * @see javax.jms.DeliveryMode#NON_PERSISTENT
	 */
	public void setDeliveryPersistent(boolean deliveryPersistent) {
		this.deliveryMode = (deliveryPersistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
	}

	/**
	 * Set the JMS ConnectionFactory that this gateway should use.
	 * This is a <em>required</em> property.
	 */
	public void setConnectionFactory(ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	/**
	 * Set the JMS Destination to which request Messages should be sent.
	 * Either this or one of 'requestDestinationName' or 'requestDestinationExpression' is required.
	 */
	public void setRequestDestination(Destination requestDestination) {
		if (requestDestination instanceof Topic) {
			this.requestPubSubDomain = true;
		}
		this.requestDestination = requestDestination;
	}

	/**
	 * Set the name of the JMS Destination to which request Messages should be sent.
	 * Either this or one of 'requestDestination' or 'requestDestinationExpression' is required.
	 */
	public void setRequestDestinationName(String requestDestinationName) {
		this.requestDestinationName = requestDestinationName;
	}

	/**
	 * Set the SpEL Expression to be used for determining the request Destination instance
	 * or request destination name. Either this or one of 'requestDestination' or
	 * 'requestDestinationName' is required.
	 */
	public void setRequestDestinationExpression(Expression requestDestinationExpression) {
		this.requestDestinationExpressionProcessor = new ExpressionEvaluatingMessageProcessor<Object>(requestDestinationExpression);
	}

	/**
	 * Set the JMS Destination from which reply Messages should be received.
	 * If none is provided, this gateway will create a {@link TemporaryQueue} per invocation.
	 */
	public void setReplyDestination(Destination replyDestination) {
		if (replyDestination instanceof Topic) {
			this.replyPubSubDomain = true;
		}
		this.replyDestination = replyDestination;
	}

	/**
	 * Set the name of the JMS Destination from which reply Messages should be received.
	 * If none is provided, this gateway will create a {@link TemporaryQueue} per invocation.
	 */
	public void setReplyDestinationName(String replyDestinationName) {
		this.replyDestinationName = replyDestinationName;
	}

	/**
	 * Provide the {@link DestinationResolver} to use when resolving either a
	 * 'requestDestinationName' or 'replyDestinationName' value. The default
	 * is an instance of {@link DynamicDestinationResolver}.
	 */
	public void setDestinationResolver(DestinationResolver destinationResolver) {
		this.destinationResolver = destinationResolver;
	}

	/**
	 * Specify whether the request destination is a Topic. This value is
	 * necessary when providing a destination name for a Topic rather than
	 * a destination reference.
	 *
	 * @param requestPubSubDomain true if the request destination is a Topic
	 */
	public void setRequestPubSubDomain(boolean requestPubSubDomain) {
		this.requestPubSubDomain = requestPubSubDomain;
	}

	/**
	 * Specify whether the reply destination is a Topic. This value is
	 * necessary when providing a destination name for a Topic rather than
	 * a destination reference.
	 *
	 * @param replyPubSubDomain true if the reply destination is a Topic
	 */
	public void setReplyPubSubDomain(boolean replyPubSubDomain) {
		this.replyPubSubDomain = replyPubSubDomain;
	}

	/**
	 * Set the max timeout value for the MessageConsumer's receive call when
	 * waiting for a reply. The default value is 5 seconds.
	 */
	public void setReceiveTimeout(long receiveTimeout) {
		this.receiveTimeout = receiveTimeout;
	}

	/**
	 * Specify the JMS DeliveryMode to use when sending request Messages.
	 */
	public void setDeliveryMode(int deliveryMode) {
		this.deliveryMode = deliveryMode;
	}

	/**
	 * Specify the JMS priority to use when sending request Messages.
	 * The value should be within the range of 0-9.
	 */
	public void setPriority(int priority) {
		this.priority = priority;
	}

	/**
	 * Specify the timeToLive for each sent Message.
	 * The default value indicates no expiration.
	 */
	public void setTimeToLive(long timeToLive) {
		this.timeToLive = timeToLive;
	}

	/**
	 * Specify whether explicit QoS settings are enabled
	 * (deliveryMode, priority, and timeToLive).
	 */
	public void setExplicitQosEnabled(boolean explicitQosEnabled) {
		this.explicitQosEnabled = explicitQosEnabled;
	}

	/**
	 * Provide the name of a JMS property that should hold a generated UUID that
	 * the receiver of the JMS Message would expect to represent the CorrelationID.
	 * When waiting for the reply Message, a MessageSelector will be configured
	 * to match this property name and the UUID value that was sent in the request.
	 * If this value is NULL (the default) then the reply consumer's MessageSelector
	 * will be expecting the JMSCorrelationID to equal the Message ID of the request.
	 * If you want to store the outbound correlation UUID value in the actual
	 * JMSCorrelationID property, then set this value to "JMSCorrelationID".
	 * However, any other value will be treated as a JMS String Property.
	 */
	public void setCorrelationKey(String correlationKey) {
		this.correlationKey = correlationKey;
	}

	/**
	 * Provide a {@link MessageConverter} strategy to use for converting the
	 * Spring Integration request Message into a JMS Message and for converting
	 * the JMS reply Messages back into Spring Integration Messages.
	 * <p>
	 * The default is {@link SimpleMessageConverter}.
	 */
	public void setMessageConverter(MessageConverter messageConverter) {
		Assert.notNull(messageConverter, "'messageConverter' must not be null");
		this.messageConverter = messageConverter;
	}

	/**
	 * Provide a {@link JmsHeaderMapper} implementation for mapping the
	 * Spring Integration Message Headers to/from JMS Message properties.
	 */
	public void setHeaderMapper(JmsHeaderMapper headerMapper) {
		this.headerMapper = headerMapper;
	}

	/**
	 * This property describes how a JMS Message should be generated from the
	 * Spring Integration Message. If set to 'true', the body of the JMS Message will be
	 * created from the Spring Integration Message's payload (via the MessageConverter).
	 * If set to 'false', then the entire Spring Integration Message will serve as
	 * the base for JMS Message creation. Since the JMS Message is created by the
	 * MessageConverter, this really manages what is sent to the {@link MessageConverter}:
	 * the entire Spring Integration Message or only its payload.
	 * <br>
	 * Default is 'true'
	 *
	 * @param extractRequestPayload
	 */
	public void setExtractRequestPayload(boolean extractRequestPayload) {
		this.extractRequestPayload = extractRequestPayload;
	}

	/**
	 * This property describes what to do with a JMS reply Message.
	 * If set to 'true', the payload of the Spring Integration Message will be
	 * created from the JMS Reply Message's body (via MessageConverter).
	 * Otherwise, the entire JMS Message will become the payload of the
	 * Spring Integration Message.
	 *
	 * @param extractReplyPayload
	 */
	public void setExtractReplyPayload(boolean extractReplyPayload) {
		this.extractReplyPayload = extractReplyPayload;
	}

	/**
	 * Specify the Spring Integration reply channel. If this property is not
	 * set the gateway will check for a 'replyChannel' header on the request.
	 */
	public void setReplyChannel(MessageChannel replyChannel) {
		this.setOutputChannel(replyChannel);
	}

	@Override
	public String getComponentType() {
		return "jms:outbound-gateway";
	}

	private Destination getRequestDestination(Message<?> message, Session session) throws JMSException {
		if (this.requestDestination != null) {
			return this.requestDestination;
		}
		if (this.requestDestinationName != null) {
			return this.resolveRequestDestination(this.requestDestinationName, session);
		}
		if (this.requestDestinationExpressionProcessor != null) {
			Object result = this.requestDestinationExpressionProcessor.processMessage(message);
			if (result instanceof Destination) {
				return (Destination) result;
			}
			if (result instanceof String) {
				return this.resolveRequestDestination((String) result, session);
			}
			throw new MessageDeliveryException(message,
					"Evaluation of requestDestinationExpression failed to produce a Destination or destination name. Result was: " + result);
		}
		throw new MessageDeliveryException(message,
				"No requestDestination, requestDestinationName, or requestDestinationExpression has been configured.");
	}

	private Destination resolveRequestDestination(String requestDestinationName, Session session) throws JMSException {
		Assert.notNull(this.destinationResolver,
				"DestinationResolver is required when relying upon the 'requestDestinationName' property.");
		return this.destinationResolver.resolveDestinationName(
				session, requestDestinationName, this.requestPubSubDomain);
	}

	private Destination getReplyDestination(Session session) throws JMSException {
		if (this.replyDestination != null) {
			return this.replyDestination;
		}
		if (this.replyDestinationName != null) {
			Assert.notNull(this.destinationResolver,
					"DestinationResolver is required when relying upon the 'replyDestinationName' property.");
//			return this.destinationResolver.resolveDestinationName(
//					session, this.replyDestinationName, this.replyPubSubDomain);
			this.replyDestination = this.destinationResolver.resolveDestinationName(
					session, this.replyDestinationName, this.replyPubSubDomain);
			return this.replyDestination;
		}
		else {
//			this.replyDestination = this.destinationResolver.resolveDestinationName(
//					session, UUID.randomUUID().toString(), this.replyPubSubDomain);
//			if (this.temporaryReplyDestination == null){
//				this.temporaryReplyDestination = this.destinationResolver.resolveDestinationName(
//						session, "queue_" + this.gatewayId, this.replyPubSubDomain);
//			}
//			return this.temporaryReplyDestination;
			this.replyDestination = session.createTemporaryQueue();
			return this.replyDestination;
		}
		//return session.createTemporaryQueue();

	}

	@Override
	public final void onInit() {
		synchronized (this.initializationMonitor) {
			if (this.initialized) {
				return;
			}
			Assert.notNull(this.connectionFactory, "connectionFactory must not be null");
			Assert.isTrue(this.requestDestination != null
					^ this.requestDestinationName != null
					^ this.requestDestinationExpressionProcessor != null,
					"Exactly one of 'requestDestination', 'requestDestinationName', or 'requestDestinationExpression' is required.");
			if (this.requestDestinationExpressionProcessor != null) {
				this.requestDestinationExpressionProcessor.setBeanFactory(getBeanFactory());
				this.requestDestinationExpressionProcessor.setConversionService(getConversionService());
			}
			this.initialized = true;
		}
	}

	@Override
	protected Object handleRequestMessage(final Message<?> message) {
		if (!this.initialized) {
			this.afterPropertiesSet();
		}
		final Message<?> requestMessage = MessageBuilder.fromMessage(message).build();
		try {
			javax.jms.Message jmsReply = this.sendAndReceive(requestMessage);
			if (jmsReply == null) {
				throw new MessageTimeoutException(message,
						"failed to receive JMS response within timeout of: " + this.receiveTimeout + "ms");
			}
			Object result = jmsReply;
			if (this.extractReplyPayload) {
				result = this.messageConverter.fromMessage(jmsReply);
				if (logger.isDebugEnabled()) {
					logger.debug("converted JMS Message [" + jmsReply + "] to integration Message payload [" + result + "]");
				}
			}
			Map<String, Object> jmsReplyHeaders = this.headerMapper.toHeaders(jmsReply);
			Message<?> replyMessage = null;
			if (result instanceof Message){
				replyMessage = MessageBuilder.fromMessage((Message<?>) result).copyHeaders(jmsReplyHeaders).build();
			}
			else {
				replyMessage = MessageBuilder.withPayload(result).copyHeaders(jmsReplyHeaders).build();
			}
			return replyMessage;
		}
		catch (JMSException e) {
			throw new MessageHandlingException(requestMessage, e);
		}
	}

	private javax.jms.Message sendAndReceive(Message<?> requestMessage) throws JMSException {
		Connection connection = this.createConnection();
		Session session = null;
		Destination replyTo = null;
		try {
			session = this.createSession(connection);

			// convert to JMS Message
			Object objectToSend = requestMessage;
			if (this.extractRequestPayload) {
				objectToSend = requestMessage.getPayload();
			}
			javax.jms.Message jmsRequest = this.messageConverter.toMessage(objectToSend, session);

			// map headers
			headerMapper.fromHeaders(requestMessage.getHeaders(), jmsRequest);

			// TODO: support a JmsReplyTo header in the SI Message?
			replyTo = this.getReplyDestination(session);
			jmsRequest.setJMSReplyTo(replyTo);
			connection.start();

			Integer priority = requestMessage.getHeaders().getPriority();
			if (priority == null) {
				priority = this.priority;
			}
			javax.jms.Message replyMessage = null;
			Destination requestDestination = this.getRequestDestination(requestMessage, session);
			if (this.correlationKey != null) {
				replyMessage = this.doSendAndReceiveWithGeneratedCorrelationId(requestDestination, jmsRequest, replyTo, session, priority);
			}
			else if (replyTo instanceof TemporaryQueue || replyTo instanceof TemporaryTopic) {
//				replyMessage = this.doSendAndReceiveWithTemporaryReplyToDestination(requestDestination, jmsRequest, replyTo, session, priority);
				replyMessage = this.doAutoCorrelatedSendAndReceive(requestDestination, jmsRequest, replyTo, session, priority);
			}
			else {
				replyMessage = this.doAutoCorrelatedSendAndReceive(requestDestination, jmsRequest, replyTo, session, priority);
			}
			return replyMessage;
		}
		finally {
			JmsUtils.closeSession(session);
//			this.deleteDestinationIfTemporary(replyTo);
			ConnectionFactoryUtils.releaseConnection(connection, this.connectionFactory, true);
		}
	}

	/**
	 * Creates the MessageConsumer before sending the request Message since we are generating our own correlationId value for the MessageSelector.
	 */
	private javax.jms.Message doSendAndReceiveWithGeneratedCorrelationId(Destination requestDestination,
			javax.jms.Message jmsRequest, Destination replyTo, Session session, int priority) throws JMSException {
		MessageProducer messageProducer = null;
		MessageConsumer messageConsumer = null;
		try {
			messageProducer = session.createProducer(requestDestination);
			String correlationId = UUID.randomUUID().toString().replaceAll("'", "''");
			Assert.state(this.correlationKey != null, "correlationKey must not be null");
			String messageSelector = null;
			if (this.correlationKey.equals("JMSCorrelationID")) {
				jmsRequest.setJMSCorrelationID(correlationId);
				messageSelector = "JMSCorrelationID = '" + correlationId + "'";
			}
			else {
				jmsRequest.setStringProperty(this.correlationKey, correlationId);
				messageSelector = this.correlationKey + " = '" + correlationId + "'";
			}
			messageConsumer = session.createConsumer(replyTo, messageSelector);
			this.sendRequestMessage(jmsRequest, messageProducer, priority);
			return this.receiveReplyMessage(messageConsumer);
		}
		finally {
			JmsUtils.closeMessageProducer(messageProducer);
			JmsUtils.closeMessageConsumer(messageConsumer);
		}
	}

//	/**
//	 * Creates the MessageConsumer before sending the request Message since we do not need any correlation.
//	 */
//	private javax.jms.Message doSendAndReceiveWithTemporaryReplyToDestination(Destination requestDestination,
//			javax.jms.Message jmsRequest, Destination replyTo, Session session, int priority) throws JMSException {
//		MessageProducer messageProducer = null;
//		MessageConsumer messageConsumer = tempQueueConsumerCache.get();
//		try {
//			messageProducer = session.createProducer(requestDestination);
//			if (messageConsumer == null){
//				messageConsumer = session.createConsumer(replyTo);
//				tempQueueConsumerCache.set(messageConsumer);
//			}
//
//			this.sendRequestMessage(jmsRequest, messageProducer, priority);
//			String messageId = jmsRequest.getJMSMessageID();
//			return this.receiveReplyMessage(messageConsumer);
//		}
//		finally {
//			JmsUtils.closeMessageProducer(messageProducer);
//			//JmsUtils.closeMessageConsumer(messageConsumer);
//		}
//	}

	/**
	 * Creates the MessageConsumer after sending the request Message since we need the MessageID for correlation with a MessageSelector.
	 */
	private javax.jms.Message doAutoCorrelatedSendAndReceive(Destination requestDestination,
			javax.jms.Message jmsRequest, Destination replyTo, Session session, int priority) throws JMSException {
		if (replyTo instanceof Topic && logger.isWarnEnabled()) {
			logger.warn("Relying on the MessageID for correlation is not recommended when using a Topic as the replyTo Destination " +
					"because that ID can only be provided to a MessageSelector after the reuqest Message has been sent thereby " +
					"creating a race condition where a fast response might be sent before the MessageConsumer has been created. " +
					"Consider providing a value to the 'correlationKey' property of this gateway instead. Then the MessageConsumer " +
					"will be created before the request Message is sent.");
		}
		MessageProducer messageProducer = null;
		//MessageConsumer messageConsumer = null;
		MessageConsumer messageConsumer = tempQueueConsumerCache.get();
		String consumerCorrelationId = Thread.currentThread().getId() + "_" + gatewayId;
		try {
			messageProducer = session.createProducer(requestDestination);;
			jmsRequest.setStringProperty("consumerCorrelationId", consumerCorrelationId);
			this.sendRequestMessage(jmsRequest, messageProducer, priority);
			String messageId = jmsRequest.getJMSMessageID();

			//messageConsumer = session.createConsumer(replyTo, messageSelector);
			if (messageConsumer == null){
				String messageSelector = "consumerCorrelationId = '" + consumerCorrelationId + "'";
				messageConsumer = session.createConsumer(replyTo, messageSelector);
				tempQueueConsumerCache.set(messageConsumer);
			}
			return this.receiveReplyMessageCorrelated(messageConsumer, messageId);
		}
		finally {
			JmsUtils.closeMessageProducer(messageProducer);
			if (!(replyTo instanceof TemporaryQueue)){
				JmsUtils.closeMessageConsumer(messageConsumer);
			}
		}
	}

	private void sendRequestMessage(javax.jms.Message jmsRequest, MessageProducer messageProducer, int priority) throws JMSException {
		if (this.explicitQosEnabled) {
			messageProducer.send(jmsRequest, this.deliveryMode, priority, this.timeToLive);
		}
		else {
			messageProducer.send(jmsRequest);
		}
	}

	private javax.jms.Message receiveReplyMessage(MessageConsumer messageConsumer) throws JMSException {
		return (this.receiveTimeout >= 0) ? messageConsumer.receive(receiveTimeout) : messageConsumer.receive();
	}

	private javax.jms.Message receiveReplyMessageCorrelated(MessageConsumer messageConsumer, String originalReplyCorrelationId) throws JMSException {
		javax.jms.Message replyMessage = (this.receiveTimeout >= 0) ? messageConsumer.receive(receiveTimeout) : messageConsumer.receive();
		if (replyMessage != null){
			String replyCorrelationId = replyMessage.getStringProperty("replyCorrelationId");
			if (StringUtils.hasText(replyCorrelationId) && originalReplyCorrelationId.equals(replyCorrelationId)){
				return replyMessage;
			}
			else {
				return this.receiveReplyMessageCorrelated(messageConsumer, originalReplyCorrelationId);
			}
		}
		return null;
	}

//	/**
//	 * Deletes either a {@link TemporaryQueue} or {@link TemporaryTopic}.
//	 * Ignores any other {@link Destination} type and also ignores any
//	 * {@link JMSException}s that may be thrown when attempting to delete.
//	 */
//	private void deleteDestinationIfTemporary(Destination destination) {
//		try {
//			if (destination instanceof TemporaryQueue) {
//				((TemporaryQueue) destination).delete();
//			}
//			else if (destination instanceof TemporaryTopic) {
//				((TemporaryTopic) destination).delete();
//			}
//		}
//		catch (JMSException e) {
//			// ignore
//		}
//	}

	/**
	 * Create a new JMS Connection for this JMS gateway.
	 */
	protected Connection createConnection() throws JMSException {
		return this.connectionFactory.createConnection();
	}

	/**
	 * Create a new JMS Session using the provided Connection.
	 */
	protected Session createSession(Connection connection) throws JMSException {
		return connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
	}

	public void destroy() throws Exception {
		//JmsUtils.
	}

}
