/*
 * Copyright 2002-2011 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.springframework.integration.store;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.springframework.integration.Message;
import org.springframework.integration.MessagingException;
import org.springframework.integration.util.UpperBound;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * Map-based in-memory implementation of {@link MessageStore} and {@link MessageGroupStore}. Enforces a maximum capacity for the
 * store.
 * 
 * @author Iwein Fuld
 * @author Mark Fisher
 * @author Dave Syer
 * @author Oleg Zhurakousky
 * 
 * @since 2.0
 */
@ManagedResource
public class SimpleMessageStore extends AbstractMessageGroupStore implements MessageStore, MessageGroupStore {
	
	private final ConcurrentMap<Object, Object> locks = new ConcurrentHashMap<Object, Object>();

	private final ConcurrentMap<UUID, Message<?>> idToMessage;

	private final ConcurrentMap<Object, SimpleMessageGroup> groupIdToMessageGroup;
	
	private final UpperBound individualUpperBound;

	private final UpperBound groupUpperBound;
	
	private volatile String prefix;
	
	/**
	 * Creates a SimpleMessageStore with a maximum size limited by the given capacity, or unlimited size if the given
	 * capacity is less than 1. The capacities are applied independently to messages stored via
	 * {@link #addMessage(Message)} and to those stored via {@link #addMessageToGroup(Object, Message)}. In both cases
	 * the capacity applies to the number of messages that can be stored, and once that limit is reached attempting to
	 * store another will result in an exception.
	 */
	public SimpleMessageStore(int individualCapacity, int groupCapacity) {
		this.idToMessage = new ConcurrentHashMap<UUID, Message<?>>();
		this.groupIdToMessageGroup = new ConcurrentHashMap<Object, SimpleMessageGroup>();
		this.individualUpperBound = new UpperBound(individualCapacity);
		this.groupUpperBound = new UpperBound(groupCapacity);
	}

	/**
	 * Creates a SimpleMessageStore with the same capacity for individual and grouped messages.
	 */
	public SimpleMessageStore(int capacity) {
		this(capacity, capacity);
	}

	/**
	 * Creates a SimpleMessageStore with unlimited capacity
	 */
	public SimpleMessageStore() {
		this(0);
	}

	@ManagedAttribute
	public long getMessageCount() {
		return idToMessage.size();
	}

	public <T> Message<T> addMessage(Message<T> message) {
		if (!individualUpperBound.tryAcquire(0)) {
			throw new MessagingException(this.getClass().getSimpleName()
					+ " was out of capacity at, try constructing it with a larger capacity.");
		}
		this.idToMessage.put(message.getHeaders().getId(), message);
		return message;
	}

	public Message<?> getMessage(UUID key) {
		return (key != null) ? this.idToMessage.get(key) : null;
	}

	public Message<?> removeMessage(UUID key) {
		if (key != null) {
			individualUpperBound.release();
			return this.idToMessage.remove(key);
		}
		else
			return null;
	}

	public MessageGroup getMessageGroup(Object groupId) {
		Assert.notNull(groupId, "'groupId' must not be null");
		
		groupId = this.normalizeGroupId(groupId);

		SimpleMessageGroup group = groupIdToMessageGroup.get(groupId);
		if (group == null) {
			return new SimpleMessageGroup(groupId);
		}
		return new SimpleMessageGroup(group);
	}

	public MessageGroup addMessageToGroup(Object groupId, Message<?> message) {
		Assert.notNull(groupId, "'groupId' must not be null");
		Assert.notNull(message, "'message' must not be null");
		groupId = this.normalizeGroupId(groupId);
		
		if (!groupUpperBound.tryAcquire(0)) {
			throw new MessagingException(this.getClass().getSimpleName()
					+ " was out of capacity at, try constructing it with a larger capacity.");
		}
		Object lock = this.obtainLock(groupId);
		synchronized (lock) {
			SimpleMessageGroup group = this.groupIdToMessageGroup.get(groupId);
			if (group == null) {
				group = new SimpleMessageGroup(groupId);
				this.groupIdToMessageGroup.putIfAbsent(groupId, group);
			}
			group.add(message);
			return group;
		}
	}

	public void removeMessageGroup(Object groupId) {
		Assert.notNull(groupId, "'groupId' must not be null");
		groupId = this.normalizeGroupId(groupId);
		
		Object lock = this.obtainLock(groupId);
		synchronized (lock) {
			if (!groupIdToMessageGroup.containsKey(groupId)) {
				return;
			}
				
			groupUpperBound.release(groupIdToMessageGroup.get(groupId).size());
			groupIdToMessageGroup.remove(groupId);
		}
	}

	public MessageGroup removeMessageFromGroup(Object groupId, Message<?> messageToRemove) {
		Assert.notNull(groupId, "'groupId' must not be null");
		Assert.notNull(messageToRemove, "'messageToRemove' must not be null");
		groupId = this.normalizeGroupId(groupId);
		
		Object lock = this.obtainLock(groupId);
		synchronized (lock) {
			SimpleMessageGroup group = this.groupIdToMessageGroup.get(groupId);
			Assert.notNull(group, "MessageGroup for groupId '" + groupId + "' " +
					"can not be located while attempting to remove Message from the MessageGroup");
			group.remove(messageToRemove);			
			return group;
		}
	}

	public Iterator<MessageGroup> iterator() {
		return new HashSet<MessageGroup>(groupIdToMessageGroup.values()).iterator();
	}
	
	public void setLastReleasedSequenceNumberForGroup(Object groupId, int sequenceNumber) {
		Assert.notNull(groupId, "'groupId' must not be null");
		groupId = this.normalizeGroupId(groupId);
		
		Object lock = this.obtainLock(groupId);
		synchronized (lock) {
			SimpleMessageGroup group = this.groupIdToMessageGroup.get(groupId);
			Assert.notNull(group, "MessageGroup for groupId '" + groupId + "' " +
					"can not be located while attempting to set 'lastReleasedSequenceNumber'");
			group.setLastReleasedMessageSequenceNumber(sequenceNumber);
		}
	}

	public void completeGroup(Object groupId) {
		Assert.notNull(groupId, "'groupId' must not be null");
		groupId = this.normalizeGroupId(groupId);
		
		Object lock = this.obtainLock(groupId);
		synchronized (lock) {
			SimpleMessageGroup group = this.groupIdToMessageGroup.get(groupId);
			Assert.notNull(group, "MessageGroup for groupId '" + groupId + "' " +
					"can not be located while attempting to complete the MessageGroup");
			group.complete();	
		}
	}

	public Message<?> pollMessageFromGroup(Object groupId) {
		Assert.notNull(groupId, "'groupId' must not be null");
		groupId = this.normalizeGroupId(groupId);
		
		Collection<Message<?>> messageList = this.getMessageGroup(groupId).getMessages();
		Message<?> message = null;
		if (!CollectionUtils.isEmpty(messageList)){
			message = messageList.iterator().next();
			if (message != null){
				this.removeMessageFromGroup(groupId, message);
			}
		}	
		return message;
	}
	
	public int messageGroupSize(Object groupId) {
		Assert.notNull(groupId, "'groupId' must not be null");
		groupId = this.normalizeGroupId(groupId);
		
		return this.getMessageGroup(groupId).size();
	}

	public void setGroupPrefix(String prefix) {
		this.prefix = prefix;
	}
	
	private Object obtainLock(Object groupId){
		Object lock = this.locks.get(groupId);
		if (lock == null){
			Object newLock = new Object();
			Object previousLock = this.locks.putIfAbsent(groupId, newLock);
			lock = (previousLock == null) ? newLock : previousLock;
		}
		return lock;
	}
	
	private Object normalizeGroupId(Object groupId){
		if (StringUtils.hasText(prefix)){
			return prefix + groupId;
		}
		else {
			return groupId;
		}
	}
}
