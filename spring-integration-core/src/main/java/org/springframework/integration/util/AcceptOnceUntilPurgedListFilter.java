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
package org.springframework.integration.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.CollectionUtils;

/**
 * An implementation of {@link ListFilter} which will queue all items that's been seen until 
 * the queue reaches its capacity after which one item from the queue will be purged to make room for a 
 * new item to be added. Note that however unlikely the removed item will now appear as unprocessed 
 * so it is highly recommended to move/delete resources which corresponds to the underlying items once processing 
 * is done to eliminate duplicate processing.
 * 
 * @author Oleg Zhurakousky
 * @since 2.1
 */
public class AcceptOnceUntilPurgedListFilter<T> implements ListFilter<T> {
	
	private final Log logger = LogFactory.getLog(this.getClass());
	
	private final Queue<T> seenItems;
	
	private final Object seenQueueMonitor = new Object();
	
	public AcceptOnceUntilPurgedListFilter(){
		this(Integer.MAX_VALUE);
	}
	
	public AcceptOnceUntilPurgedListFilter(int maxCapacity){
		seenItems = new LinkedBlockingQueue<T>(maxCapacity);
	}

	private boolean accept(T item) {
		synchronized (this.seenQueueMonitor) {
			boolean accepted = false;
		
			if (!this.seenItems.contains(item)) {
				accepted = this.seenItems.offer(item);
				if (!accepted){
					logger.warn("'seenQueueMonitor' queue of AcceptOnceUntilPurgedListFilter is at the capacity, " +
							"evicting one item to make room for another");
					this.seenItems.poll();
					accepted = this.seenItems.offer(item);
				}
			}
			
			return accepted;
		}
	}

	public List<T> filter(List<T> unfilteredListOfItems) {
		List<T> filteredListOfItems = null;
		
		if (!CollectionUtils.isEmpty(unfilteredListOfItems)){
			filteredListOfItems = new ArrayList<T>();
			for (T item : unfilteredListOfItems) {
				if (this.accept(item)){
					filteredListOfItems.add(item);
				}
			}
		}
		
		return filteredListOfItems;
	}

}
