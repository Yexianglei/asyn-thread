package com.bingo.asyn.thread;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.bingo.asyn.bean.TaskItem;



public class MyThread implements Runnable{

	private static final Logger logger = LoggerFactory.getLogger(MyThread.class);
	
	private Vector<String> runningItems;
	
	private int retryBatchSize;
	
	public MyThread(Vector<String> runningItems, int retryBatchSize, BlockingQueue<TaskItem> queue) {
		super();
		this.runningItems = runningItems;
		this.retryBatchSize = retryBatchSize;
		this.queue = queue;
	}


	private synchronized void removeRunning(String key) {
		
		runningItems.remove(key);
	
	}
	
	public static int i= 0;
	
	private static synchronized void add(){
		i++;
		System.out.println("总共执行�?:"+i);
	}
	/**
	 * 任务队列
	 */
	private BlockingQueue<TaskItem> queue;
	
	
	@Override
	public void run() {
		
		// 此次执行任务的列�?
		List<TaskItem> dataList = new ArrayList<TaskItem>(retryBatchSize);
		
		while (!queue.isEmpty()) {
			
			System.err.println("任务�?始启�?....................");
			
			if (dataList.size() == retryBatchSize) {
				
				break;
				
			}
			
			try {
				
				TaskItem data = queue.take();
				
				dataList.add(data);
				
			} catch (InterruptedException e) {
				
				logger.error("out queue exception", e);
				
			}
			
			System.err.println("任务结束操作....................");
		}
		// execute
		for (TaskItem retryItem : dataList) {
			
			try {
				
				//任务执行次数+1
				retryItem.tryIncrement();
				
				logger.info("task ready {} �?大次�? {} ", retryItem,retryItem.getCount());
				
				//消费任务
				logger.info("消费任务:"+retryItem.getKey());
				 add();
				
			} catch (Exception e) {
				
				logger.error("batchItemProcessor exception {}" + retryItem, e);
				
				e.printStackTrace();
				//出错的话 �? 如果重试次数小于�?大次�? 加入任务队列
					
					if (retryItem.getCount() < retryItem.getMaxCount()) {
					
						try {
						
							queue.put(retryItem);
						
						} catch (InterruptedException e1) {
							// TODO Auto-generated catch block
							
							e1.printStackTrace();
							
							logger.error("put queue exception", e1);
							
							removeRunning(retryItem.getKey());
						
						}
					} else {
						
						removeRunning(retryItem.getKey());
					}
				}
			
			}
		
	}

}
