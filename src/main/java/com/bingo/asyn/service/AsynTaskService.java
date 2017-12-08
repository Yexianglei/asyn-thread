package com.bingo.asyn.service;

import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpSession;

import org.apache.commons.lang.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import com.bingo.asyn.bean.TaskItem;
import com.bingo.asyn.thread.MyThread;




@Service
public class AsynTaskService {

	private ScheduledExecutorService scheduledExecutorService; 
	//任务队列
	private BlockingQueue<TaskItem> retryQueue ;
	
	private Vector<String> runningItems = new Vector<String>();
	
	private int threadSize = 1;
	
	private int retryQueueSize = 200000;
	
	private int retryIntervalInSeconds = 10;
	
	private int retryThreadSize = 2;
	
	private int retryBatchSize = 1000;
	
	private static final Logger logger = LoggerFactory.getLogger(AsynTaskService.class);
	
	
	private HttpSession session;
	/**
	 * 队列初始化长度  默认5000
	 */
	public  @PostConstruct void init(){
	
		
		if(retryQueueSize == 0){
			
			retryQueueSize = 5000;
			
		}
		retryQueue = new LinkedBlockingQueue<>(retryQueueSize);
		
		initSchedule();
		
		logger.info("异步服务初始化");
	}
	
	/**
	 * 初始化
	 */
	private void initSchedule() {
		
		scheduledExecutorService = Executors.newScheduledThreadPool(retryThreadSize);
		 
		for (int i = 0; i < retryThreadSize; i++) {
			
			scheduledExecutorService.scheduleAtFixedRate(new MyThread(runningItems, retryBatchSize, retryQueue),retryIntervalInSeconds, retryIntervalInSeconds, TimeUnit.SECONDS);
		
		}
	
	}

	/**
	 * 任务提交
	 */
	public void submit(TaskItem item){
		
		if (StringUtils.isNotBlank(item.getKey()) && !runningItems.contains(item.getKey())) {
			
				try {
					
					Thread.sleep(2000);
					
					retryQueue.put(item);
					
					addRunning(item.getKey());
					
					logger.info(item.getKey()+"加入队列成功..队列长度"+retryQueue.size());
					
				} catch (Exception ex) {
					
					ex.printStackTrace();
				
					logger.error(item.getKey()+"加入队列失败..");
				}
		
		}
		
	}
	public BlockingQueue<TaskItem> getRetryQueue() {
		return retryQueue;
	}

	public void setRetryQueue(BlockingQueue<TaskItem> retryQueue) {
		this.retryQueue = retryQueue;
	}

	public Vector<String> getRunningItems() {
		return runningItems;
	}

	public void setRunningItems(Vector<String> runningItems) {
		this.runningItems = runningItems;
	}

	public int getThreadSize() {
		return threadSize;
	}

	public void setThreadSize(int threadSize) {
		this.threadSize = threadSize;
	}

	public int getRetryQueueSize() {
		return retryQueueSize;
	}

	public void setRetryQueueSize(int retryQueueSize) {
		this.retryQueueSize = retryQueueSize;
	}

	public int getRetryIntervalInSeconds() {
		return retryIntervalInSeconds;
	}

	public void setRetryIntervalInSeconds(int retryIntervalInSeconds) {
		this.retryIntervalInSeconds = retryIntervalInSeconds;
	}

	public int getRetryThreadSize() {
		return retryThreadSize;
	}

	public void setRetryThreadSize(int retryThreadSize) {
		this.retryThreadSize = retryThreadSize;
	}
	
	
	private void addRunning(String asynKey) {
		runningItems.add(asynKey);
	}

	protected void removeRunning(String asynKey) {
		runningItems.remove(asynKey);
	}

	public int getRetryBatchSize() {
		return retryBatchSize;
	}

	public void setRetryBatchSize(int retryBatchSize) {
		this.retryBatchSize = retryBatchSize;
	}
	
	
	
}
