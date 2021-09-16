package com.blankcanvas.video.stream.duplicator;

import java.util.*;
import java.util.concurrent.*;


import com.wowza.wms.amf.*;
import com.wowza.wms.application.*;
import com.wowza.wms.logging.*;
import com.wowza.wms.stream.*;


public class DuplicatorManager {

	private Map<String, DuplicateGroup> ingestStreamsMap = new ConcurrentHashMap<String, DuplicateGroup>();
    private ExecutorService fallbackExecutorService = Executors.newSingleThreadExecutor();

	private IApplicationInstance appInstance;
	private WMSLogger logger;

	public DuplicatorManager(IApplicationInstance appInstance) {
		this.appInstance = appInstance;
		this.logger = ModuleStreamDuplicator.logger;
	}

	public synchronized void shutdown() {
		this.appInstance = null;
	}

	//
	//
	// ********		Getter/Setters
	//
	//
	
	public DuplicateGroup getDuplicatedStreamInfoBySource(String streamName) {
		return this.ingestStreamsMap.get(streamName);
	}



	private synchronized ExecutorService getExecutorService(String streamName) {
		ExecutorService ret = null;

		// If its a source stream, its not a dup
		DuplicateGroup dupStreamInfo = getDuplicatedStreamInfoBySource(streamName);
		if (dupStreamInfo == null) {
			dupStreamInfo = getDuplicateGroupByDuplicateName(streamName);
		}
		if (dupStreamInfo != null)
			ret = dupStreamInfo.getExecutorService();
		
		if (ret == null)
			ret = fallbackExecutorService;
		
		return ret;
	}

	
	//
	//
	// ********		WSE callbacks
	//
	//


	public void onMetaData(IMediaStream stream, AMFPacket packet) {
		onLivePacket(stream, packet);		
	}

	public void onLivePacket(IMediaStream stream, AMFPacket packet) {
		String streamName = stream.getName();
		
		ExecutorService executor = getExecutorService(streamName);
		
		OnLivePacketRunnable runnable = new OnLivePacketRunnable(streamName, packet);
		executor.execute(runnable);

	}

	public void onPublish(IMediaStream stream, String streamName) {
		// do not kick this into a different thread because:
		// We need to ensure the order of execution to completion: publish before codecs
		// Publish creates a single threaded Executor service for its renditions
		// which the other callbacks will use
		
		//logger.info(">onPublish:" + streamName);
		if (isDuplicateStream(streamName)) {
			//logger.info("- onPublish: Already duplicated, ignoring: " + streamName);
			return;
		}


		DuplicateGroup duplicatedStream = new DuplicateGroup(appInstance, stream, streamName);
		
		addDuplicatedIngestStream(duplicatedStream);
		
		// Start the push
		if (! duplicatedStream.startDuplication()) {
			// failure, so clean up
			IngestStream ingestStream = duplicatedStream.getIngestStream();

			if (ingestStream != null)
				ingestStreamsMap.remove(ingestStream.getName());
		}

		
		//logger.info("<onPublish:" + streamName);
	}

	public void onUnPublish(IMediaStream stream, String streamName) {
		ExecutorService executor = getExecutorService(streamName);
		
		OnUnPublishRunnable runnable = new OnUnPublishRunnable(streamName);
		executor.execute(runnable);
	}

	//
	//
	//  ********	Methods used internal to this class
	//
	//
	
	synchronized void addDuplicatedIngestStream(DuplicateGroup dupStreamInfo) {
		IngestStream ingestStream = dupStreamInfo.getIngestStream();
		if (ingestStream != null)
			this.ingestStreamsMap.put(ingestStream.getName(), dupStreamInfo);
	}


	synchronized void stopDuplication(DuplicateGroup dupStreamInfo) {
		dupStreamInfo.stopDuplication();
		IngestStream ingestStream = dupStreamInfo.getIngestStream();
		if (ingestStream != null)
			this.ingestStreamsMap.remove(ingestStream.getName());
	}

	boolean isDuplicateStream(String streamName) {
		// If its a source stream, its not a dup
		if (getDuplicateGroupBySource(streamName) != null) {
			return false;
		}
		
		// Walk all the streams and determine if it is a duplicate
		if (getDuplicateGroupByDuplicateName(streamName) != null) {
			return true;
		}
		
		return false;
	}

	public DuplicateGroup getDuplicateGroupBySource(String streamName) {
		return this.ingestStreamsMap.get(streamName);
	}

	DuplicateGroup getDuplicateGroupByDuplicateName(String name) {
		Set<String> keys = ingestStreamsMap.keySet();
		for (String streamKey : keys) {
			DuplicateGroup dupGroup = ingestStreamsMap.get(streamKey);
			if (dupGroup.hasDuplicate(name)) {
				return dupGroup;
			}			
		}
		return null;
	}

	
	boolean doesIngestStreamExist(String streamName) {
		return this.ingestStreamsMap.containsKey(streamName);
	}



	class OnUnPublishRunnable implements Runnable {
		private String streamName;
		
		public OnUnPublishRunnable(String streamName) {
			this.streamName = streamName;
		}
		public void run() {
			//logger.info(">OnUnPublishRunnable:" + streamName);
			
			DuplicateGroup dupStreamInfo = getDuplicatedStreamInfoBySource(streamName);
			if (dupStreamInfo != null) {
				stopDuplication(dupStreamInfo);
			}

			//logger.info("<OnUnPublishRunnable:" + streamName);
		}
		
	}

	class OnLivePacketRunnable implements Runnable {
		private String streamName;
		private AMFPacket packet;
		
		public OnLivePacketRunnable(String streamName, AMFPacket packet) {
			this.streamName = streamName;
			this.packet = packet;
		}
		public void run() {
			DuplicateGroup dupStreamInfo = getDuplicatedStreamInfoBySource(streamName);
			if (dupStreamInfo != null) {
				dupStreamInfo.doLivePacket(packet);
			}
		}
		
	}

}
