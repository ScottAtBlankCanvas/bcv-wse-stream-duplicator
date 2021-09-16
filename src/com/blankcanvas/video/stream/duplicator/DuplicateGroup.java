package com.blankcanvas.video.stream.duplicator;

import java.util.*;
import java.util.concurrent.*;

import com.wowza.util.*;
import com.wowza.wms.amf.*;
import com.wowza.wms.application.*;
import com.wowza.wms.logging.*;
import com.wowza.wms.stream.*;
import com.wowza.wms.stream.publish.*;
import com.wowza.wms.vhost.*;


public class DuplicateGroup {
	private Map<String, Publisher> publishers = new ConcurrentHashMap<String, Publisher>();

	private DuplicatorManager manager;
	private IApplicationInstance appInstance;
	private String appName;
	
	// Ingest stream info
	private IngestStream ingestStream;

	private WMSLogger logger;

    private ExecutorService executorService = Executors.newSingleThreadExecutor();
	boolean isShuttingDown = false;

	// Set by property
	private boolean debug = false;
	private int numberDuplicates = 1;
	private String targetAppName = null;
	private String duplicatePattern = "${com.wowza.wms.context.StreamName}_${DuplicateCount}";




	public DuplicateGroup(DuplicatorManager manager, IApplicationInstance appInst, IMediaStream stream, String streamName) {
		this.manager = manager;
		this.appInstance = appInst;
		this.appName = appInstance.getApplication().getName();
		this.ingestStream  = new IngestStream(stream, streamName);

		this.logger = ModuleStreamDuplicator.logger;

		WMSProperties props = appInstance.getProperties();

		this.debug = props.getPropertyBoolean(DuplicatorConstants.PROP_DEBUG, debug);
		this.numberDuplicates = props.getPropertyInt(DuplicatorConstants.PROP_NUMBER_DUPS, this.numberDuplicates);
		this.targetAppName  = props.getPropertyStr(DuplicatorConstants.PROP_TARGET_APP, this.appInstance.getApplication().getName());
		this.duplicatePattern = props.getPropertyStr(DuplicatorConstants.PROP_DUPLICATE_PATTERN, this.duplicatePattern);
	}


	//
	//
	// *******	Getters
	//
	//

	public IApplicationInstance getApplicationInstance() {
		return appInstance;
	}

	public WMSLogger getLogger() {
		return logger;
	}
	
	public boolean isDebug() {
		return this.debug;
	}



	public IngestStream getIngestStream() {
		return this.ingestStream;
	}


 /**
  * Get ExecutorService used for thread pool
  * @return
  */
   public ExecutorService getExecutorService() {
       return executorService;
   }
	

	/**
	 * External signal that we should start streaming
	 */
	public synchronized boolean startDuplication() {
		boolean success = false;
		//logger.info(">startDuplication");

		// if we have publishers (shouldn't),  guess we should stop them
		if (! this.publishers.isEmpty())
			stopPublishers();		
		
		success = startPublishers();

		//logger.info("<startStreamPush success:"+success);
		
		return success;
	}




	/**
	 * This stream has ended. Clean up everything that needs cleaning up.
	 */
	public synchronized void stopDuplication() {
		this.isShuttingDown = true;

		// stop all publishers associated with this stream
		stopPublishers();
		

     executorService.shutdown();
     try {
         if (!executorService.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
             executorService.shutdownNow();
         }
     } catch (InterruptedException e) {
         executorService.shutdownNow();
     }
	}


	private void stopPublishers() {
		for (Publisher publisher : publishers.values()) {
			publisher.unpublish();
			publisher.close();
		}
		publishers.clear();
	}


	private boolean startPublishers() {
		boolean success = false;


		String ingestStreamName = ingestStream.getName();
		
		for (int i = 0; i < this.numberDuplicates; i++) {
			String targetStreamName = createDuplicateStreamName(ingestStreamName, i);
			if (this.manager.doesIngestStreamExist(targetStreamName) ||
				this.manager.isDuplicateStream(targetStreamName)) {
				this.logger.warn(String.format("%s: Attempting to duplicate '%s/%s' but stream '%s/%s' already exists.  Skipping", ModuleStreamDuplicator.MODULE_NAME, this.appName, ingestStreamName, this.targetAppName, targetStreamName));
				continue;				
			}

			this.logger.info(String.format("%s: Duplicating stream: '%s/%s' --> '%s/%s'", ModuleStreamDuplicator.MODULE_NAME, this.appName, ingestStreamName, this.targetAppName, targetStreamName));
			success = startPublisher(ingestStreamName, targetStreamName);
			if (! success) return success;
		}
		
		return success;
	}

	private String createDuplicateStreamName(String ingestStreamName, int i) {
		Map<String, String> envMap = new HashMap<String, String>();
		envMap.put("com.wowza.wms.context.VHost", appInstance.getVHost().getName());
		envMap.put("com.wowza.wms.context.Application", appName);
		envMap.put("com.wowza.wms.context.ApplicationInstance", appInstance.getName());
		envMap.put("com.wowza.wms.context.StreamName", ingestStreamName);
		envMap.put("DuplicateCount", ""+i);

		String ret =  SystemUtils.expandEnvironmentVariables(this.duplicatePattern, envMap);

		return ret;
	}


	private boolean startPublisher(String inStreamName, String dstStreamName) {

		try {
			Publisher publisher = Publisher.createInstance(appInstance.getVHost(), targetAppName, IApplicationInstance.DEFAULT_APPINSTANCE_NAME);
			this.publishers.put(dstStreamName, publisher);

			if (publisher == null)
				return false;

			publisher.setStreamType(publisher.getAppInstance().getStreamType());
			publisher.setPublishDataEvents(true);

			publisher.publish(dstStreamName);

			return true;

		} catch (Exception e) {
			String msg = String.format("Publisher failed to publish to : %s/%s", targetAppName, dstStreamName);
			this.logger.info(msg, e);

			return false;
		}
	}


	public void doLivePacket(AMFPacket packet) {
		if (packet == null)
			return;

		for (Publisher publisher : publishers.values()) {
			switch (packet.getType())
			{
			case IVHost.CONTENTTYPE_AUDIO:
				publisher.addAudioData(packet.getData(), packet.getAbsTimecode());
				break;
				
			case IVHost.CONTENTTYPE_VIDEO:
				publisher.addVideoData(packet.getData(), packet.getAbsTimecode());
				break;
				
			case IVHost.CONTENTTYPE_DATA:
			case IVHost.CONTENTTYPE_DATA3:
				publisher.addDataData(packet.getData(), packet.getAbsTimecode());
			}

		}
	}



	public synchronized boolean hasDuplicate(String name) {
		return this.publishers.containsKey(name);
	}

	
}
