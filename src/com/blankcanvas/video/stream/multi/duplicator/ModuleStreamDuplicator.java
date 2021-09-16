package com.blankcanvas.video.stream.multi.duplicator;

import com.wowza.wms.amf.*;
import com.wowza.wms.application.*;
import com.wowza.wms.logging.*;
import com.wowza.wms.media.model.*;
import com.wowza.wms.module.*;
import com.wowza.wms.stream.*;


public class ModuleStreamDuplicator extends ModuleBase {
	private static final String STREAM_ACTION_NOTIFIER_ID = DuplicatorConstants.PROPERTY_PREFIX+".streamListener";
	
	public static WMSLogger logger = getLogger();	
	public static boolean debug = false;
	
	private DuplicatorManager manager = null;


	public ModuleStreamDuplicator() {
	}
	
	class StreamListener implements IMediaStreamActionNotify3, IMediaStreamLivePacketNotify {
		public void onPublish(IMediaStream stream, String streamName, boolean isRecord, boolean isAppend) {
			manager.onPublish(stream, streamName);
		}

		public void onUnPublish(IMediaStream stream, String streamName, boolean isRecord, boolean isAppend) {
			manager.onUnPublish(stream, streamName);
		}

		public void onMetaData(IMediaStream stream, AMFPacket packet) {
			manager.onMetaData(stream, packet);
			
		}

		@Override
		public void onLivePacket(IMediaStream stream, AMFPacket packet) {
			manager.onLivePacket(stream, packet);
		}

		
		public void onCodecInfoVideo(IMediaStream stream, MediaCodecInfoVideo codecInfoVideo) {
//			manager.onCodecInfoVideo(stream, codecInfoVideo);
		}

		public void onCodecInfoAudio(IMediaStream stream, MediaCodecInfoAudio codecInfoAudio) {
//			manager.onCodecInfoAudio(stream, codecInfoAudio);
		}

		
		// NOOPs
		
		public void onStop(IMediaStream stream) {}


		public void onPauseRaw(IMediaStream stream, boolean isPause, double location) {}

		public void onPause(IMediaStream stream, boolean isPause, double location) {}

		public void onPlay(IMediaStream stream, String streamName, double playStart, double playLen, int playReset) {}

		public void onSeek(IMediaStream stream, double location) {}

	}


	public void onAppStart(IApplicationInstance appInstance) {
		logger.info(String.format("ModuleDuplicateMultipleStreams.onAppStart[%s]", appInstance.getContextStr()));		

		WMSProperties props = appInstance.getProperties();
		debug  = props.getPropertyBoolean(DuplicatorConstants.PROP_DEBUG, false);

		this.manager  = new DuplicatorManager(appInstance);
	}
	
	public void onAppStop(IApplicationInstance appInstance) {
		logger.info("onAppStop app:"+appInstance.getApplication().getName());	
		
		this.manager.shutdown();		
		this.manager = null;
	}

	@SuppressWarnings("unchecked")
	public void onStreamCreate(IMediaStream stream) {
		logger.info("onStreamCreate: "+stream.getName());		

		StreamListener streamNotify = new StreamListener();

		WMSProperties props = stream.getProperties();
		synchronized (props) {
			props.put(STREAM_ACTION_NOTIFIER_ID, streamNotify);
		}
		
		stream.addClientListener(streamNotify);	
		stream.addLivePacketListener(streamNotify);
	}
	
	public void onStreamDestroy(IMediaStream stream) {
		logger.info("onStreamDestroy: "+stream.getName());		

		WMSProperties props = stream.getProperties();
		synchronized (props) {
			IMediaStreamActionNotify3 streamNotify = (IMediaStreamActionNotify3) props.getProperty(STREAM_ACTION_NOTIFIER_ID);
		
			if (streamNotify != null)
				stream.removeClientListener(streamNotify);
		}		
	}


}
