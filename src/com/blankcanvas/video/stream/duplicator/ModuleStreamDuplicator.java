package com.blankcanvas.video.stream.duplicator;

import com.wowza.wms.amf.*;
import com.wowza.wms.application.*;
import com.wowza.wms.logging.*;
import com.wowza.wms.media.model.*;
import com.wowza.wms.module.*;
import com.wowza.wms.stream.*;


public class ModuleStreamDuplicator extends ModuleBase {
	public static final String MODULE_NAME = ModuleStreamDuplicator.class.getSimpleName();
	
	public static WMSLogger logger = getLogger();	
	public static boolean debug = false;
	
	private DuplicatorManager manager = null;


	public ModuleStreamDuplicator() {
	}
	
	class StreamListener implements IMediaStreamActionNotify3, IMediaStreamLivePacketNotify {
		@Override
		public void onPublish(IMediaStream stream, String streamName, boolean isRecord, boolean isAppend) {
			manager.onPublish(stream, streamName);
		}

		@Override
		public void onUnPublish(IMediaStream stream, String streamName, boolean isRecord, boolean isAppend) {
			manager.onUnPublish(stream, streamName);
		}

		@Override
		public void onMetaData(IMediaStream stream, AMFPacket packet) {
			manager.onMetaData(stream, packet);
			
		}

		@Override
		public void onLivePacket(IMediaStream stream, AMFPacket packet) {
			manager.onLivePacket(stream, packet);
		}

		
		
		// NOOPs
		public void onCodecInfoVideo(IMediaStream stream, MediaCodecInfoVideo codecInfoVideo) {}

		public void onCodecInfoAudio(IMediaStream stream, MediaCodecInfoAudio codecInfoAudio) {}
		
		public void onStop(IMediaStream stream) {}

		public void onPauseRaw(IMediaStream stream, boolean isPause, double location) {}

		public void onPause(IMediaStream stream, boolean isPause, double location) {}

		public void onPlay(IMediaStream stream, String streamName, double playStart, double playLen, int playReset) {}

		public void onSeek(IMediaStream stream, double location) {}

	}


	public void onAppStart(IApplicationInstance appInstance) {
		logger.info(String.format("%s.onAppStart[%s]", MODULE_NAME, appInstance.getContextStr()));		

		WMSProperties props = appInstance.getProperties();
		debug  = props.getPropertyBoolean(DuplicatorConstants.PROP_DEBUG, false);

		this.manager  = new DuplicatorManager(appInstance);
	}
	
	public void onAppStop(IApplicationInstance appInstance) {
		logger.info(String.format("%s.onAppStop[%s]", MODULE_NAME, appInstance.getContextStr()));		
		
		this.manager.shutdown();		
		this.manager = null;
	}

	@SuppressWarnings("unchecked")
	public void onStreamCreate(IMediaStream stream) {
		//logger.info("onStreamCreate: "+stream.getName());		

		StreamListener streamNotify = new StreamListener();

		WMSProperties props = stream.getProperties();
		synchronized (props) {
			props.put(DuplicatorConstants.STREAM_ACTION_NOTIFIER_ID, streamNotify);
		}
		
		stream.addClientListener(streamNotify);	
		stream.addLivePacketListener(streamNotify);
	}
	
	public void onStreamDestroy(IMediaStream stream) {
		//logger.info("onStreamDestroy: "+stream.getName());		

		WMSProperties props = stream.getProperties();
		synchronized (props) {
			StreamListener streamNotify = (StreamListener) props.getProperty(DuplicatorConstants.STREAM_ACTION_NOTIFIER_ID);
		
			if (streamNotify != null) {
				stream.removeClientListener(streamNotify);
				stream.removeLivePacketListener(streamNotify);
			}
		}		
	}


}
