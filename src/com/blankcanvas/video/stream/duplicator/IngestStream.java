package com.blankcanvas.video.stream.duplicator;

import com.wowza.wms.stream.*;

public class IngestStream {
	private IMediaStream stream;
	private String name;

	public IngestStream(IMediaStream stream, String streamName) {
		this.stream = stream;
		this.name = streamName;
	}

	public IMediaStream getStream() {
		return stream;
	}

	public String getName() {
		return name;
	}


	public String getContextStr() {
		return stream.getContextStr();
	}


}
