# WSE Stream Duplicator

## Introduction

This Wowza Streaming Engine Module detects incoming rtmp stream from one application and creates duplicate streams in a different application.  This is a useful way to do various load test scenarios.

For example, you could publish a stream from OBS called ingest/myStream and create duplicates dup/myStream_000 and dup/myStream_001


## Configuration

- After installing WSE, copy build/bcv-wse-stream-duplicator.jar into <WSE_INSTALL>/lib
- Create an application to receive the ingest stream you would like to duplicate (maybe call it "ingest")
- Create an application which will receive the duplicated streams (maybe call it "dup")
- Configure "ingest" by adding the module and module custom properties:

```
<Module>
	<Name>Stream Duplicator</Name>
	<Description>Dulicates ingest stream</Description>
	<Class>com.blankcanvas.video.stream.duplicator.ModuleStreamDuplicator</Class>
</Module>
```



```
<!-- Application to push duplicate streams to -->
<Property>
	<Name>com.blankcanvas.video.stream.duplicator/targetApplicationName</Name>
	<Value>dup</Value>
</Property>

<!-- How many duplicates to create -->
<Property>
	<Name>com.blankcanvas.video.stream.duplicator/numberDuplicates</Name>
	<Value>2</Value>
	<Type>Integer</Type>
</Property>

<!-- Pattern for duplicate stream name -->
<Property>
	<Name>com.blankcanvas.video.stream.duplicator/duplicatePattern</Name>
	<Value>${com.wowza.wms.context.StreamName}_${DuplicateCount}</Value>
</Property>

<!-- Turn on detailed debugging -->
<Property>
	<Name>com.blankcanvas.video.stream.duplicator/debug</Name>
	<Value>false</Value>
	<Type>Boolean</Type>
</Property>
```

- Restart WSE
- Send a stream to the configured application and verify duplicate stream(s) are created
- When the module is loaded, you should see a log message like this:

```
ModuleStreamDuplicator.onAppStart[ingest/_definst_]	
```


## Building

A simple ant script is provided:

```
ant jar ; to build the jar from source

ant clean; to remove class files and jar file
```

## Developing

The module is best developed using Eclipse.  

Wowza provides instructions for building modules inside of Eclipse.  Follow these and then import the project into Eclipse

The module uses public API calls, as documented with WSE






