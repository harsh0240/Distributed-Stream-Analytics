package com.iot.video.app.kafka.collector;

import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.sql.Timestamp;
import java.util.Base64;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.core.Size;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.imgproc.Imgproc;
import org.opencv.videoio.VideoCapture;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

/**
 * Class to convert Video Frame into byte array and generate JSON event using Kafka Producer.
 * 
 * @author abaghel
 *
 */
public class VideoEventGenerator implements Runnable {
	private static final Logger logger = Logger.getLogger(VideoEventGenerator.class);	
	private String cameraId;
	private String url;
	private Producer<String, String> producer;
	private String topic;
	private String dirPath = "/home/harsh/processed-data";
	
	public VideoEventGenerator(String cameraId, String url, Producer<String, String> producer, String topic) {
		this.cameraId = cameraId;
		this.url = url;
		this.producer = producer;
		this.topic = topic;
	}
	
	//load OpenCV native lib
	static {
		System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
	}

	@Override
	public void run() {
		logger.info("Processing cameraId " + cameraId + " with url " + url);
		try {
			generateEvent(cameraId,url,producer,topic);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}
	
	//generate JSON events for frame
	private void generateEvent(String cameraId,String url,Producer<String, String> producer, String topic) throws Exception{
		/*VideoCapture camera = null;
		if(StringUtils.isNumeric(url)){
			camera = new VideoCapture(Integer.parseInt(url));
		}else{
			camera = new VideoCapture(url);
		}
		//check camera working
		if (!camera.isOpened()) {
			Thread.sleep(5000);
			if (!camera.isOpened()) {
				throw new Exception("Error opening cameraId "+cameraId+" with url="+url+".Set correct file path or url in camera.url key of property file.");
			}
		}*/
		Mat mat = new Mat();
        Gson gson = new Gson();
        
		try (WatchService watcher = FileSystems.getDefault().newWatchService()) {
			
			Path dir = Paths.get(dirPath);
			dir.register(watcher, StandardWatchEventKinds.ENTRY_CREATE);
			
			WatchKey key;
			while (true) {
				key = watcher.take();
						
				for (WatchEvent<?> event: key.pollEvents()) {
					Path eventPath = (Path) event.context();
					//System.out.println(eventPath);
					
					String filename = dirPath + "/" + eventPath;
					String file = eventPath.toString();
					mat = Imgcodecs.imread(filename);
					
					//System.out.println(mat.rows() + " : " + mat.cols());
					
					Imgproc.resize(mat, mat, new Size(640, 480), 0, 0, Imgproc.INTER_CUBIC);
					int cols = mat.cols();
			        int rows = mat.rows();
			        int type = mat.type();
					byte[] data = new byte[(int) (mat.total() * mat.channels())];
					mat.get(0, 0, data);
					logger.info("Calculating Time");
					Long time = Long.parseUnsignedLong(file.substring(0, file.indexOf('.'))); 
					logger.info(time.toString());
			        String timestamp = new Timestamp(time).toString();
					JsonObject obj = new JsonObject();
					obj.addProperty("cameraId",cameraId);
			        obj.addProperty("timestamp", timestamp);
			        obj.addProperty("rows", rows);
			        obj.addProperty("cols", cols);
			        obj.addProperty("type", type);
			        obj.addProperty("data", Base64.getEncoder().encodeToString(data));  
			        String json = gson.toJson(obj);
			        producer.send(new ProducerRecord<String, String>(topic,cameraId, json),new EventGeneratorCallback(cameraId));
			        logger.info("Generated events for cameraId="+cameraId+" timestamp="+timestamp);
			        Files.delete(Paths.get(filename));
				}
				
				key.reset();
			}
					
		} catch (Exception e) {
			System.err.print(e);
		}
		
		  
		mat.release();
	}
	
	private class EventGeneratorCallback implements Callback {
		private String camId;

		public EventGeneratorCallback(String camId) {
			super();
			this.camId = camId;
		}

		@Override
		public void onCompletion(RecordMetadata rm, Exception e) {
			if (rm != null) {
				logger.info("cameraId="+ camId + " partition=" + rm.partition());
			}
			if (e != null) {
				e.printStackTrace();
			}
		}
	}

}
