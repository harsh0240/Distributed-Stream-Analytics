package com.company.streamanalytics;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.opencv.core.*;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.imgproc.Imgproc;

import java.io.File;
import java.sql.Timestamp;
import java.util.*;

public class MotionDetector {
    private static final Logger logger = Logger.getLogger(MotionDetector.class);

    //load native lib
    static {
        System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
    }

    public static String detectMotion(String camId, Iterator<String> frames, String outputDir, String previousProcessedEventData) throws Exception {
        VideoEventData currentProcessedEventData = new VideoEventData();
        Mat frame = null;
        Mat copyFrame = null;
        Mat grayFrame = null;
        Mat firstFrame = null;
        Mat deltaFrame = new Mat();
        Mat thresholdFrame = new Mat();
        ArrayList<Rect> rectArray = new ArrayList<Rect>();
        Gson gson = new Gson();

        //previous processed frame
        if (previousProcessedEventData != null) {
            JSONObject jobj = new JSONObject(previousProcessedEventData);
            Timestamp timestamp = new Timestamp(Long.parseLong(jobj.getString("timestamp")));
            logger.warn("cameraId=" + camId + " previous processed timestamp=" + timestamp);
            System.out.println(timestamp);
            Mat preFrame = getMat(previousProcessedEventData);
            Mat preGrayFrame = new Mat(preFrame.size(), CvType.CV_8UC1);
            Imgproc.cvtColor(preFrame, preGrayFrame, Imgproc.COLOR_BGR2GRAY);
            Imgproc.GaussianBlur(preGrayFrame, preGrayFrame, new Size(3, 3), 0);
            firstFrame = preGrayFrame;
        }

        //sort by timestamp
        ArrayList<VideoEventData> sortedList = new ArrayList<VideoEventData>();
        while(frames.hasNext()){
            VideoEventData data = new VideoEventData(frames.next());
            sortedList.add(data);
        }
        sortedList.sort(Comparator.comparing(VideoEventData::getTimestamp));
        logger.warn("cameraId="+camId+" total frames="+sortedList.size());

        //iterate and detect motion
        for (VideoEventData eventData : sortedList) {
            frame = getMat(eventData);
            copyFrame = frame.clone();
            grayFrame = new Mat(frame.size(), CvType.CV_8UC1);
            Imgproc.cvtColor(frame, grayFrame, Imgproc.COLOR_BGR2GRAY);
            Imgproc.GaussianBlur(grayFrame, grayFrame, new Size(3, 3), 0);
            logger.warn("cameraId=" + camId + " timestamp=" + eventData.getTimestamp());
            //first
            if (firstFrame != null) {
                Core.absdiff(firstFrame, grayFrame, deltaFrame);
                Imgproc.threshold(deltaFrame, thresholdFrame, 20, 255, Imgproc.THRESH_BINARY);
                rectArray = getContourArea(thresholdFrame);
                if (rectArray.size() > 0) {
                    Iterator<Rect> it2 = rectArray.iterator();
                    while (it2.hasNext()) {
                        Rect obj = it2.next();
                        Imgproc.rectangle(copyFrame, obj.br(), obj.tl(), new Scalar(0, 255, 0), 2);
                    }
                    logger.warn("Motion detected for cameraId=" + eventData.getCameraId() + ", timestamp="+ eventData.getTimestamp());
                    //save image file
                    saveImage(copyFrame, eventData, outputDir);
                }
            }
            firstFrame = grayFrame;
            currentProcessedEventData = eventData;
        }

        JsonObject object = new JsonObject();
        object.addProperty("cameraId", currentProcessedEventData.getCameraId());
        object.addProperty("timestamp", "" + currentProcessedEventData.getTimestamp().getTime());
        object.addProperty("rows", currentProcessedEventData.getRows());
        object.addProperty("cols", currentProcessedEventData.getCols());
        object.addProperty("type", currentProcessedEventData.getType());
        object.addProperty("data", currentProcessedEventData.getData());

        return gson.toJson(object);
    }

    //Get Mat from byte[]
    private static Mat getMat(VideoEventData ed) throws Exception{
        Mat mat = new Mat(ed.getRows(), ed.getCols(), ed.getType());
        //System.out.println(Arrays.toString(Base64.getDecoder().decode(ed.getData())));
        //Mat buf = new Mat();
        //buf.put(Base64.getDecoder().decode(ed.getData()).length, 1, Base64.getDecoder().decode(ed.getData()));
        //System.out.println(buf.dump());
        //Mat ret = Imgcodecs.imdecode(buf, Imgcodecs.IMREAD_COLOR);
        //System.out.println(ret);
        mat.put(0, 0, Base64.getDecoder().decode(ed.getData()));
        return mat;
    }

    private static Mat getMat(String ed) throws Exception{
        JSONObject obj = new JSONObject(ed);

        Mat mat = new Mat(obj.getInt("rows"), obj.getInt("cols"), obj.getInt("type"));
        //System.out.println(Arrays.toString(Base64.getDecoder().decode(obj.getString("data"))));
        mat.put(0, 0, Base64.getDecoder().decode(obj.getString("data")));
        return mat;
    }

    //Detect contours
    private static ArrayList<Rect> getContourArea(Mat mat) {
        Mat hierarchy = new Mat();
        Mat image = mat.clone();
        List<MatOfPoint> contours = new ArrayList<MatOfPoint>();
        Imgproc.findContours(image, contours, hierarchy, Imgproc.RETR_EXTERNAL, Imgproc.CHAIN_APPROX_SIMPLE);
        Rect rect = null;
        double maxArea = 300;
        ArrayList<Rect> arr = new ArrayList<Rect>();
        for (int i = 0; i < contours.size(); i++) {
            Mat contour = contours.get(i);
            double contourArea = Imgproc.contourArea(contour);
            if (contourArea > maxArea) {
                rect = Imgproc.boundingRect(contours.get(i));
                arr.add(rect);
            }
        }
        return arr;
    }

    //Save image file
    private static void saveImage(Mat mat,VideoEventData ed,String outputDir){
        String imagePath = outputDir+"/"+ed.getCameraId()+"/"+ed.getTimestamp().getTime()+".png";
        File directory = new File(outputDir+"/"+ed.getCameraId());
        if (!directory.exists()) {
            directory.mkdirs();
        }
        logger.warn("Saving images to "+imagePath);
        boolean result = Imgcodecs.imwrite(imagePath, mat);
        if(!result){
            logger.error("Couldn't save images to path "+outputDir+".Please check if this path exists. This is configured in processed.output.dir key of property file.");
        }
    }

}
