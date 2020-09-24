import datetime
from flask import Flask, Response, render_template, request
from kafka import KafkaConsumer
import cv2
import numpy as np
from datetime import datetime

# Fire up the Kafka Consumer
topic1 = "distributed-video1"
topic2 = "distributed-video2"

consumer1 = KafkaConsumer(
    topic1, 
    bootstrap_servers=['localhost:9092'])

consumer2 = KafkaConsumer(
    topic2, 
    bootstrap_servers=['localhost:9092'])

fourcc = cv2.VideoWriter_fourcc(*'XVID')
webcamWriter=None
mobileCamWriter=None

webcamFlag=True
mobileCamFlag=True
recordWebFlag=False
recordMobFlag=False
# Set the consumer in a Flask App
app = Flask(__name__)

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

@app.route('/webcam', methods=['GET','POST'])
def webcamStream():
    global webcamFlag,recordWebFlag,webcamWriter
    if request.method == 'POST':
        if request.form['submit']=='Stop' and webcamFlag==True:
            webcamFlag=False
        elif request.form['submit']=='Start' and webcamFlag==False:
            webcamFlag=True
        if request.form['submit']=='Start Recording':
            recordWebFlag=True
            time=str(datetime.now().time())
            filename='./recording-'+time+'.avi'
            webcamWriter = cv2.VideoWriter(filename,fourcc, 20.0, (640,480),1)
        elif request.form['submit']=='Stop Recording' and recordWebFlag==True:
            recordWebFlag=False
            webcamWriter.release()

    return render_template('webcamStream.html')

@app.route('/mobile', methods=['GET','POST'])
def mobileCamStream():
    global mobileCamFlag
    if request.method == 'POST':
        print(request.form['submit']=='Start')
        if request.form['submit']=='Stop' and mobileCamFlag==True:
            mobileCamFlag=False
        elif request.form['submit']=='Start' and mobileCamFlag==False:
            mobileCamFlag=True
            
    return render_template('mobileStream.html')

@app.route('/webcam_feed', methods=['GET'])
def video_feed_web():
    """
    This is the heart of our video display. Notice we set the mimetype to 
    multipart/x-mixed-replace. This tells Flask to replace any old images with 
    new values streaming through the pipeline.
    """
    global webcamFlag
    print(webcamFlag)
    #image = np.asarray(bytearray(frame), dtype="uint8")
    #image = cv2.imdecode(image, cv2.IMREAD_COLOR)
    #cv2.imshow('URL2Image',frame)
    #cv2.waitKey()
    if webcamFlag:
        return Response(
            get_video_stream(consumer1,webcamWriter,recordWebFlag), 
            mimetype='multipart/x-mixed-replace; boundary=frame')


@app.route('/mobile_feed', methods=['GET'])
def video_feed_mobile():
    """
    This is the heart of our video display. Notice we set the mimetype to 
    multipart/x-mixed-replace. This tells Flask to replace any old images with 
    new values streaming through the pipeline.
    """
    global mobileCamFlag
    print(mobileCamFlag)
    if mobileCamFlag:
        return Response(
            get_video_stream(consumer2,mobileCamWriter,recordMobFlag), 
            mimetype='multipart/x-mixed-replace; boundary=frame')

def get_video_stream(consumer,outFile,flag):
    """
    Here is where we recieve streamed images from the Kafka Server and convert 
    them to a Flask-readable format.
    """
    for msg in consumer:
        yield (b'--frame\r\n'
               b'Content-Type: image/jpg\r\n\r\n' + msg.value + b'\r\n\r\n')
        if flag==True:
            image = np.fromstring(msg.value, dtype=np.uint8)
            image = cv2.imdecode(image, cv2.IMREAD_COLOR)
            outFile.write(image)

if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)


