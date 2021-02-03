from flask import Flask, Response, render_template, request,stream_with_context
from kafka import KafkaConsumer,KafkaProducer
import cv2
import numpy as np
from datetime import datetime
import json
from time import sleep
import base64

# Fire up the Kafka Consumer
topic1 = "distributed-video1"
topic2 = "distributed-video2"
topic3 = "webresolution"
topic4 = "mobileresolution"

producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda x: json.dumps(x).encode('utf-8'))

consumer1 = KafkaConsumer(
    topic1, 
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')))

consumer2 = KafkaConsumer(
    topic2, 
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    max_partition_fetch_bytes=2097152)

fourcc = cv2.VideoWriter_fourcc(*'XVID')
webcamWriter=None
mobileCamWriter=None

webcamFlag=True
mobileCamFlag=True
recordWebFlag=False
recordMobFlag=False
webresolution='Auto'
mobileresolution='Auto'
# Set the consumer in a Flask App
app = Flask(__name__)

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

@app.route('/webcam', methods=['GET','POST'])
def webcamStream():
    global webcamFlag,recordWebFlag,webcamWriter,webresolution
    if request.method == 'POST':
        if request.form['submit']=='Stop' and webcamFlag==True:
            webcamFlag=False
        elif request.form['submit']=='Start' and webcamFlag==False:
            webcamFlag=True
        elif request.form['submit']=='Start Recording' and recordWebFlag==False:
            recordWebFlag=True
            time=str(datetime.now().time())
            filename='./recording-WEBCAM--'+time+'.avi'
            webcamWriter = cv2.VideoWriter(filename,fourcc, 20.0, (640,480),1)
        elif request.form['submit']=='Stop Recording' and recordWebFlag==True:
            recordWebFlag=False
            webcamWriter.release()
        elif request.form['submit'] in ['720p','480p','360p','240p','Auto']:
            if webresolution!=request.form['submit']:
                if recordWebFlag==True:
                    webcamWriter.release()
                    time=str(datetime.now().time())
                    filename='./recording-WEBCAM--'+time+'@'+request.form['submit']+'.avi'
                    webcamWriter=cv2.VideoWriter(filename,fourcc,20.0,(640,480),1)
                webresolution=request.form['submit']
                producer.send(topic3,webresolution)
    
    return render_template('webcamStream.html')


@app.route('/mobile', methods=['GET','POST'])
def mobileCamStream():
    global mobileCamFlag,recordMobFlag,mobileCamWriter,mobileresolution
    if request.method == 'POST':
        print(request.form['submit']=='Start')
        if request.form['submit']=='Stop' and mobileCamFlag==True:
            mobileCamFlag=False
        elif request.form['submit']=='Start' and mobileCamFlag==False:
            mobileCamFlag=True
        if request.form['submit']=='Start Recording' and recordMobFlag==False:
            recordMobFlag=True
            time=str(datetime.now().time())
            filename='./recording-MOBILE--'+time+'.avi'
            mobileCamWriter = cv2.VideoWriter(filename,fourcc, 20.0, (640,480),1)
        elif request.form['submit']=='Stop Recording' and recordMobFlag==True:
            recordMobFlag=False
            mobileCamWriter.release()
        elif request.form['submit'] in ['720p','480p','360p','240p','Auto']:
            if mobileresolution!=request.form['submit']:
                if recordMobFlag==True:
                    mobileCamWriter.release()
                    time=str(datetime.now().time())
                    filename='./recording-MOBILE--'+time+'@'+request.form['submit']+'.avi'
                    mobileCamWriter=cv2.VideoWriter(filename,fourcc,20.0,(640,480),1)
                mobileresolution=request.form['submit']
                producer.send(topic4,mobileresolution)

            
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
    if webcamFlag:
        return Response(
            stream_with_context(get_video_stream(consumer1,webcamWriter,recordWebFlag)), 
            mimetype='multipart/x-mixed-replace; boundary=frame')

    return Response()


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
            stream_with_context(get_video_stream(consumer2,mobileCamWriter,recordMobFlag)), 
            mimetype='multipart/x-mixed-replace; boundary=frame')

    return Response()

def get_video_stream(consumer,outFile,flag):
    """
    Here is where we recieve streamed images from the Kafka Server and convert 
    them to a Flask-readable format.
    """
    for msg in consumer:
        decodedFrame=base64.b64decode(msg.value['data'])
        image = np.fromstring(decodedFrame, dtype=np.uint8)
        #print(decodedFrame)
        image = np.reshape(image, (480, 640, 3))
        #print(image)
        ret, frame = cv2.imencode('.jpg', image)
        #print(frame.tobytes() == decodedFrame)
        buf = frame.tobytes()
        yield (b'--frame\r\n'
               b'Content-Type: image/jpg\r\n\r\n' + buf + b'\r\n\r\n')
        if flag==True:
            #image = np.fromstring(decodedFrame, dtype=np.uint8)
            #image = cv2.imdecode(image, cv2.IMREAD_COLOR)
            #print(image)
            outFile.write(image)
        #sleep(0.07)
        
if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)


