import datetime
from flask import Flask, Response, render_template, request
from kafka import KafkaConsumer

# Fire up the Kafka Consumer
topic1 = "distributed-video1"
topic2 = "distributed-video2"

consumer1 = KafkaConsumer(
    topic1, 
    bootstrap_servers=['localhost:9092'])

consumer2 = KafkaConsumer(
    topic2, 
    bootstrap_servers=['localhost:9092'])


webcamFlag=True
mobileCamFlag=True

# Set the consumer in a Flask App
app = Flask(__name__)

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

frame=0
@app.route('/webcam', methods=['GET','POST'])
def webcamStream():
    global webcamFlag
    if request.method == 'POST':
        print(request.form['submit']=='Start')
        if request.form['submit']=='Stop' and webcamFlag==True:
            webcamFlag=False
        elif request.form['submit']=='Start' and webcamFlag==False:
            webcamFlag=True

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
    if webcamFlag:
        return Response(
            get_video_stream(consumer1), 
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
            get_video_stream(consumer2), 
            mimetype='multipart/x-mixed-replace; boundary=frame')

def get_video_stream(consumer):
    """
    Here is where we recieve streamed images from the Kafka Server and convert 
    them to a Flask-readable format.
    """
    for msg in consumer:
        yield (b'--frame\r\n'
               b'Content-Type: image/jpg\r\n\r\n' + msg.value + b'\r\n\r\n')

if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
