import sys
import time
import cv2
from kafka import KafkaProducer,KafkaConsumer
import json
import asyncio
import functools
import datetime
import base64
import numpy as np

topic1 = "distributed-video1"
topic2 = "distributed-video2"
topic3 = "webresolution"
topic4 = "mobileresolution"

webresolution='Auto'
mobileresolution='Auto'
frame_width=8.9 #640/72
frame_height=6.7 #480/72


cameras={1:0, 2:'http://25.95.71.93:8080/video'} #dictionary of cameraId mapped to camera IPs


def force_async(fn):
    '''
    turns a sync function to async function using threads
    '''
    from concurrent.futures import ThreadPoolExecutor
    import asyncio
    pool = ThreadPoolExecutor()

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        future = pool.submit(fn, *args, **kwargs)
        return asyncio.wrap_future(future)  # make it awaitable

    return wrapper

'''
def publish_video(video_file):
	"""
	Publish given video file to a specified Kafka topic. 
	Kafka Server is expected to be running on the localhost. Not partitioned.
	
	:param video_file: path to video file <string>
	"""
	# Start up producer
	producer = KafkaProducer(bootstrap_servers='localhost:9092')

	# Open file
	video = cv2.VideoCapture(video_file)
	
	print('publishing video...')

	while(video.isOpened()):
		success, frame = video.read()

		# Ensure file was read successfully
		if not success:
			print("bad read!")
			break
		
		# Convert image to png
		ret, buffer = cv2.imencode('.jpg', frame)

		# Convert to bytes and send to kafka
		producer.send(topic, buffer.tobytes())

	video.release()
	print('publish complete')
 '''

def convertToJSON(cameraId,currTime,frame_width,frame_height,buffer):
	obj={
		"cameraId": cameraId,
		"timestamp": str(currTime),
		"rows": 480,
		"cols": 640,
		"type": 16,
		"data": base64.b64encode(buffer.tobytes()).decode('utf-8')
	}
	return obj

def get_stream_resolution(topic):
	consumer = KafkaConsumer(
	topic,
	bootstrap_servers=['localhost:9092'],
	auto_offset_reset='latest',
	enable_auto_commit=False,
	value_deserializer=lambda x: json.loads(x.decode('utf-8')))
	global webresolution,mobileresolution

	for message in consumer:
		if topic==topic3:
			webresolution=message.value
		else:
			mobileresolution=message.value

def set_resolution(image,resolution):
	blurImage=None
	if resolution=='240p':
		blurImage=cv2.blur(image,(7,7),0)
	elif resolution=='360p':
		blurImage=cv2.blur(image,(5,5),0)
	elif resolution=='480p':
		blurImage=cv2.blur(image,(3,3),0)
	else:
		blurImage=image
	return blurImage


def publish_camera():
	"""
	Publish camera video stream to specified Kafka topic.
	Kafka Server is expected to be running on the localhost. Not partitioned.
	"""

	# Start up producer
	producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'), batch_size=20971520, max_request_size=2097152, compression_type="gzip")

	
	camera1 = cv2.VideoCapture(cameras[1])
	cameraId1="cam-01"
	#camera2 = cv2.VideoCapture(cameras[2])
	cameraId2="mob-01"
	
	force_async(get_stream_resolution)(topic3)
	force_async(get_stream_resolution)(topic4)

	try:
		while(True):
			
			success, frame = camera1.read()
			frame=cv2.flip(frame,1)
			modifiedFrame=frame
			if webresolution!='Auto':
				modifiedFrame=set_resolution(frame,webresolution)
			#grayframe = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
			
			ret, buffer = cv2.imencode('.jpg', modifiedFrame)
			
			currTime=str(round(time.time() * 1000))
			#path = dir_path + "/" + currTime + ".jpg"
			# save image with lower quality—smaller file size
			#cv2.imwrite(path, frame, [cv2.IMWRITE_JPEG_QUALITY, 50])
			#cv2.imwrite(path, frame)
			#print(cameraId1 + " -- " + currTime)
			
			jsonObj=convertToJSON(cameraId1,currTime,frame_width,frame_height,modifiedFrame)
			producer.send(topic1,jsonObj)
			'''
			success, frame = camera2.read()
			resizedFrame=cv2.resize(frame,(640,480))
			modifiedFrame=resizedFrame
			if mobileresolution!='Auto':
				modifiedFrame=set_resolution(resizedFrame,mobileresolution)
			ret, buffer = cv2.imencode('.jpg', modifiedFrame)
			currTime=str(round(time.time() * 1000))
			jsonObj=convertToJSON(cameraId2,currTime,frame_width,frame_height,modifiedFrame)
			
			producer.send(topic2,jsonObj)          
			'''
	except Exception as e:
		print('EXCEPTION OCCURED: ',e)
		print("\nExiting.")
		sys.exit(1)

	
	camera1.release()
	#camera2.release()

if __name__ == '__main__':
	
	print("publishing feed!")
	publish_camera()

"""
	Producer will publish to Kafka Server a video file given as a system arg. 
	Otherwise it will default by streaming webcam feed.
 
	if(len(sys.argv) > 1):
		video_path = sys.argv[1]
		publish_video(video_path)
	else:
"""
	

