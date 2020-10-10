import sys
import time
import cv2
from kafka import KafkaProducer,KafkaConsumer
from json import loads
import asyncio
import functools

topic1 = "distributed-video1"
topic2 = "distributed-video2"
topic3 = "webresolution"
topic4 = "mobileresolution"

webresolution='360p'
mobileresolution='360p'

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

def get_stream_resolution(topic):
	consumer = KafkaConsumer(
	topic,
	bootstrap_servers=['localhost:9092'],
	auto_offset_reset='latest',
	enable_auto_commit=False,
	value_deserializer=lambda x: loads(x.decode('utf-8')))
	global webresolution,mobileresolution

	for message in consumer:
		if topic==topic3:
			webresolution=message.value
		else:
			mobileresolution=message.value

def publish_camera():
	"""
	Publish camera video stream to specified Kafka topic.
	Kafka Server is expected to be running on the localhost. Not partitioned.
	"""

	# Start up producer
	producer = KafkaProducer(bootstrap_servers='localhost:9092')

	
	camera1 = cv2.VideoCapture(0)
	#camera2 = cv2.VideoCapture('http://192.168.43.85:8080/video')
	
	try:
		while(True):
			success, frame = camera1.read()
			frame=cv2.flip(frame,1)
			force_async(get_stream_resolution)(topic3)
			print(webresolution)
			#grayframe = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
			ret, buffer = cv2.imencode('.jpg', frame)
			producer.send(topic1, buffer.tobytes())


			'''
			success, frame = camera2.read()
			resizedFrame=cv2.resize(frame,(640,480))
			ret, buffer = cv2.imencode('.jpg', resizedFrame)
			producer.send(topic2, buffer.tobytes())  
			'''        
			
	except:
		print("\nExiting.")
		sys.exit(1)

	
	camera1.release()
	camera2.release()

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
	
