import cv2
import numpy as np
import glob
import os

img_array = []

def parse(filename):
	x = filename.split('-')[4]
	return int(x.split('.')[0])

def findMotion(startTime,endTime):
	for filename in sorted(glob.glob('/home/harsh/analysed-data/*.png')):
		time = parse(filename)
		print(time)
		if time > endTime:
			break
		elif time >= startTime and time <= endTime:
			img = cv2.imread(filename)
			print(filename)
			height, width, layers = img.shape
			size = (width,height)
			img_array.append(img)
			os.remove(filename)

def convertToVideo():
	out = cv2.VideoWriter('project.avi', cv2.VideoWriter_fourcc(*'DIVX'), 10, size)

	if not img_array:
		print("No motion in the selected time range.")
	else:
		for i in range(len(img_array)):
			out.write(img_array[i])

	out.release()

def stream_video():
	for img in img_array:
		yield(img)


