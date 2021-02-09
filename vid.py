import cv2
import numpy as np
import glob
import os

img_array = []
camID = input()
start = int(input())
end = int(input())

def parse(filename):
	x = filename.split('/')[5]
	return int(x.split('.')[0])

for filename in sorted(glob.glob('/home/harsh/analysed-data/'+camID+'/*.png')):
	time = parse(filename)
	print(time)
	if time > end:
		break
	elif time >= start and time <= end:
		img = cv2.imread(filename)
		print(filename)
		height, width, layers = img.shape
		size = (width,height)
		img_array.append(img)
		#os.remove(filename)


out = cv2.VideoWriter(camID+'--'+str(start)+'-'+str(end)+'.avi', cv2.VideoWriter_fourcc(*'DIVX'), 10, size)

if not img_array:
	print("No motion in the selected time range.")
else:
	for i in range(len(img_array)):
		out.write(img_array[i])

out.release()
