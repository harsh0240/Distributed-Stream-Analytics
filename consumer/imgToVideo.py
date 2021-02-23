import cv2
import numpy as np
import glob
import os
from time import sleep
from datetime import datetime

font = cv2.FONT_HERSHEY_SIMPLEX

def parse(filename):
    cameraFrame = filename.split('/')[5]
    cameraTimestamp=int(cameraFrame.split('.')[0])
    return cameraTimestamp

def convertToVideo(camID,start,end,size,img_array):
    out = cv2.VideoWriter(camID+'--'+str(start)+'-'+str(end)+'.avi', cv2.VideoWriter_fourcc(*'DIVX'), 10, size)

    if len(img_array)==0:
        print("No motion in the selected time range.")
    else:
        for i in range(len(img_array)):
            out.write(img_array[i])

    out.release()

def findMotion(camID,startTime,endTime,img_array):
    size=(0,0)
    print(camID)
    for filename in sorted(glob.glob('/home/harsh/analysed-data/'+camID+'/*.png')):
        time = parse(filename)
        #print(time)
        if time > endTime:
            break
        elif time >= startTime and time <= endTime:
            img = cv2.imread(filename)
            #adds date-time in the frame
            cv2.putText(img, str(datetime.fromtimestamp(time/1000)), (10,30), font, 0.5, (255, 255, 255), 1, cv2.LINE_AA)
            #adds camera id in the frame
            cv2.putText(img, camID, (10,470), font, 0.5, (0, 0, 0), 1, cv2.LINE_AA)
            height, width, layers = img.shape
            size = (width,height)
            img_array.append(img)
            #os.remove(filename)
    convertToVideo(camID,startTime,endTime,size,img_array)


def stream_video(img_array):
    for img in img_array:
        (flag,encodedImg)=cv2.imencode(".jpg",img)
        yield(b'--frame\r\n' b'Content-Type: image/jpeg\r\n\r\n' + bytearray(encodedImg) + b'\r\n')
        sleep(0.2)

