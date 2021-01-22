import cv2
import numpy as np
import glob
import os

img_array = []
for filename in sorted(glob.glob('/home/harsh/analysed-data/*.png')):
    img = cv2.imread(filename)
    print(filename)
    height, width, layers = img.shape
    size = (width,height)
    img_array.append(img)
    os.remove(filename)


out = cv2.VideoWriter('project.avi',cv2.VideoWriter_fourcc(*'DIVX'), 20, size)
 
for i in range(len(img_array)):
    out.write(img_array[i])
out.release()
