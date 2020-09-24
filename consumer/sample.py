import numpy as np
import urllib.request
import cv2
from datetime import datetime

now=datetime.now().time()
print(now)


with urllib.request.urlopen('http://www.pyimagesearch.com/wp-content/uploads/2015/01/google_logo.png') as url:
    s = url.read()

print(type(s))

image = np.frombuffer(bytearray(s), dtype=np.uint8)
image = cv2.imdecode(image, flags=1)
cv2.imshow('URL2Image',image)
cv2.waitKey()