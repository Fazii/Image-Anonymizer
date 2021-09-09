#!/usr/bin/env python

import math

import base64
import cv2
import json
import numpy as np
import threading
import pydicom as dicom
from io import BytesIO
from kafka import KafkaConsumer
from kafka import KafkaProducer
from PIL import Image


class Anonymizer(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                 auto_offset_reset='latest',
                                 consumer_timeout_ms=1000)

        consumer.subscribe(['input-topic'])

        while not self.stop_event.is_set():
            for message in consumer:
                data = json.loads(str(message.value)[2:-1])
                anonymizedImage = self.anonymizeImage(data['image'])
                anonymizedData = self.anonymizeData(data['data'])
                anonymizedDicom = self.anonymizeDicom(data['dicom'])
                anonymized = {
                    'data': anonymizedData,
                    'image': anonymizedImage.decode(),
                    'dicom': anonymizedDicom.decode()
                }
                anonymizedJson = json.dumps(anonymized)
                producer.send('output-topic', anonymizedJson.encode('utf-8'))

                if self.stop_event.is_set():
                    break

        consumer.close()
        producer.close(1000)

    def anonymizeImage(self, inputImg):
        image = base64.b64decode(inputImg)
        jpg_as_np = np.frombuffer(image, dtype=np.uint8)
        image = cv2.imdecode(jpg_as_np, flags=1)
        hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)
        lower = np.array([0, 0, 218])
        upper = np.array([157, 54, 255])
        mask = cv2.inRange(hsv, lower, upper)
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (5, 3))
        dilate = cv2.dilate(mask, kernel, iterations=5)
        cnts = cv2.findContours(dilate, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        cnts = cnts[0] if len(cnts) == 2 else cnts[1]
        for c in cnts:
            x, y, w, h = cv2.boundingRect(c)
            ar = w / float(h)
            if ar > 5:
                cv2.drawContours(image, [c], -1, (0, 0, 0), -1)
        _, buffer_image = cv2.imencode('.jpg', image)
        encoded = base64.b64encode(buffer_image)
        return encoded

    def anonymizeData(self, inputData):
        anonymizedData = {
            'wiek': getInterval(inputData['wiek']),
            'wzrost': getInterval(inputData['wzrost']),
            'waga': getInterval(inputData['waga']),
            'choroba': inputData['choroba']
        }
        return anonymizedData


    def getInterval(x):
        floor = int(math.floor(int(x) / 10.0)) * 10
        return "[" + str(floor) + "-" + str(floor + 10) + "]"

    def anonymizeDicom(self, inputDicom):
        dicomImage = base64.b64decode(inputDicom)
        im = dicom.dcmread(BytesIO(dicomImage), force=True)
        im = im.pixel_array.astype(float)
        rescaled_image = (np.maximum(im, 0) / im.max()) * 255
        final_image = np.uint8(rescaled_image)
        final_image = Image.fromarray(final_image)
        buff = BytesIO()
        final_image.save(buff, format="JPEG")
        encoded = base64.b64encode(buff.getvalue()).decode("utf-8")
        return encoded

def main():
    anonymizer = Anonymizer()
    anonymizer.start()


if __name__ == "__main__":
    main()
