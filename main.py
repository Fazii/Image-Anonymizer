#!/usr/bin/env python

import math

import base64
import cv2
import json
import numpy as np
import threading
import pydicom as dicom
from pydicom import dcmread, dcmwrite
from pydicom.filebase import DicomFileLike
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
                anonymizedImage = self.anonymizeImage(data)
                anonymizedData = self.anonymizeData(data)
                anonymizedDicom = self.anonymizeDicom(data)
                anonymized = {
                    'data': anonymizedData,
                    'image': anonymizedImage,
                    'dicom': anonymizedDicom
                }
                anonymizedJson = json.dumps(anonymized)
                producer.send('output-topic', anonymizedJson.encode('utf-8'))

                if self.stop_event.is_set():
                    break

        consumer.close()
        producer.close(1000)

    def anonymizeImage(self, data):
        if "image" not in data:
            return None

        image = base64.b64decode(data['image'])
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
        encoded = base64.b64encode(buffer_image).decode("utf-8")
        return encoded

    def anonymizeData(self, data):
        if "data" not in data:
            return None

        inputData = data['data']
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

    def anonymizeDicom(self, data):
        if "dicom" not in data:
            return None

        dicom_decoded = base64.b64decode(data['dicom'])
        dataset = dicom.dcmread(BytesIO(dicom_decoded), force=True)

        elements_to_remove = ['PatientID',
                         'PatientName',
                         'PatientBirthDate',
                         'PatientSex',
                         'PatientAge',
                         'PatientWeight']

        for el in elements_to_remove:
            if el in dataset:
                delattr(dataset, el)

        with BytesIO() as buffer:
            memory_dataset = DicomFileLike(buffer)
            dcmwrite(memory_dataset, dataset)
            memory_dataset.seek(0)
            bytes = memory_dataset.read()
            encoded = base64.b64encode(bytes).decode("utf-8")
            return encoded

def main():
    anonymizer = Anonymizer()
    anonymizer.start()


if __name__ == "__main__":
    main()
