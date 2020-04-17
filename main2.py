#!/usr/bin/env python

import base64
import threading
import numpy as np
from kafka import KafkaProducer
from kafka import KafkaConsumer
import cv2
from io import BytesIO


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
                image = base64.b64decode(str(message.value)[2:-1])
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
                producer.send('output-topic', encoded)

                if self.stop_event.is_set():
                    break

        consumer.close()
        producer.close(1000)


def main():
    anonymizer = Anonymizer()
    anonymizer.start()


if __name__ == "__main__":
    main()
