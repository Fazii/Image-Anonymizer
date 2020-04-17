#!/usr/bin/env python
import base64
import threading
import glob
import numpy as np
from kafka import KafkaProducer
import cv2


class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        producer = KafkaProducer(bootstrap_servers='localhost:9092')

        images = glob.glob("data/*.jpg")

        # cv2.bitwise_not
        for image in images:
            image = cv2.imread(image)
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
            producer.send('my-topic', encoded)

        producer.close(1000)


def main():
    producer = Producer()
    producer.start()


if __name__ == "__main__":
    main()
