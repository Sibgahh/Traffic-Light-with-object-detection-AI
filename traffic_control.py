import cv2
import numpy as np
import time
from ultralytics import YOLO
import RPi.GPIO as GPIO

# GPIO setup for traffic lights
GPIO.setmode(GPIO.BCM)
# Define GPIO pins for traffic lights (adjust these according to your setup)
RED_PIN = 17
YELLOW_PIN = 27
GREEN_PIN = 22

# Setup GPIO pins
GPIO.setup(RED_PIN, GPIO.OUT)
GPIO.setup(YELLOW_PIN, GPIO.OUT)
GPIO.setup(GREEN_PIN, GPIO.OUT)

# Initialize YOLO model
model = YOLO('yolov8n.pt')  # Using YOLOv8 nano model for better performance on RPi

# Define vehicle classes we want to detect
VEHICLE_CLASSES = [2, 3, 5, 7]  # car, motorcycle, bus, truck

# Define regions of interest (ROIs) for each row
# Format: [x1, y1, x2, y2] where (x1,y1) is top-left and (x2,y2) is bottom-right
ROWS = {
    'row1': [100, 100, 500, 200],
    'row2': [100, 250, 500, 350],
    'row3': [100, 400, 500, 500],
    'row4': [100, 550, 500, 650]
}

# Initialize vehicle counts
vehicle_counts = {
    'row1': 0,
    'row2': 0,
    'row3': 0,
    'row4': 0
}

# Minimum and maximum green light duration (in seconds)
MIN_GREEN_DURATION = 5
MAX_GREEN_DURATION = 30

def count_vehicles(frame, row):
    """Count vehicles in a specific row"""
    x1, y1, x2, y2 = ROWS[row]
    roi = frame[y1:y2, x1:x2]
    
    # Run YOLO detection
    results = model(roi)
    
    count = 0
    for result in results:
        boxes = result.boxes
        for box in boxes:
            if int(box.cls) in VEHICLE_CLASSES:
                count += 1
    
    return count

def control_traffic_light(row, duration):
    """Control traffic light for a specific row"""
    # Turn off all lights
    GPIO.output(RED_PIN, GPIO.LOW)
    GPIO.output(YELLOW_PIN, GPIO.LOW)
    GPIO.output(GREEN_PIN, GPIO.LOW)
    
    # Turn on green light for the specified row
    GPIO.output(GREEN_PIN, GPIO.HIGH)
    time.sleep(duration)
    
    # Yellow light transition
    GPIO.output(GREEN_PIN, GPIO.LOW)
    GPIO.output(YELLOW_PIN, GPIO.HIGH)
    time.sleep(2)
    
    # Red light
    GPIO.output(YELLOW_PIN, GPIO.LOW)
    GPIO.output(RED_PIN, GPIO.HIGH)

def calculate_green_duration(count):
    """Calculate green light duration based on vehicle count"""
    if count == 0:
        return MIN_GREEN_DURATION
    return min(MIN_GREEN_DURATION + (count * 2), MAX_GREEN_DURATION)

def main():
    # Initialize webcam
    cap = cv2.VideoCapture(0)
    
    try:
        while True:
            ret, frame = cap.read()
            if not ret:
                break
            
            # Count vehicles in each row
            for row in ROWS:
                count = count_vehicles(frame, row)
                vehicle_counts[row] = count
                
                # Draw ROI rectangle
                x1, y1, x2, y2 = ROWS[row]
                cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 255, 0), 2)
                cv2.putText(frame, f'{row}: {count}', (x1, y1-10),
                           cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)
            
            # Control traffic lights based on vehicle counts
            for row in ROWS:
                count = vehicle_counts[row]
                duration = calculate_green_duration(count)
                control_traffic_light(row, duration)
            
            # Display the frame
            cv2.imshow('Traffic Control', frame)
            
            if cv2.waitKey(1) & 0xFF == ord('q'):
                break
    
    finally:
        cap.release()
        cv2.destroyAllWindows()
        GPIO.cleanup()

if __name__ == "__main__":
    main() 