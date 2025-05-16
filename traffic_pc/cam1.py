from flask import Flask, Response
import cv2
import threading
import time
import gc
import numpy as np

app = Flask(__name__)

# Camera resolution settings - lower resolution for better performance
CAMERA_WIDTH = 640
CAMERA_HEIGHT = 480
# Frame rate limiter
FPS_LIMIT = 15  # Limit to 15 FPS for smoother performance

# Initialize external cameras with better error handling and alternative index options
print("Attempting to initialize cameras...")

# Try to open cameras with different indices
cameras = []
camera_indices = [0, 1, 2, 3, 4, 5]  # Try more indices to find all cameras

for idx in camera_indices:
    print(f"Attempting to open camera at index {idx}...")
    cap = cv2.VideoCapture(idx)
    if cap.isOpened():
        # Check if we can actually get a frame
        ret, _ = cap.read()
        if ret:
            print(f"✓ Successfully opened camera at index {idx}")
            cameras.append((idx, cap))
            # Stop after finding 4 cameras
            if len(cameras) >= 4:
                break
        else:
            print(f"✗ Camera at index {idx} opened but can't read frames - releasing")
            cap.release()
    else:
        print(f"✗ Failed to open camera at index {idx}")

# Check if we found enough cameras
if not cameras:
    print("ERROR: No cameras found! Please check your camera connections.")
    # Create a blank image to serve when no camera is available
    blank_frame = np.zeros((480, 640, 3), dtype=np.uint8)
    cv2.putText(blank_frame, "NO CAMERA FOUND", (160, 240), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 2)
    _, blank_compressed = cv2.imencode('.jpg', blank_frame)

# Assign cameras to specific positions
cap1 = cameras[0][1] if len(cameras) > 0 else None
cap2 = cameras[1][1] if len(cameras) > 1 else None
cap3 = cameras[2][1] if len(cameras) > 2 else None
cap4 = cameras[3][1] if len(cameras) > 3 else None

# Print detailed camera information
for i, (idx, cap) in enumerate(cameras):
    if cap.isOpened():
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        fps = cap.get(cv2.CAP_PROP_FPS)
        print(f"Camera {i+1} (index {idx}): Resolution {width}x{height}, FPS: {fps}")

# Set camera properties for better performance
for i, (_, cap) in enumerate(cameras):
    if cap.isOpened():
        try:
            cap.set(cv2.CAP_PROP_FRAME_WIDTH, CAMERA_WIDTH)
            cap.set(cv2.CAP_PROP_FRAME_HEIGHT, CAMERA_HEIGHT)
            cap.set(cv2.CAP_PROP_FPS, FPS_LIMIT)
            # Use more efficient codec
            cap.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc(*'MJPG'))
            # Disable autofocus - can cause CPU spikes
            cap.set(cv2.CAP_PROP_AUTOFOCUS, 0)
            print(f"Set properties for camera {i+1}")
        except Exception as e:
            print(f"Warning: Could not set properties for camera {i+1}: {e}")

# Create frame buffer for each camera to reduce direct camera access
frame_buffers = {
    1: {'frame': None, 'last_update': 0, 'lock': threading.Lock()},
    2: {'frame': None, 'last_update': 0, 'lock': threading.Lock()},
    3: {'frame': None, 'last_update': 0, 'lock': threading.Lock()},
    4: {'frame': None, 'last_update': 0, 'lock': threading.Lock()}
}

def capture_camera_frames(camera_id, cap):
    """Thread function to continuously capture frames into buffer"""
    frame_interval = 1.0 / FPS_LIMIT  # Minimum time between frames
    last_frame_time = 0
    
    while True:
        current_time = time.time()
        # Only capture new frame if enough time has passed
        if current_time - last_frame_time >= frame_interval:
            ret, frame = cap.read()
            if ret:
                # Resize if necessary for better performance
                if frame.shape[1] > CAMERA_WIDTH or frame.shape[0] > CAMERA_HEIGHT:
                    frame = cv2.resize(frame, (CAMERA_WIDTH, CAMERA_HEIGHT), 
                                       interpolation=cv2.INTER_AREA)
                    
                # Apply JPEG compression to reduce memory usage
                encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 85]
                _, compressed = cv2.imencode('.jpg', frame, encode_param)
                
                # Update the frame buffer
                with frame_buffers[camera_id]['lock']:
                    frame_buffers[camera_id]['frame'] = compressed
                    frame_buffers[camera_id]['last_update'] = current_time
                
                last_frame_time = current_time
                
                # Explicitly release memory
                del frame
                gc.collect()
            
            # Short sleep to prevent tight CPU loop
            time.sleep(0.01)
        else:
            # Even shorter sleep when skipping frame capture
            time.sleep(0.001)

def generate_camera(camera_id):
    """Generate MJPEG stream from frame buffer"""
    frame_interval = 1.0 / FPS_LIMIT
    last_frame_time = 0
    
    while True:
        current_time = time.time()
        # Rate limit the stream
        if current_time - last_frame_time >= frame_interval:
            # Get frame from buffer if available
            with frame_buffers[camera_id]['lock']:
                compressed_frame = frame_buffers[camera_id]['frame']
                last_update = frame_buffers[camera_id]['last_update']
            
            # Check if we have a valid frame that's not too old
            if compressed_frame is not None and current_time - last_update < 1.0:
                # No need to encode again, buffer already has compressed JPEG
                yield (b'--frame\r\n'
                       b'Content-Type: image/jpeg\r\n\r\n' + 
                       compressed_frame.tobytes() + b'\r\n')
                
                last_frame_time = current_time
            
            # Short sleep when no valid frame
            time.sleep(0.01)
        else:
            # Short sleep when rate limiting
            time.sleep(0.001)

@app.route('/video_feed1')
def video_feed1():
    return Response(generate_camera(1), 
                    mimetype='multipart/x-mixed-replace; boundary=frame')

@app.route('/video_feed2')
def video_feed2():
    return Response(generate_camera(2), 
                    mimetype='multipart/x-mixed-replace; boundary=frame')

@app.route('/video_feed3')
def video_feed3():
    return Response(generate_camera(3), 
                    mimetype='multipart/x-mixed-replace; boundary=frame')

@app.route('/video_feed4')
def video_feed4():
    return Response(generate_camera(4), 
                    mimetype='multipart/x-mixed-replace; boundary=frame')

@app.route('/')
def index():
    return """
    <html>
      <head>
        <title>Multiple Camera Feeds</title>
        <style>
          .camera-container {
            display: grid;
            grid-template-columns: 1fr 1fr;
            grid-gap: 10px;
            width: 100%;
            max-width: 1200px;
            margin: 0 auto;
          }
          .camera {
            width: 100%;
          }
          h2 {
            text-align: center;
          }
        </style>
      </head>
      <body>
        <h1 style="text-align: center;">Camera Feeds</h1>
        <div class="camera-container">
          <div>
            <h2>Camera 1</h2>
            <img class="camera" src="/video_feed1" />
          </div>
          <div>
            <h2>Camera 2</h2>
            <img class="camera" src="/video_feed2" />
          </div>
          <div>
            <h2>Camera 3</h2>
            <img class="camera" src="/video_feed3" />
          </div>
          <div>
            <h2>Camera 4</h2>
            <img class="camera" src="/video_feed4" />
          </div>
        </div>
      </body>
    </html>
    """

if __name__ == '__main__':
    # Start camera capture threads
    for i, (idx, cap) in enumerate(cameras, 1):
        if cap.isOpened():
            print(f"Starting capture thread for camera {i}")
            thread = threading.Thread(target=capture_camera_frames, args=(i, cap), daemon=True)
            thread.start()
        else:
            print(f"Camera {i} not available - skipping thread creation")
    
    # Set Flask to use a production server instead of the development server
    from waitress import serve
    print("Starting server with optimized camera streaming...")
    serve(app, host='0.0.0.0', port=5000, threads=4)
    
    # Release all cameras on shutdown
    for _, cap in cameras:
        cap.release()