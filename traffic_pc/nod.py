# Required packages:
# pip install ultralytics mysql-connector-python opencv-python numpy requests
# If you encounter problems installing mysql-connector-python, you can try:
# pip install --only-binary :all: mysql-connector-python

import cv2
import time
import numpy as np
from ultralytics import YOLO
import requests
import threading
import queue
from collections import defaultdict
import mysql.connector
from datetime import datetime
import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
import json
import gc  # For garbage collection

# Shared state between camera processors
class SharedState:
    def __init__(self):
        self.lock = threading.Lock()
        self.active_camera = 1  # Start with camera 2 active
        self.next_camera_trigger_time = None
        self.camera_states = {
            1: {'active': True, 'duration_threshold': 15, 'last_send_time': time.time()},
            2: {'active': False, 'duration_threshold': 15, 'last_send_time': time.time()}
        }  # Track state of each camera
        # Add a cooldown timer to prevent rapid switching back to camera 1
        self.last_switch_time = time.time()
        self.switching_cooldown = 1.0  # Reduced from 3.0 to 1.0 second cooldown
        
        # Add storage for camera data to share between instances
        self.camera_data = {
            1: None,
            2: None,
            3: None,
            4: None
        }
        
        # Add data sending status tracking for each camera
        self.data_sending_status = {
            1: {'sending': False, 'completed': True},
            2: {'sending': False, 'completed': True},
            3: {'sending': False, 'completed': True},
            4: {'sending': False, 'completed': True}
        }
        
        # Add flag to prevent camera switching during data sending
        self.switching_blocked = False

# Create a global shared state instance
shared_state = SharedState()

class VideoStreamProcessor:
    def __init__(self, stream_url, model_path='best.pt', confidence=0.25, road_section_id=1):
        """
        Initialize video stream processor with robust vehicle counting
        
        :param stream_url: URL of the video stream
        :param model_path: Path to YOLO model
        :param confidence: Confidence threshold for detections
        :param road_section_id: ID of the road section being monitored
        """
        # Model and detection parameters
        self.model = YOLO(model_path)
        self.confidence = confidence
        self.stream_url = stream_url
        self.road_section_id = road_section_id
        
        # Performance tracking
        self.frame_count = 0
        self.total_fps = 0
        self.is_running = False
        
        # Vehicle classes to count
        self.vehicle_classes = ['mobil', 'truck', 'motor', 'bus']
        
        # Vehicle counting 
        self.vehicle_counts = defaultdict(int)
        self.total_vehicles = 0
        
        # Threading for improved performance - smaller queue sizes to prevent memory issues
        self.frame_queue = queue.Queue(maxsize=64)  # Reduced from 256 to 64
        self.result_queue = queue.Queue(maxsize=32)  # Reduced from 256 to 32
        
        # Detection and display threads
        self.detection_thread = None
        self.processing_thread = None
        self.display_thread = None
        
        # Database connection
        self.db_connection = None
        self.cursor = None
        self.connect_to_database()
        
        # Vehicle type mapping to database IDs
        self.vehicle_type_ids = {
            'mobil': 1,  # Mobil
            'motor': 2,  # Motor
            'truck': 3,  # Truck
            'bus': 4     # Bus
        }
        
        # Get current time for initialization
        current_time = time.time()
        
        # Last MQTT send time
        self.last_mqtt_send_time = current_time
        # Duration threshold in seconds - Using 15 seconds instead of 10 seconds
        self.duration_threshold = 15
        # MQTT topic for duration
        self.mqtt_topic = "traffic/duration"
        # Track current duration seconds remaining
        self.duration_remaining = self.duration_threshold
        # Track if this camera is currently active
        self.is_active = (self.road_section_id == 1)  # Start with camera 2 active
        # Flag to track if we're waiting for MQTT response
        self.waiting_for_mqtt_response = False
        # Track when camera became active for the delay
        self.became_active_time = current_time if self.is_active else None
        # Flag to track if data was sent after the delay
        self.sent_data_after_delay = not self.is_active

        # Add memory tracking
        self.last_gc_time = time.time()
        self.gc_interval = 10.0  # Run garbage collection every 10 seconds

        # MQTT setup
        try:
            # Check which MQTT client initialization approach to use
            if 'has_callback_api' in globals() and has_callback_api:
                # Modern client setup with MQTTv5
                try:
                    self.mqtt_client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
                    print(f"[Camera {self.road_section_id}] Using MQTTv5 with VERSION2 callback API")
                except Exception as client_error:
                    print(f"[Camera {self.road_section_id}] Failed to initialize MQTTv5 client, error: {client_error}")
                    print(f"[Camera {self.road_section_id}] Falling back to MQTTv311 client...")
                    self.mqtt_client = mqtt.Client(protocol=mqtt.MQTTv311)
            else:
                # Fallback to older client initialization
                try:
                    print(f"[Camera {self.road_section_id}] Using older MQTT client API")
                    self.mqtt_client = mqtt.Client(client_id=f"camera_{self.road_section_id}_{int(time.time())}")
                except Exception as fallback_error:
                    print(f"[Camera {self.road_section_id}] Failed to initialize MQTT client, error: {fallback_error}")
                    print(f"[Camera {self.road_section_id}] Using default MQTT client parameters")
                    self.mqtt_client = mqtt.Client()
        except Exception as e:
            print(f"[Camera {self.road_section_id}] Error initializing MQTT client: {e}")

        self.mqtt_client.on_connect = self.on_mqtt_connect
        # Change broker to one with wider accessibility if needed
        self.mqtt_broker = "broker.emqx.io"
        self.mqtt_port = 1883
        # Add client ID to make connection more reliable
        self.client_id = f"camera_{self.road_section_id}_{int(time.time())}"
        self.mqtt_client.client_id = self.client_id
        self.connect_mqtt()

        # Track last logged vehicle counts for change detection
        self.last_logged_vehicle_counts = None
        # Track last sent vehicle count for MQTT
        self.last_sent_vehicle_count = None
        # Store latest duration from MQTT
        self.latest_duration = None
        
        # Display window name
        self.window_name = f"Camera {road_section_id}"
        
        # Register in shared state
        with shared_state.lock:
            # Update camera state if road_section_id exists
            if self.road_section_id in shared_state.camera_states:
                shared_state.camera_states[self.road_section_id].update({
                    'active': self.is_active,
                    'duration_threshold': self.duration_threshold,
                    'last_send_time': self.last_mqtt_send_time
                })
            else:
                # Create entry if it doesn't exist
                shared_state.camera_states[self.road_section_id] = {
                    'active': self.is_active,
                    'duration_threshold': self.duration_threshold,
                    'last_send_time': self.last_mqtt_send_time
                }
                
            # If this is camera 2, set it as active
            if self.road_section_id == 2:
                shared_state.active_camera = 2
                shared_state.next_camera_trigger_time = self.last_mqtt_send_time + self.duration_threshold - 3

    def on_mqtt_connect(self, client, userdata, flags, rc_or_reasoncode, *args):
        """
        MQTT connection callback - handles both MQTTv5 and MQTTv311 protocols
        For MQTTv5: client, userdata, flags, reason_code, properties
        For MQTTv311: client, userdata, flags, rc
        """
        # Get reason code (integer in MQTTv311, object in MQTTv5)
        reason_code = 0
        try:
            # Convert to int if it's not already
            reason_code = int(rc_or_reasoncode)
        except (TypeError, ValueError):
            # If it's an object or otherwise not convertible to int,
            # try to get the reason_code attribute (for MQTTv5)
            try:
                reason_code = rc_or_reasoncode.value
            except:
                # Default to a non-zero code
                reason_code = 1
        
        if reason_code == 0:
            print(f"[Camera {self.road_section_id}] Successfully connected to MQTT broker!")
            # Subscribe to command topics
            self.mqtt_client.subscribe(f"traffic/command/{self.road_section_id}")
            self.mqtt_client.subscribe("traffic/command/all")
            
            # Subscribe to duration topics - with both underscore and dash formats
            self.mqtt_client.subscribe("traffic/duration")
            self.mqtt_client.subscribe("traffic/duration/#")  # Wildcard subscription
            
            print(f"[Camera {self.road_section_id}] Subscribed to command and duration topics")
            
            # Publish connection status again to confirm
            self.mqtt_client.publish(f"traffic/status/{self.road_section_id}", 
                                    json.dumps({
                                        "status": "online",
                                        "camera_id": self.road_section_id,
                                        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                    }), 
                                    qos=1, retain=True)
            
            # Sync camera status - this will also reset the timer if this camera is active
            self.sync_camera_status()
            
            # Initial camera setup: Camera 2 is active at startup
            with shared_state.lock:
                # Only set these settings when Camera 2 connects
                if self.road_section_id == 2:
                    shared_state.active_camera = 2
                    self.is_active = True
                    self.last_mqtt_send_time = time.time()
                    print(f"[Camera 2] Forced to be active at startup")
                    
                    # Make sure all other cameras are set to inactive
                    for cam_id in [1, 3, 4]:
                        if cam_id in shared_state.camera_states:
                            shared_state.camera_states[cam_id]['active'] = False
                    
                    # Update Camera 2 state
                    shared_state.camera_states[2]['active'] = True
                    shared_state.camera_states[1]['last_send_time'] = time.time()
                    
                # For all other cameras, set to inactive
                elif self.road_section_id in [1, 3, 4]:
                    self.is_active = False
                    if self.road_section_id in shared_state.camera_states:
                        shared_state.camera_states[self.road_section_id]['active'] = False
                        
            # Resync camera status after updating shared state
            self.sync_camera_status()
        else:
            print(f"[Camera {self.road_section_id}] Failed to connect to MQTT broker, return code {reason_code}")

    def on_mqtt_message(self, client, userdata, message):
        try:
            payload = message.payload.decode('utf-8', errors='replace')  # Use error handling for decode
            print(f"[Camera {self.road_section_id}] Received message on topic {message.topic}: {payload}")
            
            # Handle duration updates from multiple possible topics
            if message.topic == "traffic/duration" or message.topic.startswith("traffic/duration/"):
                try:
                    data = json.loads(payload)
                    # Check if duration field exists in the payload
                    if "duration" in data:
                        duration = data.get("duration")
                        if duration is not None and isinstance(duration, (int, float)) and duration > 0:
                            # Update duration in this instance
                            self.duration_threshold = int(duration)
                            # Don't reset duration_remaining when receiving updated duration
                            # Only update if duration_remaining is currently greater than the new threshold
                            if self.duration_remaining > self.duration_threshold:
                                self.duration_remaining = self.duration_threshold
                            
                            # Update in shared state for synchronization
                            with shared_state.lock:
                                # Reset the sequence if duration changes
                                shared_state.next_camera_trigger_time = None
                                
                                # Update shared state for all cameras
                                if self.road_section_id in shared_state.camera_states:
                                    shared_state.camera_states[self.road_section_id]['duration_threshold'] = self.duration_threshold
                                    
                                # If we're the active camera, update our timer but preserve countdown
                                if self.is_active:
                                    # Only update last_mqtt_send_time to maintain correct duration_remaining calculation
                                    # Don't reset duration_remaining directly
                                    elapsed = self.duration_threshold - self.duration_remaining
                                    self.last_mqtt_send_time = time.time() - elapsed
                                    shared_state.next_camera_trigger_time = self.last_mqtt_send_time + self.duration_threshold - 3
                            
                            # Reset the waiting flag to allow sending data in the next cycle
                            self.waiting_for_mqtt_response = False
                            
                            print(f"[Camera {self.road_section_id}] Updated duration to {self.duration_threshold} seconds")
                    else:
                        # Keep using default of 15 seconds if no duration in payload
                        print(f"[Camera {self.road_section_id}] No duration in payload, keeping current duration of {self.duration_threshold} seconds")
                except json.JSONDecodeError as e:
                    print(f"[Camera {self.road_section_id}] JSON decode error in duration message: {e}")
                except Exception as e:
                    print(f"[Camera {self.road_section_id}] Error processing duration message: {e}")
                    # Keep using current duration on error
                    print(f"[Camera {self.road_section_id}] Using current duration of {self.duration_threshold} seconds")
                
            # Handle command messages
            elif message.topic == f"traffic/command/{self.road_section_id}" or message.topic == "traffic/command/all":
                try:
                    data = json.loads(payload)
                    command = data.get("command")
                    
                    if command == "update_settings":
                        # Handle settings update
                        if "confidence" in data:
                            self.confidence = float(data["confidence"])
                            print(f"[Camera {self.road_section_id}] Updated confidence threshold to {self.confidence}")
                        
                        if "duration" in data:
                            self.duration_threshold = int(data["duration"])
                            # Update in shared state
                            with shared_state.lock:
                                if self.road_section_id in shared_state.camera_states:
                                    shared_state.camera_states[self.road_section_id]['duration_threshold'] = self.duration_threshold
                            print(f"[Camera {self.road_section_id}] Updated update interval to {self.duration_threshold} seconds")
                    
                    elif command == "send_update":
                        # Force immediate update only if specifically requested
                        print(f"[Camera {self.road_section_id}] Received command to send immediate update")
                        if self.is_active:
                            self.publish_vehicle_count()
                            self.log_traffic_data()
                            self.waiting_for_mqtt_response = True  # Wait for response after manual update
                        else:
                            print(f"[Camera {self.road_section_id}] Ignoring send_update command - camera not active")
                    
                    elif command == "set_active":
                        # Force this camera to be active
                        with shared_state.lock:
                            current_time = time.time()
                            
                            # Always allow Camera 1 to take priority if it specifically received a set_active command
                            # This ensures Camera 1 can always be set active when needed
                            if self.road_section_id == 1 or message.topic == f"traffic/command/1":
                                # Immediately set Camera 1 as active
                                shared_state.active_camera = 1
                                shared_state.last_switch_time = current_time
                                shared_state.next_camera_trigger_time = None
                                
                                # Update camera states
                                if 1 in shared_state.camera_states:
                                    shared_state.camera_states[1]['active'] = True
                                    shared_state.camera_states[1]['last_send_time'] = current_time
                                
                                # Deactivate Camera 2
                                if 2 in shared_state.camera_states:
                                    shared_state.camera_states[2]['active'] = False
                                
                                # Update this camera's status
                                if self.road_section_id == 1:
                                    self.is_active = True
                                    self.last_mqtt_send_time = current_time
                                    self.became_active_time = current_time
                                    self.sent_data_after_delay = False
                                    print(f"[Camera 1] Forced to be active with priority")
                                else:
                                    self.is_active = False
                                    print(f"[Camera 2] Set to standby as Camera 1 was activated")
                                
                            # For Camera 2, check cooldown period
                            elif self.road_section_id == 2:
                                # Check if we're in the cooldown period and trying to set camera 2 active
                                if current_time - shared_state.last_switch_time < shared_state.switching_cooldown:
                                    print(f"[Camera {self.road_section_id}] Cannot set active during cooldown period")
                                else:
                                    # Set the switch time
                                    shared_state.last_switch_time = current_time
                                    
                                    # Update active camera
                                    shared_state.active_camera = self.road_section_id
                                    shared_state.next_camera_trigger_time = None
                                    self.is_active = True
                                    # Adjust last_mqtt_send_time to maintain current duration_remaining
                                    # Only do this if we already have a valid duration_remaining
                                    if hasattr(self, 'duration_remaining') and self.duration_remaining > 0:
                                        elapsed = self.duration_threshold - self.duration_remaining
                                        self.last_mqtt_send_time = current_time - elapsed
                                    else:
                                        self.last_mqtt_send_time = current_time
                                    self.became_active_time = current_time
                                    self.sent_data_after_delay = False
                                    
                                    # Make sure the other camera is set to standby
                                    if 1 in shared_state.camera_states:
                                        shared_state.camera_states[1]['active'] = False
                                        
                                    # Update this camera's status in shared state
                                    if 2 in shared_state.camera_states:
                                        shared_state.camera_states[2]['active'] = True
                                        
                                    print(f"[Camera {self.road_section_id}] Forced to be active camera")
                    
                    # Special command for Camera 1 to force sending data to DB and MQTT
                    elif command == "force_send_data" and self.road_section_id == 1:
                        print(f"[Camera 1] Received command to force send data")
                        # Only send if we're active
                        if self.is_active:
                            # Send data immediately regardless of state
                            self.log_traffic_data()
                            self.publish_vehicle_count()
                            print(f"[Camera 1] Forced data send completed")
                        else:
                            print(f"[Camera 1] Ignoring force_send_data command - camera not active")
                    
                    # Acknowledge command receipt
                    self.mqtt_client.publish(f"traffic/command_ack/{self.road_section_id}", 
                                           json.dumps({
                                               "received": command,
                                               "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                           }), 
                                           qos=1)
                except json.JSONDecodeError as e:
                    print(f"[Camera {self.road_section_id}] JSON decode error in command message: {e}")
                except Exception as e:
                    print(f"[Camera {self.road_section_id}] Error processing command: {e}")
        except Exception as e:
            print(f"[Camera {self.road_section_id}] Error processing MQTT message: {e}")
            # Proceed without crashing

    def connect_mqtt(self):
        print(f"[Camera {self.road_section_id}] Connecting to MQTT broker at {self.mqtt_broker} with client ID {self.client_id}...")
        try:
            # Set message callback
            self.mqtt_client.on_message = self.on_mqtt_message
            # Set last will message
            self.mqtt_client.will_set(f"traffic/status/{self.road_section_id}", "offline", qos=1, retain=True)
            
            # Configure automatic reconnect
            self.mqtt_client.reconnect_delay_set(min_delay=1, max_delay=10)
            
            # Connect with timeout
            self.mqtt_client.connect(self.mqtt_broker, self.mqtt_port, 60)
            # Start the loop
            self.mqtt_client.loop_start()
            # Publish connection status
            self.mqtt_client.publish(f"traffic/status/{self.road_section_id}", "online", qos=1, retain=True)
            # For debugging - publish a test message
            self.mqtt_client.publish(f"traffic/test/{self.road_section_id}", f"Camera {self.road_section_id} connected at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", qos=1)
            print(f"[Camera {self.road_section_id}] MQTT connection established and status published")
            
            # Sync camera status
            self.sync_camera_status()
        except Exception as e:
            print(f"[Camera {self.road_section_id}] Error connecting to MQTT broker: {e}")
            # Wait a bit before returning to avoid rapid reconnection attempts
            time.sleep(1)

    def sync_camera_status(self):
        """Synchronize camera status with shared state and notify other components"""
        try:
            previous_active_state = self.is_active
            
            with shared_state.lock:
                # Update our active status from shared state
                self.is_active = (shared_state.active_camera == self.road_section_id)
                
                # Update active status in shared state
                if self.road_section_id in shared_state.camera_states:
                    shared_state.camera_states[self.road_section_id]['active'] = self.is_active
                
                # If we just became active, reset our timer
                if self.is_active and not previous_active_state:
                    current_time = time.time()
                    self.last_mqtt_send_time = current_time
                    # Don't reset duration_remaining when becoming active
                    # This allows the countdown to continue from its current value
                    self.became_active_time = current_time
                    self.sent_data_after_delay = False
                    
                    # Also update in shared state
                    if self.road_section_id in shared_state.camera_states:
                        shared_state.camera_states[self.road_section_id]['last_send_time'] = current_time
                    
                    # Special debug for Camera 1
                    if self.road_section_id == 1:
                        print(f"[IMPORTANT] Camera 1 became active, continuing with current duration")
                
            # Publish current status
            status = "active" if self.is_active else "standby"
            self.mqtt_client.publish(f"traffic/camera_status/{self.road_section_id}", 
                                    json.dumps({
                                        "status": status,
                                        "road_section_id": self.road_section_id,
                                        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                    }), 
                                    qos=1, retain=True)
            
            print(f"[Camera {self.road_section_id}] Synchronized status: {status}")
        except Exception as e:
            print(f"[Camera {self.road_section_id}] Error synchronizing camera status: {e}")

    def connect_to_database(self):
        """
        Connect to the MySQL database
        """
        try:
            # Close existing connection if any
            if self.db_connection is not None:
                try:
                    self.cursor.close()
                    self.db_connection.close()
                except:
                    pass  # Ignore errors during cleanup
            
            # Additional retry logic for Camera 1
            max_retries = 3 if self.road_section_id == 1 else 1
            retry_count = 0
            last_error = None
            
            while retry_count < max_retries:
                try:
                    self.db_connection = mysql.connector.connect(
                        host="api-traffic-light.apotekbless.my.id",
                        user="u190944248_traffic_light",
                        password="TrafficLight2025.",
                        database="u190944248_traffic_light",
                        connection_timeout=5,  # Reduced from 10 to 5 for faster response
                        autocommit=True
                    )
                    self.cursor = self.db_connection.cursor()
                    print(f"[Camera {self.road_section_id}] Connected to database")
                    return True
                except mysql.connector.Error as err:
                    last_error = err
                    retry_count += 1
                    if retry_count < max_retries:
                        # Only retry for Camera 1 with increasing backoff
                        retry_delay = 0.2 * retry_count
                        print(f"[Camera {self.road_section_id}] Connection attempt {retry_count} failed: {err}. Retrying in {retry_delay:.1f}s...")
                        time.sleep(retry_delay)
                    else:
                        break
            
            # If we got here, all retries failed
            if last_error:
                print(f"[Camera {self.road_section_id}] Database connection error after {retry_count} attempts: {last_error}")
            self.db_connection = None
            self.cursor = None
            
            # Special handling for Camera 1 - try to create at least a minimal connection
            if self.road_section_id == 1:
                try:
                    print(f"[Camera 1] Attempting fallback minimal connection...")
                    # Try with minimal options (no database selection)
                    self.db_connection = mysql.connector.connect(
                        host="api-traffic-light.apotekbless.my.id",
                        user="u190944248_traffic_light",
                        password="TrafficLight2025.",
                        connection_timeout=3
                    )
                    # Then select the database
                    self.db_connection.database = "u190944248_traffic_light"
                    self.cursor = self.db_connection.cursor()
                    print(f"[Camera 1] Established fallback minimal connection")
                    return True
                except Exception as fallback_err:
                    print(f"[Camera 1] Fallback connection failed: {fallback_err}")
            
            return False
        except Exception as err:
            print(f"[Camera {self.road_section_id}] Unexpected error during database connection: {err}")
            self.db_connection = None
            self.cursor = None
            return False

    def log_traffic_data(self):
        """
        Log current vehicle counts to the database
        """
        # Set the waiting flag to show alert in display
        self.waiting_for_mqtt_response = True

        # Check if we're in the process of switching cameras
        current_time = time.time()
        with shared_state.lock:
            switching_in_progress = (current_time - shared_state.last_switch_time < 1.0)
        
        if switching_in_progress and not self.is_active:
            # Camera 1 should log data even during switching to ensure it's always sending
            print(f"[Camera {self.road_section_id}] Skipping database logging during camera switch")
            self.waiting_for_mqtt_response = False  # Reset flag if we're skipping
            return
        
        # Check if we should log based on conditions:
        # Only if we're the active camera or in transition period before switch
        should_log = False
        is_transition = False
        
        with shared_state.lock:
            # Check if we're the active camera
            if shared_state.active_camera == self.road_section_id:
                should_log = True
            
            # Check if we're in the transition period (about to become active)
            elif (shared_state.next_camera_trigger_time is not None and 
                current_time >= shared_state.next_camera_trigger_time and
                self.road_section_id == (shared_state.active_camera % 4 + 1)):  # Next camera in sequence
                should_log = True
                is_transition = True
        
        if not should_log:
            print(f"[Camera {self.road_section_id}] Skipping database log - camera not active or transitioning")
            self.waiting_for_mqtt_response = False  # Reset flag if we're not logging
            return
            
        # Check database connection and reconnect if needed
        if not self.db_connection or not self.cursor:
            print(f"[Camera {self.road_section_id}] Database connection not available. Trying to reconnect...")
            connection_success = self.connect_to_database()
            if not connection_success:
                print(f"[Camera {self.road_section_id}] Failed to reconnect to database.")
                # For Camera 1, try one more time with a short delay
                if self.road_section_id == 1:
                    time.sleep(0.5)
                    connection_success = self.connect_to_database()
                    if not connection_success:
                        print(f"[Camera 1] Second attempt to reconnect to database failed.")
                        self.waiting_for_mqtt_response = False  # Reset flag if connection failed
                        return
                else:
                    self.waiting_for_mqtt_response = False  # Reset flag if connection failed
                    return
            
        db_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            # Check if connection is still alive
            try:
                self.db_connection.ping(reconnect=True, attempts=3, delay=1)
            except:
                # Force reconnection if ping fails
                print(f"[Camera {self.road_section_id}] Database connection lost. Reconnecting...")
                connection_success = self.connect_to_database()
                if not connection_success:
                    print(f"[Camera {self.road_section_id}] Failed to reconnect to database.")
                    # For Camera 1, try one more time with a short delay
                    if self.road_section_id == 1:
                        time.sleep(0.5)
                        connection_success = self.connect_to_database()
                        if not connection_success:
                            print(f"[Camera 1] Second attempt to reconnect to database failed.")
                            self.waiting_for_mqtt_response = False  # Reset flag if reconnection failed
                            return
                    else:
                        self.waiting_for_mqtt_response = False  # Reset flag if reconnection failed
                        return
            
            # Determine which camera's data to send based on the sequential pattern
            # Camera 1 sends Camera 2's data, Camera 2 sends Camera 3's data, etc.
            target_camera_id = self.road_section_id % 4 + 1  # 1→2, 2→3, 3→4, 4→1
            target_vehicle_counts = {}
            
            # Try to get the target camera's data from shared state
            with shared_state.lock:
                if shared_state.camera_data[target_camera_id] is not None:
                    target_vehicle_counts = shared_state.camera_data[target_camera_id].get('vehicle_counts', {})
                    print(f"[Camera {self.road_section_id}] Logging Camera {target_camera_id}'s data from shared state")
                else:
                    # If no target data, use our own data but with the target camera ID
                    target_vehicle_counts = self.vehicle_counts
                    print(f"[Camera {self.road_section_id}] No data for Camera {target_camera_id}, using own data with target ID")
            
            # Always log, even if all counts are zero
            has_data = False
            
            # Insert counts for each vehicle type
            for vehicle_type, count in target_vehicle_counts.items():
                if vehicle_type in self.vehicle_type_ids:
                    vehicle_type_id = self.vehicle_type_ids[vehicle_type]
                    
                    # Insert into traffic_logs table using target_camera_id, not self.road_section_id
                    insert_query = """
                    INSERT INTO traffic_logs 
                    (road_section_id, datetime, vehicle_type_id, amount) 
                    VALUES (%s, %s, %s, %s)
                    """
                    values = (target_camera_id, db_time, vehicle_type_id, count)
                    
                    self.cursor.execute(insert_query, values)
                    has_data = True
            
            # If we didn't find any valid vehicle types, log a zero count for a default type
            if not has_data:
                # Log a zero count for "mobil" (ID 1) as default
                insert_query = """
                INSERT INTO traffic_logs 
                (road_section_id, datetime, vehicle_type_id, amount) 
                VALUES (%s, %s, %s, %s)
                """
                values = (target_camera_id, db_time, 1, 0)  # Use mobil (ID 1) with zero count and target camera ID
                self.cursor.execute(insert_query, values)
            
            # Commit the transaction
            self.db_connection.commit()
            
            # Simplified status message
            if is_transition:
                print(f"[Camera {self.road_section_id}] Traffic data for Camera {target_camera_id} logged to database during transition")
            else:
                print(f"[Camera {self.road_section_id}] Traffic data for Camera {target_camera_id} logged to database")
            
            # Keep waiting_for_mqtt_response flag true for a short time to show the alert
            # It will be reset in the display loop after the alert duration
                
        except mysql.connector.Error as err:
            print(f"[Camera {self.road_section_id}] Database error: {err}")
            # Try to reconnect if connection was lost
            if err.errno in (2006, 2013, 2003, 2002):  # Various MySQL connection loss errors
                print(f"[Camera {self.road_section_id}] Connection lost, attempting to reconnect...")
                self.connect_to_database()
                
                # For Camera 1, attempt to retry the operation immediately
                if self.road_section_id == 1:
                    try:
                        print(f"[Camera 1] Retrying database operation immediately")
                        # Delay slightly to let the reconnection stabilize
                        time.sleep(0.3)
                        
                        # Recreate the cursor if needed
                        if not self.cursor:
                            self.cursor = self.db_connection.cursor()
                            
                        # Determine target camera ID for Camera 1 (should be Camera 2)
                        retry_target_id = 2  # Camera 1 sends Camera 2's data
                            
                        # Log a default entry for Camera 1 to ensure data is sent
                        insert_query = """
                        INSERT INTO traffic_logs 
                        (road_section_id, datetime, vehicle_type_id, amount) 
                        VALUES (%s, %s, %s, %s)
                        """
                        values = (retry_target_id, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 1, 0)
                        self.cursor.execute(insert_query, values)
                        self.db_connection.commit()
                        print(f"[Camera 1] Retry operation successful for Camera {retry_target_id} data")
                    except Exception as retry_err:
                        print(f"[Camera 1] Retry operation failed: {retry_err}")
                        self.waiting_for_mqtt_response = False  # Reset flag if retry failed
            
            # Don't retry immediately to prevent overwhelming the server
            time.sleep(0.5)  # Reduced from 1 second for faster response

    def fetch_frames(self):
        """
        Continuously fetch frames from the stream
        """
        retry_count = 0
        max_retries = 5
        retry_delay = 2  # seconds
        
        while self.is_running:
            try:
                print(f"[Camera {self.road_section_id}] Connecting to stream: {self.stream_url}")
                stream = requests.get(self.stream_url, stream=True, timeout=10)
                
                if stream.status_code != 200:
                    print(f"[Camera {self.road_section_id}] Failed to connect to stream, status code: {stream.status_code}")
                    retry_count += 1
                    if retry_count > max_retries:
                        print(f"[Camera {self.road_section_id}] Maximum retries reached. Stopping processor.")
                        self.is_running = False
                        break
                    print(f"[Camera {self.road_section_id}] Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    continue
                
                # Reset retry count on successful connection
                retry_count = 0
                bytes_data = bytes()
                
                print(f"[Camera {self.road_section_id}] Successfully connected to stream")
                
                # Use a moderate chunk size to balance speed and memory usage
                for chunk in stream.iter_content(chunk_size=16384):  # Decreased from 32768 to 16384
                    if not self.is_running:
                        break
                        
                    bytes_data += chunk
                    
                    # Find all JPEG frames in the current buffer
                    # This allows processing multiple frames at once
                    start_idx = 0
                    while True:
                        a = bytes_data.find(b'\xff\xd8', start_idx)
                        if a == -1:
                            # No more start markers
                            if start_idx > 0:
                                # Keep the remaining data for next iteration
                                bytes_data = bytes_data[start_idx:]
                            break
                            
                        b = bytes_data.find(b'\xff\xd9', a)
                        if b == -1:
                            # End marker not found, keep searching in next chunk
                            break
                            
                        # Extract the JPEG frame
                        jpg_data = bytes_data[a:b+2]
                        
                        # Skip to next potential frame
                        start_idx = b + 2
                        
                        # Skip frames if queue is getting full - more aggressive frame skipping
                        if self.frame_queue.qsize() > 50:  # Decreased from 200 to 50
                            # Skip 80% of frames when queue is filling up
                            if np.random.rand() < 0.8:  # Increased from 67% to 80%
                                continue
                        
                        # Decode the frame
                        frame = cv2.imdecode(np.frombuffer(jpg_data, dtype=np.uint8), cv2.IMREAD_COLOR)
                        
                        if frame is None:
                            continue
                            
                        # Add the frame to the queue for processing
                        if not self.frame_queue.full():
                            # Resize large frames before adding to queue to save memory
                            h, w = frame.shape[:2]
                            if w > 960 or h > 720:
                                frame = cv2.resize(frame, (0, 0), fx=0.5, fy=0.5, interpolation=cv2.INTER_AREA)
                            self.frame_queue.put(frame)
                            
                            # Force garbage collection periodically
                            current_time = time.time()
                            if current_time - self.last_gc_time > self.gc_interval:
                                gc.collect()
                                self.last_gc_time = current_time
                    
                    # If we've consumed all data, reset the buffer
                    if start_idx >= len(bytes_data):
                        bytes_data = bytes()
            except requests.exceptions.RequestException as e:
                print(f"[Camera {self.road_section_id}] Connection error: {e}")
                retry_count += 1
                if retry_count > max_retries:
                    print(f"[Camera {self.road_section_id}] Maximum retries reached. Stopping processor.")
                    self.is_running = False
                    break
                print(f"[Camera {self.road_section_id}] Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            except Exception as e:
                print(f"[Camera {self.road_section_id}] Error fetching stream: {e}")
                time.sleep(1)  # Brief pause before retry
                
    def process_frames(self):
        """
        Process frames for object detection and vehicle counting
        """
        # Track when we last sent data
        last_data_sent_time = time.time()
        # Add a flag to track when cameras become active
        camera_activation_times = {1: None, 2: None, 3: None, 4: None}
        
        # Performance optimizations
        # Skip more frames for detection to improve processing speed
        frame_counter = 0
        detection_interval = 4  # Increased from 2 to 4 - only process every 4th frame for detection
        
        # Add a variable to handle downsampling for inactive cameras
        inactive_detection_interval = 8  # Process every 8th frame when camera is inactive
        
        # Add variables to reduce processing during camera transitions
        switching_mode = False
        switching_start_time = 0
        switching_cooldown_period = 1.5  # seconds to reduce processing load during switching
        
        # Track data sending status for this camera
        data_sent_in_current_period = False
        data_send_initiated = False
        
        while self.is_running:
            try:
                # Initialize current_time at the beginning of each loop iteration
                current_time = time.time()
                
                # Check if we're in camera switching mode
                with shared_state.lock:
                    time_since_switch = current_time - shared_state.last_switch_time
                    if time_since_switch < switching_cooldown_period:
                        switching_mode = True
                        switching_start_time = shared_state.last_switch_time
                    else:
                        switching_mode = False
                
                if not self.frame_queue.empty():
                    frame = self.frame_queue.get()
                    frame_counter += 1
                    
                    # First handle the camera activation logic
                    with shared_state.lock:
                        # Check if this camera just became active
                        if self.is_active == False and shared_state.active_camera == self.road_section_id:
                            # We just became active
                            print(f"[CRITICAL] Camera {self.road_section_id} detected it just became active")
                            camera_activation_times[self.road_section_id] = current_time
                            self.became_active_time = current_time
                            self.sent_data_after_delay = False
                            self.is_active = True
                            self.last_mqtt_send_time = current_time
                            self.duration_remaining = self.duration_threshold
                            # Also update in shared state
                            shared_state.camera_states[self.road_section_id]['last_send_time'] = current_time
                            shared_state.camera_states[self.road_section_id]['active'] = True
                            # Reset data sending flags
                            data_sent_in_current_period = False
                            data_send_initiated = False
                            # Reset shared state status
                            if self.road_section_id in shared_state.data_sending_status:
                                shared_state.data_sending_status[self.road_section_id]['sending'] = False
                                shared_state.data_sending_status[self.road_section_id]['completed'] = False
                            
                            # No longer force immediate data send when camera becomes active
                            # Only send data in the last 5 seconds
                        
                        # Handle camera deactivation and switching
                        if self.is_active:
                            elapsed_time = int(current_time - self.last_mqtt_send_time)
                            
                            # Only calculate the remaining time, don't reset it
                            self.duration_remaining = max(0, self.duration_threshold - elapsed_time)
                            
                            # Check if this camera has completed sending data when duration is up
                            if elapsed_time >= self.duration_threshold:
                                # Check if we have completed sending data
                                data_sending_complete = True
                                if self.road_section_id in shared_state.data_sending_status:
                                    # If we're still sending data, don't switch yet
                                    if shared_state.data_sending_status[self.road_section_id]['sending'] == True:
                                        data_sending_complete = False
                                        shared_state.switching_blocked = True
                                        print(f"[Camera {self.road_section_id}] Blocking camera switch - still sending data")
                                    # If data hasn't been sent at all, force sending
                                    elif not data_sent_in_current_period and not data_send_initiated:
                                        print(f"[Camera {self.road_section_id}] Duration elapsed but data not sent yet - forcing send")
                                        # Use threading to prevent blocking
                                        threading.Timer(0.1, self.log_traffic_data).start()
                                        threading.Timer(0.3, self.publish_vehicle_count).start()
                                        data_send_initiated = True
                                        shared_state.data_sending_status[self.road_section_id]['sending'] = True
                                        shared_state.switching_blocked = True
                                        data_sending_complete = False
                                    # If data has been sent, we can proceed with switching
                                    else:
                                        data_sending_complete = True
                                        shared_state.switching_blocked = False
                                        
                                # Only switch if data sending is complete
                                if data_sending_complete and not shared_state.switching_blocked:
                                    # Determine the next camera in sequence (round-robin: 1->2->3->4->1)
                                    next_camera_id = self.road_section_id % 4 + 1
                                    
                                    print(f"[Camera {self.road_section_id}] Duration elapsed, data sending complete - switching to Camera {next_camera_id}")
                                    shared_state.active_camera = next_camera_id
                                    shared_state.last_switch_time = current_time
                                    self.is_active = False
                                    
                                    # Update all camera states
                                    for cam_id in range(1, 5):
                                        is_next = (cam_id == next_camera_id)
                                        if cam_id in shared_state.camera_states:
                                            shared_state.camera_states[cam_id]['active'] = is_next
                                            if is_next:
                                                # Reset the next camera's timer
                                                shared_state.camera_states[cam_id]['last_send_time'] = current_time
                                                print(f"[CRITICAL] Explicitly setting Camera {cam_id}'s last_send_time when switching")
                                                # Reset data sending status for the next camera
                                                if cam_id in shared_state.data_sending_status:
                                                    shared_state.data_sending_status[cam_id]['sending'] = False
                                                    shared_state.data_sending_status[cam_id]['completed'] = False
                                    
                                    # Don't reset duration_remaining here to avoid visual jumps
                                    # just update the last_mqtt_send_time
                                    self.last_mqtt_send_time = current_time
                                    
                                    # Reset our own data sending status
                                    data_sent_in_current_period = False
                                    data_send_initiated = False
                                    
                                    # Clear frame queues to prevent backlog processing during switching
                                    self.clear_queues()
                                else:
                                    # If we've hit our duration but data sending isn't complete, wait
                                    print(f"[Camera {self.road_section_id}] Duration elapsed but waiting for data sending to complete...")
                        
                        # Check cooldown period - only relevant for cameras that aren't currently active
                        cooldown_active = (shared_state.active_camera != self.road_section_id and
                                          current_time - shared_state.last_switch_time < shared_state.switching_cooldown)
                        
                        # Update our active status from shared state unless in cooldown
                        if not cooldown_active:
                            self.is_active = (shared_state.active_camera == self.road_section_id)
                            
                            # Update the active status in shared state as well
                            if self.road_section_id in shared_state.camera_states:
                                shared_state.camera_states[self.road_section_id]['active'] = self.is_active
                        
                        # If we're the active camera, update next camera trigger time
                        if self.is_active:
                            # Calculate when the next camera should start (3 sec before our duration ends)
                            elapsed_time = int(current_time - self.last_mqtt_send_time)
                            self.duration_remaining = max(0, self.duration_threshold - elapsed_time)
                            
                            # If we're approaching the end of our duration, trigger next camera
                            if self.duration_remaining <= 3 and shared_state.next_camera_trigger_time is None:
                                # Next camera ID in round-robin sequence
                                next_camera_id = self.road_section_id % 4 + 1
                                
                                # Set the next camera to be triggered
                                shared_state.next_camera_trigger_time = current_time
                                print(f"[Camera {self.road_section_id}] Triggering next camera ({next_camera_id}) in {self.duration_remaining} seconds")
                                
                                # Make sure the next camera has its timer reset
                                if next_camera_id in shared_state.camera_states:
                                    shared_state.camera_states[next_camera_id]['last_send_time'] = current_time
                                    print(f"[CRITICAL] Setting Camera {next_camera_id}'s last_send_time to current time to ensure full duration")
                            
                            # If it's time to switch cameras at the end of our duration
                            if self.duration_remaining == 0:
                                # Only if data sending is complete and not blocked
                                data_sending_complete = True
                                if self.road_section_id in shared_state.data_sending_status:
                                    if shared_state.data_sending_status[self.road_section_id]['sending'] == True:
                                        data_sending_complete = False
                                
                                if data_sending_complete and not shared_state.switching_blocked:
                                    # Next camera ID in round-robin sequence
                                    next_camera_id = self.road_section_id % 4 + 1
                                    
                                    # Skip cooldown check - immediately switch to next camera
                                    should_switch = True
                                    
                                    if should_switch:
                                        # Set the switch time to enforce cooldown
                                        shared_state.last_switch_time = current_time
                                        
                                        # Switch active camera
                                        shared_state.active_camera = next_camera_id
                                        shared_state.next_camera_trigger_time = None
                                        print(f"\n{'='*50}")
                                        print(f"[CAMERA SWITCH] Camera {self.road_section_id} switching to Camera {next_camera_id} at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
                                        print(f"{'='*50}\n")
                                        
                                        # Update this camera's status to standby
                                        self.is_active = False
                                        
                                        # Update all camera states
                                        for cam_id in range(1, 5):
                                            is_next = (cam_id == next_camera_id)
                                            if cam_id in shared_state.camera_states:
                                                shared_state.camera_states[cam_id]['active'] = is_next
                                                if is_next:
                                                    # Reset the next camera's timer
                                                    shared_state.camera_states[cam_id]['last_send_time'] = current_time
                                                    print(f"[CRITICAL] Setting Camera {cam_id}'s last_send_time to ensure full duration")
                                                    # Reset data sending status for the next camera
                                                    if cam_id in shared_state.data_sending_status:
                                                        shared_state.data_sending_status[cam_id]['sending'] = False
                                                        shared_state.data_sending_status[cam_id]['completed'] = False
                                        
                                        # Don't reset duration_remaining here either
                                        # just update last_mqtt_send_time
                                        self.last_mqtt_send_time = current_time
                                        
                                        # Reset our own data sending status
                                        data_sent_in_current_period = False
                                        data_send_initiated = False
                                        
                                        # Clear frame queues to prevent backlog processing
                                        self.clear_queues()
                    
                    # Skip detection on some frames to improve performance - more aggressive skipping
                    should_skip_detection = False
                    
                    # During camera switching, be much more aggressive with skipping frames
                    if switching_mode:
                        should_skip_detection = (frame_counter % 10 != 0)  # Process only every 10th frame
                    # For inactive cameras, we skip more frames to save resources
                    elif not self.is_active:
                        should_skip_detection = (frame_counter % inactive_detection_interval != 0)
                    # For active cameras with large queue, skip some frames
                    elif self.frame_queue.qsize() > 40:  # Reduced from 100 to 40
                        should_skip_detection = (frame_counter % detection_interval != 0)
                    # For normal processing with small queue
                    elif self.frame_queue.qsize() > 10:  # Reduced from 20 to 10
                        should_skip_detection = (frame_counter % 2 != 0)
                    
                    # Always process at least some frames for vehicle detection
                    if should_skip_detection:
                        # Skip detection but still display frame
                        if not self.result_queue.full():
                            self.result_queue.put((frame, None, []))
                        continue
                    
                    # Optimize detection by reducing resolution for better performance
                    # Always resize large frames for detection to prevent slowdowns
                    original_height, original_width = frame.shape[:2]
                    detection_frame = frame
                    scale_factor = 1.0
                    
                    # More aggressive resizing to prevent freezes
                    if original_width > 640 or original_height > 480:
                        target_width = 640
                        scale_factor = target_width / original_width
                        new_height = int(original_height * scale_factor)
                        detection_frame = cv2.resize(frame, (target_width, new_height), interpolation=cv2.INTER_AREA)
                    
                    # During switching, use even smaller resolution for detection
                    if switching_mode:
                        target_width = 320
                        scale_factor = target_width / original_width
                        new_height = int(original_height * scale_factor)
                        detection_frame = cv2.resize(frame, (target_width, new_height), interpolation=cv2.INTER_AREA)
                    
                    # Run detection on the processed frame
                    results = self.model.predict(detection_frame, conf=self.confidence, verbose=False)
                    
                    # Calculate and track FPS
                    end_time = time.time()
                    
                    # Vehicle counting logic - optimized to do less work
                    current_vehicle_counts = defaultdict(int)
                    current_total_vehicles = 0
                    frame_vehicles = []
                    
                    if len(results) > 0 and len(results[0].boxes) > 0:
                        for box in results[0].boxes.data:
                            x1, y1, x2, y2, conf, cls = box
                            # Scale back to original dimensions if resized
                            if scale_factor != 1.0:
                                x1, x2 = x1/scale_factor, x2/scale_factor
                                y1, y2 = y1/scale_factor, y2/scale_factor
                            
                            class_name = self.model.names[int(cls)]
                            
                            # Check if detection is a vehicle
                            if class_name in self.vehicle_classes:
                                current_vehicle_counts[class_name] += 1
                                current_total_vehicles += 1
                                frame_vehicles.append((class_name, (x1, y1, x2, y2, conf, cls)))
                    
                    # Compare with last logged counts to see if we need to update our internal state
                    vehicle_counts_changed = (self.last_logged_vehicle_counts is None or 
                                            dict(current_vehicle_counts) != self.last_logged_vehicle_counts)
                    
                    # Update internal state with new counts
                    self.vehicle_counts = current_vehicle_counts
                    self.total_vehicles = current_total_vehicles
                    
                    if vehicle_counts_changed:
                        self.last_logged_vehicle_counts = dict(current_vehicle_counts)
                        
                        # Store our current data in shared state for other cameras to access
                        with shared_state.lock:
                            # Create the data structure for our current state
                            camera_data = {
                                "road_section_id": self.road_section_id,
                                "total_vehicles": self.total_vehicles,
                                "vehicle_counts": dict(current_vehicle_counts),
                                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            }
                            # Store in shared state
                            shared_state.camera_data[self.road_section_id] = camera_data
                            
                        # Only send data in the last 5 seconds of the active period
                        if self.is_active and self.duration_remaining <= 5 and not self.waiting_for_mqtt_response and not data_send_initiated:
                            # Check if we're in the process of switching cameras
                            switching_in_progress = False
                            with shared_state.lock:
                                switching_in_progress = (current_time - shared_state.last_switch_time < 1.0)
                            
                            # Only send data if we're not in the process of switching
                            if not switching_in_progress:
                                print(f"[Camera {self.road_section_id}] Sending data in last 5 seconds of period - {self.duration_remaining}s remaining")
                                
                                # Mark data sending as in progress in shared state
                                with shared_state.lock:
                                    if self.road_section_id in shared_state.data_sending_status:
                                        shared_state.data_sending_status[self.road_section_id]['sending'] = True
                                        shared_state.data_sending_status[self.road_section_id]['completed'] = False
                                
                                # Use threading to prevent blocking
                                threading.Timer(0.1, self.log_traffic_data).start()
                                threading.Timer(0.3, self.publish_vehicle_count).start()
                                
                                # Update tracking variables
                                self.waiting_for_mqtt_response = True
                                data_send_initiated = True
                                last_data_sent_time = current_time
                                print(f"[Camera {self.road_section_id}] Sent data and waiting for MQTT response")
                            else:
                                print(f"[Camera {self.road_section_id}] Skipping data send during camera switch")
                    
                    # Always update the display regardless of active status
                    if not self.result_queue.full():
                        self.result_queue.put((frame, results, frame_vehicles))
                    
                    # Force garbage collection periodically
                    if current_time - self.last_gc_time > self.gc_interval:
                        gc.collect()
                        self.last_gc_time = current_time
                else:
                    # Short sleep to prevent tight CPU loop when queue is empty
                    time.sleep(0.001)  # Slightly increased from 0.0005 to 0.001 for lower CPU usage
            except Exception as e:
                print(f"[Camera {self.road_section_id}] Detection error: {e}")
                import traceback
                traceback.print_exc()
                
                # Force garbage collection after exceptions
                gc.collect()

    def clear_queues(self):
        """Clear frame and result queues to prevent backlog during camera switching"""
        try:
            # Clear frame queue
            while not self.frame_queue.empty():
                try:
                    self.frame_queue.get_nowait()
                except:
                    break
                
            # Clear result queue
            while not self.result_queue.empty():
                try:
                    self.result_queue.get_nowait()
                except:
                    break
                
            print(f"[Camera {self.road_section_id}] Cleared queues during camera switch")
        except Exception as e:
            print(f"[Camera {self.road_section_id}] Error clearing queues: {e}")

    def display_results(self):
        """
        Display processed frames with detections and vehicle counts
        """
        # Track FPS for display
        fps_counter = 0
        fps_timer = time.time()
        current_fps = 0
        last_frame_time = time.time()
        
        # Performance optimization - add frame skipping for higher display FPS
        skip_frames = 0
        
        # Reduce display processing frequency to prevent UI freezes
        display_interval = 2  # Only process every 2nd frame for display
        frame_counter = 0
        
        # Track when data is being sent for visual alert
        sending_data = False
        sending_data_start_time = 0
        sending_data_duration = 1.5  # Show alert for 1.5 seconds
        
        while self.is_running:
            try:
                if not self.result_queue.empty():
                    frame_counter += 1
                    frame, results, vehicles = self.result_queue.get()
                    
                    # Skip frames for display to maintain responsiveness
                    if frame_counter % display_interval != 0 and self.result_queue.qsize() > 10:
                        continue
                    
                    # Resize large frames for display to improve performance
                    h, w = frame.shape[:2]
                    if w > 960:
                        display_frame = cv2.resize(frame, (960, int(h * 960 / w)), interpolation=cv2.INTER_AREA)
                    else:
                        display_frame = frame.copy()
                    
                    # Draw detection boxes and information - optimized
                    if vehicles and len(vehicles) > 0:
                        for vehicle_class, (x1, y1, x2, y2, conf, cls) in vehicles:
                            # Scale coordinates if we resized for display
                            if w > 960:
                                scale = 960 / w
                                x1, x2 = int(x1 * scale), int(x2 * scale)
                                y1, y2 = int(y1 * scale), int(y2 * scale)
                            
                            # Use simpler drawing operations
                            cv2.rectangle(display_frame, (int(x1), int(y1)), (int(x2), int(y2)), (0, 255, 0), 2)
                    
                    # Calculate FPS for display - limit frequency
                    current_time = time.time()
                    if current_time - fps_timer >= 0.5:  # Update FPS every 0.5 seconds
                        current_fps = fps_counter / (current_time - fps_timer)
                        fps_counter = 0
                        fps_timer = current_time
                    else:
                        fps_counter += 1
                    
                    # Add status information to the frame
                    active_text = "Active" if self.is_active else "Standby"
                    fps_text = f"FPS: {current_fps:.1f}"
                    status_text = f"Camera {self.road_section_id}: {active_text}"
                    count_text = f"Vehicles: {self.total_vehicles}"
                    
                    # Use optimized text rendering
                    cv2.putText(display_frame, status_text, (10, 30), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 0), 2)
                    cv2.putText(display_frame, count_text, (10, 70), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 0), 2)
                    cv2.putText(display_frame, fps_text, (10, 110), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 0), 2)
                    
                    # Display remaining time for active camera
                    if self.is_active:
                        time_text = f"Time: {self.duration_remaining}s"
                        cv2.putText(display_frame, time_text, (10, 150), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 0), 2)
                    
                    # Check if we need to show sending data alert
                    # Check the waiting_for_mqtt_response flag or if it's time to send data
                    current_time = time.time()
                    if self.waiting_for_mqtt_response:
                        sending_data = True
                        sending_data_start_time = current_time
                    elif sending_data and (current_time - sending_data_start_time) < sending_data_duration:
                        # Continue showing the alert for the specified duration
                        sending_data = True
                    else:
                        sending_data = False
                    
                    # If in active state and duration_remaining <= 5, show preparing to send
                    preparing_to_send = self.is_active and self.duration_remaining <= 5 and not sending_data
                    
                    # Add visual alert for data sending
                    if sending_data:
                        # Draw a prominent alert box with text
                        overlay = display_frame.copy()
                        cv2.rectangle(overlay, (display_frame.shape[1]//2 - 150, 10), 
                                      (display_frame.shape[1]//2 + 150, 60), (0, 0, 255), -1)
                        alpha = 0.7  # Transparency factor
                        cv2.addWeighted(overlay, alpha, display_frame, 1 - alpha, 0, display_frame)
                        cv2.putText(display_frame, "SENDING DATA", 
                                    (display_frame.shape[1]//2 - 120, 45), 
                                    cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2)
                    elif preparing_to_send:
                        # Draw a yellow alert for preparing to send
                        overlay = display_frame.copy()
                        cv2.rectangle(overlay, (display_frame.shape[1]//2 - 150, 10), 
                                      (display_frame.shape[1]//2 + 150, 60), (0, 165, 255), -1)
                        alpha = 0.7  # Transparency factor
                        cv2.addWeighted(overlay, alpha, display_frame, 1 - alpha, 0, display_frame)
                        cv2.putText(display_frame, "PREPARING TO SEND", 
                                    (display_frame.shape[1]//2 - 145, 45), 
                                    cv2.FONT_HERSHEY_SIMPLEX, 0.9, (255, 255, 255), 2)
                    
                    # Show the frame
                    cv2.imshow(self.window_name, display_frame)
                    
                    # Adding a shorter waitKey time to prevent freezing
                    key = cv2.waitKey(1)
                    if key == 27:  # ESC key
                        self.is_running = False
                        break
                    
                    # Release memory explicitly
                    del display_frame
                else:
                    # Shorter sleep when no frames are waiting
                    time.sleep(0.005)
                    
                    # Force garbage collection periodically
                    current_time = time.time()
                    if current_time - self.last_gc_time > self.gc_interval:
                        gc.collect()
                        self.last_gc_time = current_time
                        
                    # Heartbeat for display to prevent Windows "not responding"
                    if current_time - last_frame_time > 0.5:
                        cv2.waitKey(1)  # Process UI events
                        last_frame_time = current_time
            except Exception as e:
                print(f"[Camera {self.road_section_id}] Display error: {e}")
                import traceback
                traceback.print_exc()
                # Force garbage collection after exceptions
                gc.collect()

    def run(self):
        """
        Start video stream processing
        """
        self.is_running = True
        
        # Create and start threads
        self.detection_thread = threading.Thread(target=self.fetch_frames)
        self.processing_thread = threading.Thread(target=self.process_frames)
        self.display_thread = threading.Thread(target=self.display_results)
        
        self.detection_thread.start()
        self.processing_thread.start()
        self.display_thread.start()
        
        try:
            # Wait for threads to complete
            self.detection_thread.join()
            self.processing_thread.join()
            self.display_thread.join()
        except Exception as e:
            print(f"[Camera {self.road_section_id}] Error in run method: {e}")
        finally:
            # Cleanup
            print(f"[Camera {self.road_section_id}] Cleaning up resources...")
            try:
                cv2.destroyWindow(self.window_name)
            except:
                pass
                
            # Close database connection
            if self.db_connection:
                try:
                    self.cursor.close()
                    self.db_connection.close()
                    print(f"[Camera {self.road_section_id}] Database connection closed.")
                except:
                    pass
            
            # Publish offline status before stopping MQTT client
            try:
                self.mqtt_client.publish(f"traffic/status/{self.road_section_id}", "offline", qos=1, retain=True)
                self.mqtt_client.loop_stop()
                self.mqtt_client.disconnect()
                print(f"[Camera {self.road_section_id}] Disconnected from MQTT broker")
            except:
                pass
            
            print(f"[Camera {self.road_section_id}] Shutdown complete.")

    def publish_vehicle_count(self):
        """Publish vehicle count data to MQTT broker."""
        try:
            # Set waiting_for_mqtt_response flag to trigger alert in display
            self.waiting_for_mqtt_response = True
            
            # Set data sending in progress in shared state
            with shared_state.lock:
                if self.road_section_id in shared_state.data_sending_status:
                    shared_state.data_sending_status[self.road_section_id]['sending'] = True
                    shared_state.data_sending_status[self.road_section_id]['completed'] = False
                    
            # Check if MQTT client is initialized
            if not hasattr(self, 'mqtt_client') or self.mqtt_client is None:
                print(f"[Camera {self.road_section_id}] Cannot publish - MQTT client not initialized")
                self.waiting_for_mqtt_response = False  # Reset flag if we can't publish
                # Mark data sending as failed
                with shared_state.lock:
                    if self.road_section_id in shared_state.data_sending_status:
                        shared_state.data_sending_status[self.road_section_id]['sending'] = False
                        shared_state.data_sending_status[self.road_section_id]['completed'] = False
                        # Unblock switching since we failed
                        shared_state.switching_blocked = False
                return
                
            # Check if we're in the process of switching cameras
            current_time = time.time()
            with shared_state.lock:
                switching_in_progress = (current_time - shared_state.last_switch_time < 1.0)
            
            # Skip publishing during switching, except for the active camera
            if switching_in_progress and not self.is_active:
                print(f"[Camera {self.road_section_id}] Skipping MQTT publish during camera switch")
                self.waiting_for_mqtt_response = False  # Reset flag if we're skipping
                # Mark data sending as failed
                with shared_state.lock:
                    if self.road_section_id in shared_state.data_sending_status:
                        shared_state.data_sending_status[self.road_section_id]['sending'] = False
                        shared_state.data_sending_status[self.road_section_id]['completed'] = False
                        # Unblock switching since we failed
                        shared_state.switching_blocked = False
                return
                
            # Check if we should publish based on conditions:
            # Only if we're the active camera or in transition period before switch
            should_publish = False
            is_transition = False
            
            with shared_state.lock:
                # Check if we're the active camera
                if shared_state.active_camera == self.road_section_id:
                    should_publish = True
                
                # Check if we're in the transition period (about to become active)
                elif (shared_state.next_camera_trigger_time is not None and 
                      current_time >= shared_state.next_camera_trigger_time and
                      self.road_section_id == (shared_state.active_camera % 4 + 1)):  # Next camera in sequence
                    should_publish = True
                    is_transition = True
            
            if not should_publish:
                print(f"[Camera {self.road_section_id}] Skipping publish - camera not active or transitioning")
                self.waiting_for_mqtt_response = False  # Reset flag if we're not publishing
                # Mark data sending as failed
                with shared_state.lock:
                    if self.road_section_id in shared_state.data_sending_status:
                        shared_state.data_sending_status[self.road_section_id]['sending'] = False
                        shared_state.data_sending_status[self.road_section_id]['completed'] = False
                        # Unblock switching since we failed
                        shared_state.switching_blocked = False
                return
                
            # Determine which camera's data to send based on the sequential pattern
            # Camera 1 sends Camera 2's data, Camera 2 sends Camera 3's data, etc.
            target_camera_id = self.road_section_id % 4 + 1  # 1→2, 2→3, 3→4, 4→1
            
            # Get the data from the target camera
            target_data = None
            with shared_state.lock:
                # First check if we have access to other cameras' data
                if hasattr(shared_state, 'camera_data') and shared_state.camera_data is not None:
                    # If we have data for the target camera, use it
                    if target_camera_id in shared_state.camera_data:
                        target_data = shared_state.camera_data[target_camera_id]
                        print(f"[Camera {self.road_section_id}] Using stored data for Camera {target_camera_id}")
            
            # If we couldn't get the target camera's data, use our own data
            # This ensures we always send something, even if the ideal data isn't available
            if target_data is None:
                # Format our own data according to the simplified structure
                target_data = {
                    "road_section_id": target_camera_id,  # Use target camera ID, not our own
                    "total_vehicles": self.total_vehicles,
                    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                # Process the vehicle_counts to only include non-zero counts
                filtered_counts = {}
                for vehicle_type, count in self.vehicle_counts.items():
                    if count > 0:
                        filtered_counts[vehicle_type] = count
                
                target_data["vehicle_counts"] = filtered_counts
                print(f"[Camera {self.road_section_id}] Using own data for Camera {target_camera_id} (fallback)")
            
            # Log that we're sending data
            if is_transition:
                print(f"[Camera {self.road_section_id}] Sending Camera {target_camera_id} data during transition")
            else:
                print(f"[Camera {self.road_section_id}] Sending Camera {target_camera_id} data")
            
            self.last_sent_vehicle_count = target_data.get("vehicle_counts", {}).copy()
            message = json.dumps(target_data)
            
            # Send to both specific and general topics for better compatibility
            try:
                # Specific topic with target road_section_id (not our own)
                mqtt_topic = f"traffic/vehicle_count/{target_camera_id}"
                result1 = self.mqtt_client.publish(mqtt_topic, message, qos=1, retain=True)
                
                # General topic that all subscribers can listen to
                general_topic = "traffic/vehicle_count"
                result2 = self.mqtt_client.publish(general_topic, message, qos=1, retain=True)
                
                # Also publish to topic with dash instead of underscore for compatibility
                alternate_topic = "traffic/vehicle-count"
                result3 = self.mqtt_client.publish(alternate_topic, message, qos=1, retain=True)
                
                # Special topic for each camera
                camera_topic = f"traffic/camera{target_camera_id}_updates"
                self.mqtt_client.publish(camera_topic, message, qos=1, retain=True)
                
                status1 = result1[0]
                status2 = result2[0]
                status3 = result3[0]
                
                if status1 != 0 or status2 != 0 or status3 != 0:
                    print(f"[Camera {self.road_section_id}] Failed to publish message, status: {status1}, {status2}, {status3}")
                    
                    # Retry immediately if publish failed
                    print(f"[Camera {self.road_section_id}] Retrying MQTT publish immediately")
                    time.sleep(0.2)  # Short delay before retry
                    self.mqtt_client.reconnect()  # Force reconnection
                    
                    # Try publishing again
                    self.mqtt_client.publish(mqtt_topic, message, qos=1, retain=True)
                    self.mqtt_client.publish(general_topic, message, qos=1, retain=True)
                    self.mqtt_client.publish(alternate_topic, message, qos=1, retain=True)
                    self.mqtt_client.publish(camera_topic, message, qos=1, retain=True)
                    print(f"[Camera {self.road_section_id}] MQTT publish retry completed")
                
                # Safely handle flush of MQTT messages
                try:
                    # Force immediate flush of MQTT messages
                    self.mqtt_client.loop_stop()
                    self.mqtt_client.loop_start()
                except Exception as loop_error:
                    print(f"[Camera {self.road_section_id}] Error during MQTT loop restart: {loop_error}")
                
                # Mark data sending as completed in shared state
                with shared_state.lock:
                    if self.road_section_id in shared_state.data_sending_status:
                        shared_state.data_sending_status[self.road_section_id]['sending'] = False
                        shared_state.data_sending_status[self.road_section_id]['completed'] = True
                        print(f"[Camera {self.road_section_id}] Data sending completed successfully")
                        # Allow switching now that data is sent
                        shared_state.switching_blocked = False
                
                # Keep waiting_for_mqtt_response flag true for a short time to show the alert
                # It will be reset in the display loop after the alert duration
            
            except Exception as publish_error:
                print(f"[Camera {self.road_section_id}] Error publishing to MQTT: {publish_error}")
                # Mark data sending as failed in shared state
                with shared_state.lock:
                    if self.road_section_id in shared_state.data_sending_status:
                        shared_state.data_sending_status[self.road_section_id]['sending'] = False
                        shared_state.data_sending_status[self.road_section_id]['completed'] = False
                        # Unblock switching since we failed
                        shared_state.switching_blocked = False
                
                # Try to reconnect but don't crash if it fails
                try:
                    self.mqtt_client.reconnect()
                    
                    # Retry immediately after reconnection
                    time.sleep(0.2)  # Short delay
                    # Try publishing again after reconnection
                    print(f"[Camera {self.road_section_id}] Retrying MQTT publish after reconnection")
                    self.mqtt_client.publish(mqtt_topic, message, qos=1, retain=True)
                    self.mqtt_client.publish(general_topic, message, qos=1, retain=True)
                    self.mqtt_client.publish(alternate_topic, message, qos=1, retain=True)
                    self.mqtt_client.publish(camera_topic, message, qos=1, retain=True)
                    
                    # If retry succeeds, mark as completed
                    with shared_state.lock:
                        if self.road_section_id in shared_state.data_sending_status:
                            shared_state.data_sending_status[self.road_section_id]['sending'] = False
                            shared_state.data_sending_status[self.road_section_id]['completed'] = True
                            print(f"[Camera {self.road_section_id}] Data sending completed after retry")
                            # Allow switching now that data is sent
                            shared_state.switching_blocked = False
                except:
                    pass
                
        except Exception as e:
            print(f"[Camera {self.road_section_id}] Error in publish_vehicle_count method: {e}")
            self.waiting_for_mqtt_response = False  # Reset flag if we encountered an exception
            
            # Mark data sending as failed in shared state
            with shared_state.lock:
                if self.road_section_id in shared_state.data_sending_status:
                    shared_state.data_sending_status[self.road_section_id]['sending'] = False
                    shared_state.data_sending_status[self.road_section_id]['completed'] = False
                    # Unblock switching since we failed
                    shared_state.switching_blocked = False
                    
            # Proceed without crashing

def main():
    # Create a list of stream URLs for all 4 external cameras
    stream_urls = [
        'http://127.0.0.1:5000/video_feed1',  # First external camera
        'http://127.0.0.1:5000/video_feed2',  # Second external camera
        'http://127.0.0.1:5000/video_feed3',  # Third external camera
        'http://127.0.0.1:5000/video_feed4',   # Fourth external camera
    ]
    
    # Explicitly map each camera to its correct road_section_id
    # Expanded to include all 4 cameras
    camera_configs = [
        {"url": 'http://127.0.0.1:5000/video_feed1', "road_section_id": 1},  # First external camera → road section 1
        {"url": 'http://127.0.0.1:5000/video_feed2', "road_section_id": 2},  # Second external camera → road section 2
        {"url": 'http://127.0.0.1:5000/video_feed3', "road_section_id": 3},  # Third external camera → road section 3
        {"url": 'http://127.0.0.1:5000/video_feed4', "road_section_id": 4}   # Fourth external camera → road section 4
    ]
    
    # Initialize shared state before creating processors
    start_time = time.time()
    with shared_state.lock:
        # Start with camera 2 active
        shared_state.active_camera = 2
        # Don't set a trigger time initially - will only send data in the last 5 seconds
        shared_state.next_camera_trigger_time = None
        shared_state.last_switch_time = start_time - 10  # Set switch time in the past to avoid cooldown at startup
        
        # Explicitly set camera states with 15-second duration for all 4 cameras
        shared_state.camera_states = {
            1: {'active': False, 'duration_threshold': 15, 'last_send_time': start_time},
            2: {'active': True, 'duration_threshold': 15, 'last_send_time': start_time},
            3: {'active': False, 'duration_threshold': 15, 'last_send_time': start_time},
            4: {'active': False, 'duration_threshold': 15, 'last_send_time': start_time}
        }
        
        print("\n" + "="*50)
        print("SYSTEM STARTING: Camera 2 (Section 2) is set as active")
        print("CAMERA SWITCHING: Fixed 15-second alternating between cameras")
        print("DATA SENDING: Only in the last 5 seconds of each camera's active period")
        print("EXPANDED SYSTEM: Now monitoring 4 cameras (road sections 1-4)")
        print("="*50 + "\n")
    
    # Create a processor for each camera with its specific road_section_id
    processors = []
    for config in camera_configs:
        processor = VideoStreamProcessor(
            stream_url=config["url"], 
            road_section_id=config["road_section_id"]
        )
        processors.append(processor)
        print(f"Created processor for camera at {config['url']} with road_section_id {config['road_section_id']}")
    
    # Apply a delay to make sure Camera 2 gets full initial duration
    time.sleep(0.5)
    print("[SYSTEM] Applied 0.5s delay after initialization to ensure Camera 2 gets full duration")
    
    # Ensure Camera 2 has its timer reset correctly
    with shared_state.lock:
        # Reset Camera 2's timer explicitly
        current_time = time.time()
        shared_state.camera_states[2]['last_send_time'] = current_time
        print("[SYSTEM] Explicitly Reset Camera 2's last_send_time at startup to ensure full 15s duration")
    
    # Start all processors in separate threads
    processor_threads = []
    for processor in processors:
        thread = threading.Thread(target=processor.run)
        thread.daemon = True  # Allow main thread to exit even if processors are still running
        thread.start()
        processor_threads.append(thread)
    
    try:
        # Wait for threads to complete or keyboard interrupt
        for thread in processor_threads:
            thread.join()
    except KeyboardInterrupt:
        print("\nShutting down all camera processors...")
        # Stop all processors
        for processor in processors:
            processor.is_running = False
        
        # Give threads time to clean up
        time.sleep(2)
        
        # Attempt to close OpenCV windows
        cv2.destroyAllWindows()
        print("All camera processors stopped.")

if __name__ == "__main__":
    main()
