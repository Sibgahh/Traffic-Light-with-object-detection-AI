#include <ArduinoJson.h>

// Pin definitions for 4 traffic lights (green, yellow, red for each)
const int row1Pins[] = {16, 17, 18}; // Green, Yellow, Red for Row 1
const int row2Pins[] = {19, 21, 22}; // Green, Yellow, Red for Row 2
const int row3Pins[] = {23, 25, 26}; // Green, Yellow, Red for Row 3
const int row4Pins[] = {27, 32, 33}; // Green, Yellow, Red for Row 4

// Current light durations for each row (in seconds)
struct LightDurations {
  int green;
  int yellow;
  int red;
};

LightDurations row1 = {30, 5, 65};
LightDurations row2 = {30, 5, 65};
LightDurations row3 = {30, 5, 65};
LightDurations row4 = {30, 5, 65};

// Current state of each row's traffic light
enum LightState {GREEN, YELLOW, RED};
LightState row1State = RED;
LightState row2State = RED;
LightState row3State = RED;
LightState row4State = GREEN;

// Timers for each row
unsigned long row1Timer = 0;
unsigned long row2Timer = 0;
unsigned long row3Timer = 0;
unsigned long row4Timer = 0;
unsigned long currentMillis = 0;

// Buffer for incoming JSON data
String inputBuffer = "";
const int JSON_BUFFER_SIZE = 1024;

void setup() {
  // Initialize serial communication
  Serial.begin(115200);
  
  // Set all traffic light pins as OUTPUT
  for (int i = 0; i < 3; i++) {
    pinMode(row1Pins[i], OUTPUT);
    pinMode(row2Pins[i], OUTPUT);
    pinMode(row3Pins[i], OUTPUT);
    pinMode(row4Pins[i], OUTPUT);
  }
  
  // Initial state: Row 4 is GREEN, all others are RED
  updateLights();
  
  // Initialize timers
  currentMillis = millis();
  row1Timer = currentMillis;
  row2Timer = currentMillis;
  row3Timer = currentMillis;
  row4Timer = currentMillis;
  
  Serial.println("ESP32 Traffic Control System Ready");
}

void loop() {
  // Check for incoming data from Python
  readSerialData();
  
  // Update traffic light states
  currentMillis = millis();
  updateTrafficLights();
  
  // Update physical lights
  updateLights();
  
  // Small delay to prevent CPU hogging
  delay(10);
}

void readSerialData() {
  while (Serial.available() > 0) {
    char c = Serial.read();
    
    // End of message
    if (c == '\n') {
      if (inputBuffer.length() > 0) {
        parseJsonData(inputBuffer);
        inputBuffer = "";
      }
    } else {
      inputBuffer += c;
    }
  }
}

void parseJsonData(String jsonString) {
  // Allocate the JSON document
  StaticJsonDocument<JSON_BUFFER_SIZE> doc;
  
  // Parse JSON
  DeserializationError error = deserializeJson(doc, jsonString);
  
  // Test if parsing succeeds
  if (error) {
    Serial.print("deserializeJson() failed: ");
    Serial.println(error.c_str());
    return;
  }
  
  // Extract durations from JSON
  if (doc.containsKey("row1")) {
    row1.green = doc["row1"]["green"];
    row1.yellow = doc["row1"]["yellow"];
    row1.red = doc["row1"]["red"];
  }
  
  if (doc.containsKey("row2")) {
    row2.green = doc["row2"]["green"];
    row2.yellow = doc["row2"]["yellow"];
    row2.red = doc["row2"]["red"];
  }
  
  if (doc.containsKey("row3")) {
    row3.green = doc["row3"]["green"];
    row3.yellow = doc["row3"]["yellow"];
    row3.red = doc["row3"]["red"];
  }
  
  if (doc.containsKey("row4")) {
    row4.green = doc["row4"]["green"];
    row4.yellow = doc["row4"]["yellow"];
    row4.red = doc["row4"]["red"];
  }
  
  // Send acknowledgment
  Serial.println("Durations updated");
}

void updateTrafficLights() {
  // Row 1 state machine
  updateRowState(row1State, row1Timer, row1.green, row1.yellow, row1.red);
  
  // Row 2 state machine
  updateRowState(row2State, row2Timer, row2.green, row2.yellow, row2.red);
  
  // Row 3 state machine
  updateRowState(row3State, row3Timer, row3.green, row3.yellow, row3.red);
  
  // Row 4 state machine
  updateRowState(row4State, row4Timer, row4.green, row4.yellow, row4.red);
}

void updateRowState(LightState &state, unsigned long &timer, int greenTime, int yellowTime, int redTime) {
  unsigned long elapsedTime = currentMillis - timer;
  
  switch (state) {
    case GREEN:
      if (elapsedTime >= greenTime * 1000) {
        state = YELLOW;
        timer = currentMillis;
      }
      break;
      
    case YELLOW:
      if (elapsedTime >= yellowTime * 1000) {
        state = RED;
        timer = currentMillis;
      }
      break;
      
    case RED:
      if (elapsedTime >= redTime * 1000) {
        state = GREEN;
        timer = currentMillis;
      }
      break;
  }
}

void updateLights() {
  // Update Row 1 lights
  digitalWrite(row1Pins[0], row1State == GREEN ? HIGH : LOW);
  digitalWrite(row1Pins[1], row1State == YELLOW ? HIGH : LOW);
  digitalWrite(row1Pins[2], row1State == RED ? HIGH : LOW);
  
  // Update Row 2 lights
  digitalWrite(row2Pins[0], row2State == GREEN ? HIGH : LOW);
  digitalWrite(row2Pins[1], row2State == YELLOW ? HIGH : LOW);
  digitalWrite(row2Pins[2], row2State == RED ? HIGH : LOW);
  
  // Update Row 3 lights
  digitalWrite(row3Pins[0], row3State == GREEN ? HIGH : LOW);
  digitalWrite(row3Pins[1], row3State == YELLOW ? HIGH : LOW);
  digitalWrite(row3Pins[2], row3State == RED ? HIGH : LOW);
  
  // Update Row 4 lights
  digitalWrite(row4Pins[0], row4State == GREEN ? HIGH : LOW);
  digitalWrite(row4Pins[1], row4State == YELLOW ? HIGH : LOW);
  digitalWrite(row4Pins[2], row4State == RED ? HIGH : LOW);
} 