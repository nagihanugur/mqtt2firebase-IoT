import paho.mqtt.client as mqtt
import firebase_admin
from firebase_admin import credentials, db
from firebase_admin import firestore
import json
import datetime
from time import sleep
import time

Fb_Coll = "consumption2"

# MQTT broker configuration

mqtt_port = 1883
mqtt_publish_topic = "/topic"


# Firebase configuration
firebase_credentials = "login.json"

# initialize firebase-admin    
   
cred = credentials.Certificate(firebase_credentials)
firebase_admin.initialize_app(cred)

Server = "localhost"
db = firestore.client()


# MQTT client callback functions

def publish_message(client):
    

    # MQTT konusuna mesajı yayınla
    
    # Publish a message to MQTT topic
    

        with open('output.json') as f:
           data_mess = json.load(f)

        now = datetime.datetime.now()
        data_mess['time'] = now.strftime("%d/%m/%Y %H:%M:%S")
        client.publish(mqtt_publish_topic, json.dumps(data_mess))

        

        

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker")
    client.subscribe(mqtt_publish_topic)

def on_message(client, userdata, message):
    try:
        
        payload_json = json.loads(message.payload.decode())
        
        print("Received data:", payload_json)

        # Send data to Firebase Realtime Database
        doc_ref = db.collection('consumption2').document('consumption')
        doc_ref.set(payload_json)
       
        print("Data sent to Firebase")

    except Exception as e:
        print("Error processing MQTT message:", str(e))



# Create MQTT client
client = mqtt.Client()
client.on_connect = lambda client, userdata, flags, rc: print("Bağlantı sağlandı: " + str(rc))
client.on_message = on_message

# Connect to MQTT broker
client.connect(Server, mqtt_port, 60)

client.subscribe(mqtt_publish_topic)

#publish_message(client)

publish_message(client)

# Start the MQTT loop

client.loop_forever()
