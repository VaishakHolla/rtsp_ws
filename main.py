import asyncio
import websockets
import os
import json
import xml.etree.ElementTree as ET
import math

from gi.repository import Gst

Gst.init(None)
frame_sample_buffer = [] #Buffer to keep samples until entire frame is available  
#Rename variables 
object_tracking_buffer=[]  #Buffer to keep track of objects 

object_info_tracking_stack={}

dataNumber=1

async def handle_websocket(websocket, path):
    print(f"Client connected from {websocket.remote_address}",flush=True)
    # Send the initial response expected from v2x upon client connection
    initial_response = {
        "messageType": "Subscription",
        "subscription": {
            "returnValue": "OK",
            "type": "Data"
        }
    }
    await websocket.send(json.dumps(initial_response))
    
    # GStreamer pipeline creation and connection logic
    rtsp_url=os.getenv("RTSP_URL")
    pipeline_str = f"rtspsrc location={rtsp_url} ! application/x-rtp, media=application, payload=107, encoding-name=VND.ONVIF.METADATA! rtpjitterbuffer ! appsink name=appsink"
    pipeline = Gst.parse_launch(pipeline_str)
    pipeline.set_state(Gst.State.PLAYING)

    try:
        # Retrieve the appsink element from the pipeline
        appsink = pipeline.get_by_name("appsink")
        appsink.set_property("emit-signals", True)

        # Connect the new-sample signal to a callback function
        appsink.connect("new-sample", on_new_sample, {"websocket": websocket, "loop": asyncio.get_event_loop()})
            
        while True:
            await asyncio.sleep(0.1)
    except websockets.ConnectionClosedError:
        print("Client disconnected",flush=True)
    finally:
        # Cleanup GStreamer pipeline
        pipeline.set_state(Gst.State.NULL)
        
def on_new_sample(appsink, data):
    sample = appsink.emit("pull-sample")
    if sample:
        buffer = sample.get_buffer()
        payload_size = buffer.get_size()
        payload_data = buffer.extract_dup(0, payload_size)

        rtp_header = payload_data[:12]
        timestamp = int.from_bytes(rtp_header[4:8], byteorder='big')
        sequence_number = int.from_bytes(rtp_header[2:4], byteorder='big')
        paload_body = payload_data[12:]
        decoded_data = paload_body.decode('UTF-8')
        # Process payload_data as needed
        if _is_complete_metadata_frame(decoded_data):
            #Combine the metadata frame
            frame_sample_buffer.append(decoded_data)
            combined_metadata = "".join(frame_sample_buffer)
            frame_sample_buffer.clear()

            loop = data["loop"]
            websocketserver=data["websocket"]

            #Process metadata and send message to the client
            _process_metadata(combined_metadata,loop,websocketserver)
        else:
            frame_sample_buffer.append(decoded_data)
    return Gst.FlowReturn.OK

def _is_complete_metadata_frame(data):
    return data.endswith("</tt:MetadataStream>")
    
def _process_metadata( data,loop,websocketserver):
    
    #Tracking Notification Topics 
    entering_topic = "tns1:IVA/EnteringField/Entering_field"
    leaving_topic = "tns1:IVA/LeavingField/Leaving_field"
    infield_topc = "tns1:IVA/ObjectInField/Object_in_Field_1"

    data_by_object_id = {}

    root = ET.fromstring(data)
    for notification_message in root.findall('.//wsnt:NotificationMessage', namespaces={'wsnt': 'http://docs.oasis-open.org/wsn/b-2'}):
        topic = notification_message.find('./wsnt:Topic', namespaces={'wsnt': 'http://docs.oasis-open.org/wsn/b-2'}).text

        #Add the object entering field to the buffer
        if topic == infield_topc:
            entering_object_keys = notification_message.find(".//tt:Message/tt:Key", namespaces={"tt": "http://www.onvif.org/ver10/schema"})
            for key_element in entering_object_keys:
                value = key_element.get("Value")
                object_tracking_buffer.append(value)
                if value not in object_info_tracking_stack:
                    object_info_tracking_stack[value] = {
                        "initial_heading_x": None,
                        "initial_heading_y": None,
                    }

        #Remove the object leaving field from the buffer
        if topic == leaving_topic:
            exiting_object_keys = notification_message.find(".//tt:Message/tt:Key", namespaces={"tt": "http://www.onvif.org/ver10/schema"})
            if exiting_object_keys:
                for key_element in exiting_object_keys:         
                    value = key_element.get("Value")
                    object_tracking_buffer.remove(value)
                    object_info_tracking_stack.pop(value)


    if len(object_info_tracking_stack.keys())>0:
        # Find and extract information for each target ObjectId
        for target_object_id in object_info_tracking_stack.keys():
            object_data = {}

            # Find the object with the specified ObjectId
            for object_elem in root.findall(".//tt:Object", namespaces={"tt": "http://www.onvif.org/ver10/schema"}):
                if object_elem.get("ObjectId") == target_object_id:
                    # Extract the desired information for the selected ObjectId
                    object_data["utc_time"] = root.find(".//tt:Frame", namespaces={"tt": "http://www.onvif.org/ver10/schema"}).get('UtcTime')
                    
                    center_of_gravity_elem = object_elem.find(".//tt:CenterOfGravity", namespaces={"tt": "http://www.onvif.org/ver10/schema"})
                    if center_of_gravity_elem is not None:
                        object_data["x"] = center_of_gravity_elem.get("x")
                        object_data["y"] = center_of_gravity_elem.get("y")
                        if object_info_tracking_stack[target_object_id]["initial_heading_x"]==None:
                            object_info_tracking_stack[target_object_id]["initial_heading_x"]=center_of_gravity_elem.get("x")
                        if object_info_tracking_stack[target_object_id]["initial_heading_y"]==None:
                            object_info_tracking_stack[target_object_id]["initial_heading_y"]=center_of_gravity_elem.get("y")

                        object_data["Heading"]= math.degrees(math.atan2(float(center_of_gravity_elem.get("y"))-float(object_info_tracking_stack[target_object_id]["initial_heading_y"]), float(center_of_gravity_elem.get("x"))-float(object_info_tracking_stack[target_object_id]["initial_heading_y"])))
                        
                    class_candidate_elem = object_elem.find(".//tt:ClassCandidate", namespaces={"tt": "http://www.onvif.org/ver10/schema"})
                    if class_candidate_elem is not None:
                        object_data["ClassCandidate"] = {
                            "type": class_candidate_elem.find(".//tt:Type", namespaces={"tt": "http://www.onvif.org/ver10/schema"}).text,
                            "likelihood": class_candidate_elem.find(".//tt:Likelihood", namespaces={"tt": "http://www.onvif.org/ver10/schema"}).text,
                        }

                    latitude_elem = root.find(".//tt:Extension/NavigationalData/Latitude", namespaces={"tt": "http://www.onvif.org/ver10/schema"})
                    if latitude_elem is not None:
                        object_data["latitude"] = latitude_elem.text

                    longitude_elem = root.find(".//tt:Extension/NavigationalData/Longitude", namespaces={"tt": "http://www.onvif.org/ver10/schema"})
                    if longitude_elem is not None:
                        object_data["longitude"] = longitude_elem.text
                    
                        # object_data["elevation"] = geolocation_elem.get("elevation")

                    speed_elem = object_elem.find(".//tt:Speed", namespaces={"tt": "http://www.onvif.org/ver10/schema"})
                    if speed_elem is not None:
                        object_data["Speed"] = speed_elem.text

                    # No need to continue searching once the object is found
                    break

            # Store the data for this ObjectId
            data_by_object_id[target_object_id] = object_data

    # Send the extracted information to websocket
    for object_id, value in data_by_object_id.items():
        # print(f"Object ID: {object_id}",flush=True)
        # print("x:", data.get("x"),flush=True)
        # print("y:", data.get("y"),flush=True)
        # print("Class Candidate:", data.get("ClassCandidate"),flush=True)
        # print("Latitude:", data.get("latitude"),flush=True)
        # print("Longitude:", data.get("longitude"),flush=True)
        # # print("Elevation:", data.get("elevation"),flush=True)
        # print("Speed:", data.get("Speed"),flush=True)
        # print("Heading:", data.get("Heading"),flush=True)
        # print("UTC time:", data.get("utc_time"),flush=True)
        # print(flush=True)
        global dataNumber
        if value.get("utc_time"):
            metadata_dict = {
            "dataNumber": dataNumber,
            "messageType": "Data",
            "time": value.get("utc_time"),
            "track": [
                {
                "angle": value.get("Heading"),
                "class": "Pedestrian",
                "iD": object_id,
                "latitude": value.get("latitude"),
                "longitude": value.get("longitude"),
                # "speed": value.get("Speed"),
                "speed": "10",
                "x": value.get("x"),
                "y": value.get("y")
                }
            ],
            "type": "PedestrianPresenceTracking"
            }
            # increment the dataNumber (Not being used at V2X hub)
            dataNumber=dataNumber+1

            loop.create_task(send_message(websocketserver, metadata_dict))

async def send_message(websocket, payload_data):
    # Send a  message to the client
    await websocket.send(json.dumps(payload_data))

if __name__ == "__main__":
    start_server = websockets.serve(handle_websocket, "0.0.0.0", 80)

    asyncio.get_event_loop().run_until_complete(start_server)
    print("WebSocket server running at ws://0.0.0.0:80",flush=True)

    try:
        asyncio.get_event_loop().run_forever()
    except KeyboardInterrupt:
        print("WebSocket server stopped",flush=True)
