import sys
import cPickle
import boto3
import time
import multiprocessing
import requests
import uuid
import cv2
import numpy as np
import json
import gc
import logging
 
kinesis_client = boto3.client("kinesis")
 
stream_response = kinesis_client.describe_stream(StreamName="FrameStreamV2")
 
 
#sends frame package to Kinesis
def put_frame(frame_stream, frame_package, shard):
    start = time.time()
    data_package = cPickle.dumps(frame_package, 2)
    pickle_end = time.time()
   
    response = kinesis_client.put_record(
        StreamName= frame_stream,
        Data=data_package,
        ExplicitHashKey = stream_response['StreamDescription']['Shards'][shard]['HashKeyRange']["StartingHashKey"],
        PartitionKey = "test"
 
    )
 
    upload_end = time.time()
    if start - upload_end > .3:
        print ("total time " + str(start-upload_end) + ", pickle time " + str(start-pickle_end) + ", kinesis upload" + str(pickle_end-upload_end))
 
 
 
#packages frame with metadata call put_frame to send package to Kinesis
def send_jpg(frame, frame_stream, cameraID, change, name, shard, enable_kinesis=True, write_file=False, box = None):
   
   
   
    customerID = "WS" + str(name)
   
    change = int(change)
   
 
   
   
   
    try:
        img_bytes = frame
 
        now_ts_utc = time.time()
       
        frame_package = {
            'ApproximateCaptureTime' : now_ts_utc,
            'ImageBytes' : img_bytes,
            'CameraID' : cameraID,
            'Motion' : change,
            'CustomerID' : customerID,
            'Timezone' : 'US/Central',
            'FrameID' : str(uuid.uuid4()),
            'Name' : name,
            'Box' : box
           
        }
 
 
       
        if write_file:
            print "Writing file img_{}.jpg".format(str(uuid.uuid4()))
            target = open("test_frames/{}.jpg".format(name), 'w+')
            target.write(img_bytes)
            target.close()
 
 
        if enable_kinesis:
            t1 = time.time()
            put_frame(frame_stream, frame_package, shard)
            del frame_package
            del img_bytes
            del frame
 
            t2 = time.time()
            if t2 - t1 > .3:
                print("kinesis time " + str(t2-t1))
 
 
 
       
    except Exception as e:
        print e
        print cameraID
        print(sys.getsizeof(now_ts_utc) + sys.getsizeof(img_bytes) + sys.getsizeof(cameraID) + sys.getsizeof("parker"))
 
       
#connects to specific customer camera and pulls frames    
def stream_request_worker(url, id, camera_id, username, password, name, shard):
 
 
 
 
 
 
    frame_count = 0
   
 
    cap = cv2.VideoCapture("http://"+ str(username) + ":" + str(password) + "@" + str(url) + ":7001/media/" + str(id) + '.mpegts?resolution=540p')
 
   
   
 
 
    prev = 0
   
    time_since_motion = time.time()
   
    start = time.time()
   
    motionless = False
    while True:
        ret, frame = cap.read()
       
 
        if ret is False:
            print("link broken")
            break
        if frame_count%100 == 0:
            gc.collect()
   
        if time.time() - start >.25:
            start = time.time()
 
            frame_jpg_bytes = cv2.imencode(".jpg", frame)[1].tostring()
 
            img = cv2.imdecode(np.fromstring(frame_jpg_bytes, dtype=np.uint8), 1)
            height, width = img.shape[:2]
   
            x_min_px = int(width*0.15)
            x_max_px = int(width*0.85)
            crop_img = img[0:height, x_min_px:x_max_px]
            img_str = cv2.imencode('.jpg', crop_img)[1].tostring()
            new = cv2.cvtColor(crop_img, cv2.COLOR_RGB2GRAY)
            diff = cv2.absdiff(new, prev)
            change = diff.sum()
            now = time.time()
 
           
            prev = new
            if change < 4000000:
                if (now-time_since_motion)>30:
                    if motionless == False:
                        motionless = True
                        print("no motion on " + str(name))
                else:
                    send_jpg(img_str, "FrameStreamV2", camera_id, change, name, shard, True, False)
 
            else:
                if motionless == True:
                    motionless = False
                    print("motion on " + str(name))
                time_since_motion = time.time()
                send_jpg(img_str, "FrameStreamV2", camera_id, change, name, shard, True, False)
 
       
        frame_count += 1
           
           
#
def stream_request(url, id ,camera_id, username, password, name, shard):
    while True:
        try:
            stream_request_worker(url, id ,camera_id, username, password,  modulo, name, shard)
        except requests.exceptions.ChunkedEncodingError:
            continue
        else:
            print("error with " + str(name))
       
           
 
 
 
#spawns seperate processes for each camera
def main():
 
   
    response = requests.get("http://38.102.61.250:7001/ec2/getCamerasEx?format=json", auth = ('username', 'password') , stream = False)
   
    response_str = str(response.content)
 
    json_data = json.loads(response_str)
 
    camera_dict = {}
    camera_uid_dict = {}
   
    for x in json_data:
        name = x["name"]
        camera_id = name.split('. ', 1)[0]
        camera_name = name.split('. ', 1)[1]
        camera_name = camera_name.replace('&', 'and')
        camera_dict[int(camera_id)] = camera_name
        id = x["id"]
        id = id.replace("{", "")
        id = id.replace("}", "")
        camera_uid_dict[int(camera_id)] = id
 
    results = []
    camera_list = [2, 3, 4, 5, 6, 8, 9, 15, 16, 17]
    shard  = 18
   
    for camera_id in camera_list:
 
        try:
            name = camera_dict[camera_id]
            id = camera_uid_dict[camera_id]
            shard = shard+ 1
            result = multiprocessing.Process(target=stream_request, args=("38.102.61.250", id, camera_id, 'username', 'password',  name, shard))
            results.append(result)
 
        except:
            print "Error: unable to start process " + str(camera_id)
    [result.start() for result in results]
    [result.join() for result in results]
if __name__ == '__main__':
    main()
