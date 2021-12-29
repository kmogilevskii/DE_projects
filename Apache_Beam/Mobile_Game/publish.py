import os
import time
from google.cloud import pubsub_v1

project_id = '##################################' # project id in the GCP

topic_name = '##################################' # topic name that receives the stream of data

path_service_account = '.#######################' # path to json file with creddentials

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_service_account

publisher = pubsub_v1.PublisherClient()

input_file = './mobile_game.txt'

with open(input_file, 'rb') as ifp:
    header = ifp.readline()
    for line in ifp:
        event_data = line  # entire line of input txt is the message
        print('Publishing {0} to {1}'.format(event_data, topic_name))
        publisher.publish(topic_name, event_data)
        time.sleep(1)
