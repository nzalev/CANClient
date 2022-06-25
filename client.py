import requests
import can
import sys
from config import Config

config = Config()


interface = config.interface
vehicle_id = config.vehicle_id
frame_limit_count = 1000
url = config.url + '/frames/bulk'
headers = {
    'Content-type': 'application/json',
    'apikey': config.api_key
}

counter = 0
frames = []

bus = can.Bus(interface='socketcan',
              channel=interface,
              receive_own_messages=True)

for msg in bus:

    data = {
        'vehicle_id': vehicle_id,
        'arbitration_id': msg.arbitration_id,
        'data_len': len(msg.data),
        'data_string': ' '.join('{:02X}'.format(x) for x in msg.data)
    }

    counter += 1
    frames.append(data)

    if (counter >= frame_limit_count):

        try:
            requests.post(url=url, headers=headers, json=frames)
        except Exception as e:
            print('send failed', e)

        # After sending bulk request, clear the counter and frame buffer
        counter = 0
        frames = []


    x = "  {0}\t  {1:0>3X}\t[{2}]\t{3}".format(
        interface,
        data['arbitration_id'],
        data['data_len'],
        data['data_string'])
    print(x)


# create sender class (thread it)
# use queue to store frames
# use threading.Queue