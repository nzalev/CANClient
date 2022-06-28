import can
from config import Config
from queue import Queue
from Sender import Sender
from time import time

config = Config()
q = Queue()
sender: Sender = Sender(q, config)

bus = can.Bus(interface='socketcan',
              channel=config.interface,
              receive_own_messages=True)

try:
    sender.start()

    for msg in bus:
        frame = {
            'vehicle_id': config.vehicle_id,
            'arbitration_id': msg.arbitration_id,
            'data_len': len(msg.data),
            'data_string': ' '.join('{:02X}'.format(x) for x in msg.data),
            'time_recorded': time()
        }
        sender.add(frame)

except KeyboardInterrupt:
    print('exiting')
    sender.kill()