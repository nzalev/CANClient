import can
import struct
from config import Config
from queue import Queue
from time import time
from CompressedSender import CompressedSender

config = Config()
q = Queue()
sender = CompressedSender(q, config)

bus = can.Bus(interface='socketcan',
              channel=config.interface,
              receive_own_messages=True)

try:
    sender.start()

    for msg in bus:

        data_int = int.from_bytes(msg.data, 'little')

        # u_short  u_char  u_longlong  double
        # aid      len     data        time
        frame = struct.pack("<HBQd", msg.arbitration_id, len(msg.data), data_int, time())

        '''
        frame = {
            'vehicle_id': config.vehicle_id,
            'arbitration_id': msg.arbitration_id,
            'data_len': len(msg.data),
            'data_string': ' '.join('{:02X}'.format(x) for x in msg.data),
            'time_recorded': time()
        }
        '''
        sender.add(frame)

except KeyboardInterrupt:
    print('exiting')
    sender.kill()