from queue import Queue
from config import Config
from Sender import Sender
import base64
import gzip
import json

class CompressedSender(Sender):
    def __init__(self, queue: Queue, config: Config) -> None:
        super().__init__(queue, config)
        self.url = config.url + '/compressedframes'
        self.headers['Content-encoding'] = 'gzip'
        self.https_session.headers.update(self.headers)
        self.config = config


    # Override
    def _generate_message(self) -> any:
        # Make the dict json serializable via b64 encoding
        buf = []
        for frame in self.frame_buffer:
            buf.append(base64.b64encode(frame).decode('utf8') )

        msg = {
            'vehicle_id': self.config.vehicle_id,
            'frames': buf
        }

        return gzip.compress(json.dumps(msg).encode())