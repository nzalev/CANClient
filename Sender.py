from time import sleep
import requests
import threading
from queue import Empty, Queue
from config import Config


class Sender(threading.Thread):

    # Dependency inject the queue for less reliance on specific type
    def __init__(self, queue: Queue, config: Config) -> None:
        super(Sender, self).__init__()

        self.queue = queue
        self.frame_count_limit = 1000
        self.frame_buffer = []

        self.url = config.url + '/frames/bulk'
        self.headers = {
            'Content-type': 'application/json',
            'apikey': config.api_key
        }

        self._killed = False


    def add(self, frame) -> None:
        # Add Full exception check
        self.queue.put(frame)

    def kill(self) -> None:
        self._killed = True


    # Fill the frame buffer with up to n frames from the queue
    # Returns the number of frames read (can be < n)
    def _get_n(self, n) -> int:
        counter = 0

        try:
            size_now = self.queue.qsize()
            if (size_now < self.frame_count_limit):
                n = size_now
            else:
                n = self.frame_count_limit

            while (counter < n):
                self.frame_buffer.append(self.queue.get(block=False))
                counter += 1

        except Empty:
            pass

        finally:
            return counter


    def _send(self) -> None:
        # begin timer

        try:
            res = requests.post(url=self.url,
                                headers=self.headers,
                                json=self.frame_buffer)
            # check res.status for 413
        except Exception as e:
            print(e)

        # end timer
        # return elapsed time


    def run(self) -> None:
        while not self._killed:
            n = self._get_n(self.frame_count_limit)

            print('sending: ', n)
            print('remaining: ', self.queue.qsize())

            if (n > 0):
                self._send()
                self.frame_buffer = []

            sleep(2)
