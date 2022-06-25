import time
import requests
import threading
from queue import Empty, Queue
from config import Config


class Sender(threading.Thread):

    # Dependency inject the queue for less reliance on specific type
    def __init__(self, queue: Queue, config: Config) -> None:
        super(Sender, self).__init__()

        self.queue = queue
        self.frame_buffer = []
        self.frame_batch_size = 1000
        self.backoff_timer = 0.5
        self.bmin = config.backoff_timer_min
        self.bmax = config.backoff_timer_max
        self.target_req_time = config.target_req_time

        self.url = config.url + '/frames/bulk'
        self.headers = {
            'Content-type': 'application/json',
            'apikey': config.api_key
        }

        self._killed = False

        self.https_session = requests.Session()
        self.https_session.headers.update(self.headers)


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
            if (size_now < self.frame_batch_size):
                n = size_now
            else:
                n = self.frame_batch_size

            while (counter < n):
                self.frame_buffer.append(self.queue.get(block=False))
                counter += 1

        except Empty:
            pass

        finally:
            return counter


    def _send(self) -> float:
        begin = time.time()

        try:
            res = self.https_session.post(url=self.url,
                                          json=self.frame_buffer)
            # check res.status for 413
        except Exception as e:
            print(e)

        end = time.time()
        
        return (end - begin)


    def _update_backoff_timer(self) -> None:
        limit = self.frame_batch_size
        queued = self.queue.qsize()

        # if we are queueing frames too slowly, add delay
        # if we have more frames than the limit, reduce delay
        if (queued < limit):
            self.backoff_timer = min(self.backoff_timer + 0.5, self.bmax)
        elif (queued > limit):
            self.backoff_timer = max(self.backoff_timer / 2, self.bmin)


    # Keep HTTP requests reasonably fast by adjusting batch size
    def _update_batch_size(self, elapsed) -> None:
        
        # If req time is in goldilocks region, don't update
        if (elapsed > self.target_req_time - 0.1 and
            elapsed < self.target_req_time + 0.1):
            return

        if (elapsed < self.target_req_time):
            self.frame_batch_size += 500
        else:
            self.frame_batch_size = max(self.frame_batch_size / 2, 1)


    def run(self) -> None:
        while not self._killed:
            self._update_backoff_timer()
            print('backoff: ', self.backoff_timer)


            n = self._get_n(self.frame_batch_size)
            remaining = self.queue.qsize()

            if (n > 0):
                print('frame lim: ', self.frame_batch_size)

                elapsed = self._send()
                self._update_batch_size(elapsed)

                self.frame_buffer = []

                print('sending: ', n)
                print('remaining: ', remaining)
                print('req speed: ', elapsed)
                print('')

            time.sleep(self.backoff_timer)
