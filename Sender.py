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
        self.request_timeout = self.target_req_time + 10

        self.url = config.url + '/frames/bulk'
        self.headers = {
            'Content-type': 'application/json',
            'apikey': config.api_key
        }

        self._killed = False

        self.https_session = requests.Session()
        self.https_session.headers.update(self.headers)

        self._retry = False


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
        additional_time = 0

        try:
            res = self.https_session.post(url=self.url,
                                          json=self.frame_buffer,
                                          timeout=self.request_timeout)
            
            if (not res):
                self._retry = True
                additional_time += self.request_timeout
            else:
                additional_time += res.elapsed.total_seconds()

            # check for specific status codes
            if (res.status_code == 200):
                self._retry = False
            else:
                print('Req failed. Status: ', res.status_code)

                # If Req too large, send the buffer in two halfs
                # ---
                # At present, frames will be lost if a retry occurs
                # on the first send
                # Can be fixed by combining the frame buffers again
                # and quitting
                if (res.status_code == 413):
                    mid = int(len(self.frame_buffer) / 2)
                    first_half = self.frame_buffer[ : mid]
                    second_half = self.frame_buffer[mid : ]

                    self.frame_buffer = first_half
                    additional_time += self._send()

                    self.frame_buffer = second_half
                    additional_time += self._send()
                else:
                    self._retry = True

        except Exception as e:
            print(e)
            self._retry = True
            additional_time += self.request_timeout

        finally:
            return additional_time


    def _update_backoff_timer(self) -> None:
        # If we are in an error state, do not spin rapidly
        if (self._retry):
            self.backoff_timer = self.bmax / 2
            return

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
            if (self.frame_batch_size < 500):
                self.frame_batch_size += 100
            else:
                self.frame_batch_size += 500
        else:
            self.frame_batch_size = max(self.frame_batch_size / 2, 1)


    # TODO: Clean up the main loop for retry condition
    def run(self) -> None:
        while not self._killed:
            self._update_backoff_timer()
            print('backoff: ', self.backoff_timer)

            if (not self._retry):
                n = self._get_n(self.frame_batch_size)

            remaining = self.queue.qsize()

            if (n > 0):
                print('batch size: ', self.frame_batch_size)

                elapsed = self._send()
                self._update_batch_size(elapsed)

                if (not self._retry):
                    self.frame_buffer = []

                print('sending: ', n)
                print('remaining: ', remaining)
                print('req speed: ', elapsed)
                print('')

            time.sleep(self.backoff_timer)
