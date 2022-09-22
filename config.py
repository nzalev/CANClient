class Config():
    def __init__(self) -> None:
        self.api_key = ''
        self.url = ''
        self.interface = 'vcan0'
        self.vehicle_id = ''
        self.backoff_timer_min = 0.1
        self.backoff_timer_max = 10.0
        self.target_req_time = 1.5
