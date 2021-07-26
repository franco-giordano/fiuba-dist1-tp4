from threading import Thread

class InternalMonitor:
    def __init__(self, internal_queue_name, rabbit_conn, ):
        self.monitor_thread = Thread()
        self.current_leader = -1 # unknown

    def start_sending_heartbeats(self):
        pass

    def start_monitoring_leader(self):
        pass

    def reset_leader_timer(self):
        pass

