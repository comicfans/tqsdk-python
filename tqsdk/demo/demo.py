import os
import sys
import structlog
import datetime
from trace_to_chrome import convert
from multiprocessing import Process,Queue

# local tqsdk import first
to_insert = os.path.abspath(os.path.join(
                os.path.dirname(__file__), '../../'))
sys.path.insert(0, to_insert)

import tqsdk
import tqsdk.algorithm

json_queue = None
json_log = 'json.log'

def write_json_log(queue):
    with open(json_log, 'w') as f:
        while True:
            obj = queue.get()
            f.write(obj)
            f.write('\n')
    
class MyJson(structlog.processors.JSONRenderer):
    def __call__(
        self, logger, name, event_dict
    ) -> str | bytes:
        """
        The return type of this depends on the return type of self._dumps.
        """
        global json_queue
        json_str  = self._dumps(event_dict, **self._dumps_kw)
        json_queue.put(json_str)

        raise structlog.DropEvent

structlog.configure(
        processors=[
            structlog.processors.CallsiteParameterAdder(
                {
                    structlog.processors.CallsiteParameter.FILENAME,
                    structlog.processors.CallsiteParameter.FUNC_NAME,
                    structlog.processors.CallsiteParameter.LINENO,
                }
            ),
            structlog.processors.dict_tracebacks,
            structlog.processors.TimeStamper(fmt="%Y-%m-%dT%H:%M:%S.%f", utc = False, key="timestamp"),
            MyJson(ensure_ascii = False)
        ])

log = structlog.get_logger()

def loop_wait(tqapi):
    after_30s = datetime.datetime.now() + datetime.timedelta(seconds = 30)
    while datetime.datetime.now() < after_30s:
        log.debug("wait_update", my_event = "wait")
        tqapi.wait_update()
        log.debug("wait_update", my_event = "complete")

def run_main():
    global json_queue 
    json_queue = Queue()
    json_process = Process(target = write_json_log, args = (json_queue,))
    json_process.start()

    
    log.debug("login", my_event = "wait")
    account = tqsdk.TqAccount("simnow", "username", "password")
    tqapi = tqsdk.TqApi(account, auth='username,password', debug=False)
    log.debug("login", my_event = "complete")

    tasks = {}
    for key in ['SHFE.ru2309', 'DCE.l2309']:
        time_table = tqsdk.algorithm.twap_table(tqapi, key, target_pos=10,duration=120, min_volume_each_step=1,
                                                        max_volume_each_step=5)
        tasks[key] = tqsdk.TargetPosScheduler(tqapi, key, time_table)

    loop_wait(tqapi)

    for key in tasks:
        tasks[key].cancel()

    loop_wait(tqapi)
    tqapi.close()

if __name__ == '__main__':
    # running tq api
    run_main()
    convert(json_log)
    
