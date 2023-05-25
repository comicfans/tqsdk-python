import json
import datetime
import argparse

def load_file_as_line_by_line_json(filename):
    with open(filename, 'r') as f:
        for line in f:
            try:
                obj = json.loads(line)
                yield obj
            except json.JSONDecodeError as e:
                continue



def try_parse_time(dt_str):
    try:
        res = datetime.datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%f")
        return res
    except ValueError:
        res =  datetime.datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S")
        return res
   
def convert_to_chrome_trace(json_list):
    trace_events = []
    prev_timestamp = None

    for event_counter,json_obj in enumerate(json_list):
        if prev_timestamp is None:
            prev_timestamp = try_parse_time(json_obj['timestamp'])

        this_timeoffset = try_parse_time(json_obj['timestamp']) - prev_timestamp
        ms = this_timeoffset.total_seconds() * 1000000

        args_key = ["lineno","depends","filename","event_rev","wait_update_counter"]

        args = dict([(k,json_obj[k]) for k in args_key if k in json_obj])

        if json_obj['my_event'] == 'await':
            start_event = {
                'name': json_obj['event'],
                'cat': 'function',
                'ph': 'B',
                'ts': ms,
                'pid': json_obj['symbol'],
                'tid': json_obj['clazz']+"."+json_obj['func_name']+":"+str(json_obj['current_task']),
                'id': event_counter,
                'args': args
            }
            trace_events.append(start_event)
        elif json_obj['my_event'] == 'resume':
            end_event = {
                'cat': 'function',
                'ph': 'E',
                'ts': ms,
                'pid': json_obj['symbol'],
                'tid': json_obj['clazz']+"."+json_obj['func_name']+":"+str(json_obj['current_task']),
                'id': event_counter,
                'args': args
            }
            trace_events.append(end_event)
        elif json_obj['my_event'] == 'wait':
            start_event = {
                'name': json_obj['event'],
                'cat': 'function',
                'ph': 'B',
                'ts': ms,
                'pid': 'mainloop',
                'tid': 'mainloop',
                'id': event_counter,
                'args': args
            }
            trace_events.append(start_event)
        elif json_obj['my_event'] == 'complete':
            end_event = {
                'cat': 'function',
                'ph': 'E',
                'ts': ms,
                'pid': 'mainloop',
                'tid': 'mainloop',
                'id': event_counter,
                'args': args
            }
            trace_events.append(end_event)
    chrome_trace = {
        'traceEvents': trace_events,
        'displayTimeUnit': 'ns'
    }

    return chrome_trace

def filter_my_event_log_only(json_stream):
    for json in json_stream:
        if 'my_event' not in json or json['my_event'] not in ['await','resume','wait','complete']:
            continue
        yield json

def convert(trace):
    chrome_json = convert_to_chrome_trace(filter_my_event_log_only(load_file_as_line_by_line_json(trace)))

    with open(args.trace + '.chrome.json','w') as f:
        f.write(json.dumps(chrome_json, indent=4, sort_keys = True, default = str))


if __name__ == '__main__': 

    # parse arg with argument --trace 
    parser = argparse.ArgumentParser(description='Convert async log to chrome trace format, output write as trace.chrome.json')
    parser.add_argument('--trace', dest='trace', required=True, help='async log file')
    # now parse command line
    args = parser.parse_args()
    convert(args.trace)



