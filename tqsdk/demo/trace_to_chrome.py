import json
import datetime
from collections import defaultdict
import argparse
import xxhash

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
    prev_timestamp = None

    #key is process, value is
    # {tid: stack[]}
    async_stack_map = defaultdict(lambda:defaultdict(lambda:[]))

    sync_stack = []
    # pid as key,
    reuse_tid_map = defaultdict(lambda:defaultdict(lambda:[]))
    
    canbe_reuse_tid = defaultdict(lambda:[])


    yield {
                    'ph': 'M',
                    'name': 'process_name',
                    'pid': xxhash.xxh32("mainloop").intdigest(),
                    'args': {
                        "name": "mainloop"
                    }

    }

    yield {
                    'ph': "M",
                    'name': 'thread_name',
                    'tid': xxhash.xxh32("mainloop").intdigest(),
                    'pid': xxhash.xxh32("mainloop").intdigest(),
                    'args': {
                        'name':"mainloop" 
                    }
    }


    def get_remap_tid(pid, tid):
        if tid in reuse_tid_map[pid]:
            # already have mapping, use it
            # mapping is 1:1, so if this orig tid
            # is unique (it use id(task))it must
            # be unqiue (at same time).
            # (id(task) maybe appear at different time,
            # but will never exist at same time)
            return reuse_tid_map[pid][tid]

        # checking if we have similar tid which can be reused
        # it has to be a finished task 
        # and since its finished, it must have a 
        # empty async_stack
        # (because finished async task will pop out all entries)
        entry_prefix = tid.split(":")[0]
        canbe_reuse= [x for x in canbe_reuse_tid[pid] if x.startswith(entry_prefix)]
        if len(canbe_reuse) == 0:
            # no empty stack can be reused
            reuse_tid_map[pid][tid] = tid
            return tid
        # can be reuse
        remapped = canbe_reuse[0]
        assert len(async_stack_map[pid][remapped]) == 0
        canbe_reuse_tid[pid].remove(remapped)
        reuse_tid_map[pid][tid] = remapped
        return remapped

    for event_counter,json_obj in enumerate(json_list):
        if prev_timestamp is None:
            prev_timestamp = try_parse_time(json_obj['timestamp'])

        this_timeoffset = try_parse_time(json_obj['timestamp']) - prev_timestamp
        ms = this_timeoffset.total_seconds() * 1000000

        if json_obj['func_name'].startswith("_wrap"):
            # remove _wrap prefix from string
            json_obj['func_name'] = json_obj['func_name'][len('_wrap'):]


        async_pid = None
        async_tid = None
        async_remap_tid = None
        if json_obj['my_event'] in ['await','resume']:
            async_pid = json_obj['symbol'] if 'symbol' in json_obj else json_obj['clazz']
            async_tid = (json_obj['clazz']+"."+json_obj['func_name'] if 'symbol' in json_obj else json_obj['func_name'])+":"+str(json_obj['current_task'])
            async_remap_tid = get_remap_tid(async_pid, async_tid)


        if json_obj['my_event'] == 'await':
            if 'clazz' not in json_obj:
                json_obj['clazz'] = 'TqApi'

            start_event = {
                'name': json_obj['event'],
                'cat': 'function',
                'ph': 'B',
                'ts': ms,
                'pid': xxhash.xxh32(async_pid).intdigest(),
                'tid': xxhash.xxh32(async_pid+async_remap_tid).intdigest(),
                'id': event_counter,
                'args': json_obj
            }

            if async_pid not in async_stack_map:
                # this is first time process appear
                yield {
                    'ph': 'M',
                    'name': 'process_name',
                    'pid': xxhash.xxh32(async_pid).intdigest(),
                    'args': {
                        "name": async_pid
                    }

                }

            if async_remap_tid not in async_stack_map[async_pid]:
                yield {
                    'ph': "M",
                    'name': 'thread_name',
                    'tid': xxhash.xxh32(async_pid+async_remap_tid).intdigest(),
                    'pid': xxhash.xxh32(async_pid).intdigest(),
                    'args': {
                        'name': async_remap_tid
                    }
                }


            async_stack_map[async_pid][async_tid].append(start_event)
            yield start_event
        elif json_obj['my_event'] == 'resume':
            if 'clazz' not in json_obj:
                json_obj['clazz'] = 'TqApi'

            end_event = {
                'cat': 'function',
                'ph': 'E',
                'ts': ms,
                'pid': xxhash.xxh32(async_pid).intdigest(),
                'tid': xxhash.xxh32(async_pid+async_remap_tid).intdigest(),
                'id': event_counter,
                'args': json_obj
            }
            stack = async_stack_map[async_pid][async_tid]
            poped = stack.pop()
            assert poped['name'] == json_obj['event']
            if len(stack) == 0:
                canbe_reuse_tid[async_pid].append(async_remap_tid)
                del reuse_tid_map[async_pid][async_tid]
            yield end_event
        elif json_obj['my_event'] == 'wait':
            start_event = {
                'name': json_obj['event'],
                'cat': 'function',
                'ph': 'B',
                'ts': ms,
                'pid': xxhash.xxh32('mainloop').intdigest(),
                'tid': xxhash.xxh32('mainloop').intdigest(),
                'id': event_counter,
                'args': json_obj
            }
            sync_stack.append(start_event)
            yield start_event
        elif json_obj['my_event'] == 'complete':
            end_event = {
                'cat': 'function',
                'ph': 'E',
                'ts': ms,
                'pid': xxhash.xxh32('mainloop').intdigest(),
                'tid': xxhash.xxh32('mainloop').intdigest(),
                'id': event_counter,
                'args': json_obj
            }
            poped = sync_stack.pop()
            assert poped['name'] == json_obj['event']
            yield end_event
        else:
            pass

def filter_my_event_log_only(json_stream):
    for json in json_stream:
        if 'my_event' not in json or json['my_event'] not in ['await','resume','wait','complete']:
            continue
        yield json


def convert(trace):
    chrome_json = convert_to_chrome_trace(filter_my_event_log_only(load_file_as_line_by_line_json(trace)))

    with open(trace + '.chrome.json','w') as f:
        f.write('[\n')
        for obj in chrome_json:
            f.write(json.dumps(obj))
            f.write(',\n')


if __name__ == '__main__': 

    # parse arg with argument --trace 
    parser = argparse.ArgumentParser(description='Convert async log to chrome trace format, output write as trace.chrome.json')
    parser.add_argument('--trace', dest='trace', required=True, help='async log file')
    # now parse command line
    args = parser.parse_args()
    convert(args.trace)



