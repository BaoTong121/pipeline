from .model import db, Graph, Vertex, Edge, Pipeline, Track
from .model import STATE_PENDING, STATE_RUNNING, STATE_FINISH, STATE_SUCCEED, STATE_FAILED, STATE_WAITING
from .service import transactional
from collections import defaultdict

def start(g_id, name:str, desc:str=''):
    # checked
    g = db.session.query(Graph).filter((Graph.id == g_id) & (Graph.checked == 1)).first()
    if not g:
        return

    # 找到所有顶点，拷贝到track
    query = db.session.query(Vertex).filter(Vertex.g_id == g_id)
    vertexes = query.all()
    if not vertexes:
        return

    # 起点， 状态PENDING
    query = query.filter(Vertex.id.notin_(
        db.session.query(Edge.head).filter(Edge.g_id == g_id)
    ))

    zds =[v.id for v in query]
    print(zds, '~~~~~~~~~~~~')

    # 新建一个任务流
    p = Pipeline()
    p.g_id = g_id
    p.name = name
    p.desc = desc
    p.state = STATE_RUNNING
    db.session.add(p)


    # track表初始化所有节点
    for v in vertexes:
        t = Track()
        t.v_id = v.id
        t.pipeline = p
        t.state = STATE_PENDING if v.id in zds else STATE_WAITING
        db.session.add(t)

    # 封闭graph sealed
    if g.sealed == 0:
        g.sealed = 1
        db.session.add(g)

    try:
        db.session.commit()
        print('start ok~~~~~~~~')
        pass # TODO
    except Exception as e:
        db.session.rollback()
        print(e)




def showpipeline(p_id, states=[STATE_PENDING], exclude=[STATE_FAILED]):
    # 显示所有流程的相关信息，流程信息、顶点状态、顶点里面的input和script
    # tracks
    ret = []
    # query = db.session.query(Track).filter(Track.p_id == p_id).filter(Track.state == state)
    # for track in query:
    #     ret.append((track.pipeline.id, track.pipeline.name, track.pipeline.state,
    #                 track.id, track.v_id, track.state,
    #                 track.vertex.input, track.vertex.script))

    query = db.session.query(Pipeline.id, Pipeline.name, Pipeline.state,
                             Track.id, Track.v_id, Track.state,
                             Vertex.input, Vertex.script)\
        .join(Track, Pipeline.id == Track.p_id).\
        join(Vertex, Vertex.id == Track.v_id)\
        .filter(Pipeline.state.notin_(exclude))\
        .filter(Track.p_id == p_id)\
        .filter(Track.state.in_(states))
    return query.all()

import simplejson

TYPES = {
    'str':str,
    'int':int,
    'string':str
    #'ip':IP # 127.001.01.1
}


def finish_params(v_id, d:dict):
    ret = {}
    value = db.session.query(Vertex.input, Vertex.script).filter(Vertex.id == v_id).first()
    print(value, '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
    inp ,script = value
    if inp:
        inp = simplejson.loads(inp)
        #{"required": true, "default": "192.168.0.100", "type": "str"}
        for k,v in inp.items():
            if k in d.keys():
                ret[k] = TYPES[inp[k].get('type', 'str')](d[k])
            elif inp[k].get('default') is not None:
                ret[k] = TYPES[inp[k].get('type', 'str')](inp[k].get('default'))
            else:
                raise TypeError()

    return ret, script

import re


@transactional
def finish_script(t_id, params, script): # t_id 等价 p_id + v_id
    newline = ''
    print('-'*30)
    print(params, type(params)) # {'ip': '192.168.0.100'}
    print(script, type(script)) # str
    if script:
        script = simplejson.loads(script).get('script', '') # dict=>str echo \"test1.A\"\nping {ip}

        regex = re.compile(r'{([^{}]+)}')

        start = 0

        # {"ip": {"required": true, "default": "192.168.0.100", "type": "str"}
        for matcher in regex.finditer(script):
            newline += script[start:matcher.start()]
            print(matcher, matcher.group(1))
            key = matcher.group(1)
            tmp = params.get(key, '') # ip value, type
            newline += str(tmp)
            start = matcher.end()
        else:
            newline += script[start:]

        # 入库track input script
        t = db.session.query(Track).filter(Track.id == t_id).first()
        if t:
            t.input = simplejson.dumps(params)
            t.script = newline
            db.session.add(t)

    return newline


from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import threading, time
from subprocess import Popen
from tempfile import TemporaryFile
# 生成唯一id
import uuid
from queue import Queue

class Executor:
    def __init__(self, workers=5):
        self.__executor = ThreadPoolExecutor(max_workers=workers)
        self.__tasks = {}
        self.__event = threading.Event()
        self.__queue = Queue()
        threading.Thread(target=self._run).start()
        threading.Thread(target=self._save_track).start()

    def _execute(self, script, key):
        # 多行的
        codes = 0

        with TemporaryFile('a+') as f:
            for line in script.splitlines():
                p = Popen(line, shell=True, stdout=f)
                code = p.wait() # 阻塞等
                codes += code
            f.flush()
            f.seek(0)
            text = f.read()
        print('+~' * 30)
        print(key, codes, text)
        return key, codes, text



    def execute(self, t_id, script):
        try:
            t = db.session.query(Track).filter(Track.id == t_id).one()
            key = uuid.uuid4().hex
            self.__tasks[self.__executor.submit(self._execute, script, key)] = key, t_id  # 生成任务

            t.state = STATE_RUNNING
            db.session.add(t)

            db.session.commit()
        except Exception as e:
            db.session.rollback()
            print(e)


    def _run(self):
        while not self.__event.wait(2):
            for future in as_completed(self.__tasks): # 有可能阻塞
                key, t_id = self.__tasks[future]
                print('+++' * 30)
                try:
                    key, code, text = future.result()
                    # 拿到结果干什么？ 异步处理方案
                    self.__queue.put((t_id, code, text)) # 推送到第三方 消息队列
                except Exception as e:
                    print(e)
                    print(key, 'failed')
                finally:
                    del self.__tasks[future]

    def _save_track(self):
        # 存储结果
        # 从Q里面拿数据存储
        while True:
            t_id, code, text = self.__queue.get() # 阻塞拿
            print(t_id, code, text)

            track = db.session.query(Track).filter(Track.id == t_id).one()
            track.state = STATE_SUCCEED if code == 0 else STATE_FAILED
            track.output = text

            if code != 0:
                track.pipeline.state = STATE_FAILED
            else:
                # 流转代码， 隐含 自己成功，看别的顶点
                # pipeline是否失败 ， track表中查找是否有失败的
                tracks = db.session.query(Track).filter((Track.p_id == track.p_id) & (Track.id != t_id)).all()

                states = {STATE_WAITING:0, STATE_PENDING:0, STATE_RUNNING:0, STATE_FAILED:0, STATE_SUCCEED:0}

                for t in tracks:
                    states[t.state] += 1

                if states[STATE_FAILED] > 0:
                    track.pipeline.state = STATE_FAILED
                elif len(tracks) == states[STATE_SUCCEED]: # 说明除去自己之外全是成功的，你当然就是最后的那一个顶点，也就是终点
                    track.pipeline.state = STATE_FINISH
                else: # 还有节点没有做完，判断自己有没有下一级
                    # heads = db.session.query(Edge.head).filter(Edge.tail == track.v_id).all()
                    # if len(heads) == 0:
                    #     pass # 什么都不做，因为你没下一级，就是其中一个先做完的终点
                    # else:
                    query = db.session.query(Edge).filter(Edge.g_id == track.pipeline.g_id)

                    t2h = defaultdict(list)
                    h2t = defaultdict(list)

                    for e in query:
                        t2h[e.tail].append(e.head)
                        h2t[e.head].append(e.tail)

                    if track.v_id in t2h.keys():
                        nexts = t2h[track.v_id]
                        for n in nexts:
                            tails = h2t[n] # n pending 条件是tails所有状态都必须是成功
                            # 统计tails是否都是成功的，可以pending,
                            # select count(state) from track where track.v_id in (1,2,4)
                            # and track.state = STATE_SUCCEED  and pid
                            s_count = db.session.query(Track).filter(Track.p_id == track.p_id)\
                                .filter(Track.v_id.in_(tails))\
                                .filter(Track.state == STATE_SUCCEED).count()
                            if s_count == len(tails):
                                # pending
                                nx = db.session.query(Track).filter(Track.v_id == n).one()
                                nx.state = STATE_PENDING
                                db.session.add(nx)
                            else:
                                pass # 什么都不做

                    else:
                        pass  # 什么都不做，因为你没下一级，就是其中一个先做完的终点


            db.session.add(track)
            try:
                db.session.commit()
                pass # TODO
            except Exception as e:
                db.session.rollback()
                print(e)


EXECUTOR = Executor()

