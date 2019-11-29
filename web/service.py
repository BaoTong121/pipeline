from .model import db, Pipeline, Track, Vertex, Edge

import random
def getxy():
    return random.randint(300, 600)


def getdag(gid): # 定义
    pass


def getpipeline(pid): # 获取一个pipeline已经track
    # p = db.session.query(Pipeline).filter(Pipeline.id == pid)
    # p.tracks  # []
    # track.vertex.name

    query = db.session.query(
        Pipeline.id, Pipeline.name, Pipeline.state, Pipeline.g_id,
        Vertex.id, Vertex.name, Track.state,
        Track.input, Track.script
    ).\
        join(Track, Track.p_id == pid).\
        join(Vertex, Vertex.id == Track.v_id).\
        filter(Pipeline.id == pid)


    # 顶点数据
    data = []
    vertexes = {}
    for p_id, p_name, p_state, g_id, v_id, v_name, t_state, t_input, t_script in query:
        data.append({
            'name': v_name,
            'x':getxy(),
            'y':getxy(),
            'value':t_script
        })
        vertexes[v_id] = v_name

    # 边数据
    links = []
    query = db.session.query(Edge.tail, Edge.head).filter(Edge.g_id == g_id).all()
    for tail, head in query:
        links.append({
            'source' : vertexes[tail],
            'target' : vertexes[head]
        })

    title = p_name

    return {'data':data, 'links':links, 'title':title}