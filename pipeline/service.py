from .model import db
from .model import Graph, Vertex, Edge
from .model import Pipeline, Track

def transactional(fn):
    def wrapper(*args, **kwargs):
        ret = fn(*args, **kwargs)
        try:
            db.session.commit()
            return ret
        except Exception as e:
            print(e)
    return wrapper


# 创建DAG
@transactional #  create_graph = transactional(create_graph)
def create_graph(name, desc=None):
    g = Graph()
    g.name = name
    g.desc = desc
    db.session.add(g)
    return g


# 为DAG增加顶点
@transactional
def add_vertex(graph:Graph, name:str, input=None, script=None):
    v = Vertex()
    v.g_id = graph.id
    v.name = name
    v.input = input
    v.script = script
    db.session.add(v)
    return v


# 为DAG增加边
@transactional
def add_edge(graph:Graph, tail:Vertex, head:Vertex):
    e = Edge()
    e.g_id = graph.id
    e.tail = tail.id
    e.head = head.id
    db.session.add(e)
    return e

# 删除顶点
# 删除顶点就要删除所有顶点关联的边
@transactional
def del_vertex(id):
    query = db.session.query(Vertex).filter(Vertex.id == id)
    v = query.first()
    if v: # 找到顶点后，删除关联的边，然后删除顶点
        db.session.query(Edge).filter((Edge.tail == v.id) | (Edge.head == v.id)).delete()
        query.delete()
    return v
## 其它增删改方法，都差不多，不再赘述


from collections import defaultdict
def check_graph(graph:Graph) -> bool:
    query = db.session.query(Vertex).filter(Vertex.g_id == graph.id)
    vertexes = {vertex.id for vertex in query}

    query = db.session.query(Edge).filter(Edge.g_id == graph.id)
    edges = defaultdict(list)
    ids = set() # 有入度的顶点
    for edge in query:
        # defaultdict(<class 'list'>, {1: [(1, 2), (1, 3)], 2: [(2, 4)], 3: [(3, 2)]})
        edges[edge.tail].append((edge.tail, edge.head))
        ids.add(edge.head)
    print('-='*30)
    print(vertexes, edges)

    # ===============测试数据===============
    # {1, 2, 3, 4}
    # defaultdict(<class 'list'>, {1: [(1, 2), (1, 3)], 2: [(2, 4)], 3: [(3, 2)]})
    # vertexes = {1, 2, 3, 4}
    # edges = {1: [(1, 2), (1, 3)], 2: [(2, 4)], 3: [(3, 2)]}
    # ids = set() # 有入度的顶点
    # =====================================

    if len(edges) == 0:
        return False # 一条边都没有，这样的DAG业务上不用
    # 如果edges不为空，一定有ids，也就是有入度的顶点一定会有
    zds = vertexes - ids # zds入度为0的顶点
    # zds为0说明没有找到入度为0的顶点，算法终止
    if len(zds):
        for zd in zds:
            if zd in edges:
                del edges[zd]

        while edges:
            # 将顶点集改为当前入度顶点集ids
            # 能到这一步说明出度为0的已经清除了
            vertexes = ids
            ids = set() # 重新寻找有入度的顶点

            for lst in edges.values():
                for edge in lst:
                    ids.add(edge[1])
            zds = vertexes - ids
            print(vertexes, ids, zds)
            if len(zds) == 0:
                break
            for zd in zds:
                if zd in edges: # 有可能顶点没有出度
                    del edges[zd]
            print(edges)

    # 边集为空，剩下所有顶点都是入度为0的，都可以多次迭代删除掉
    if len(edges) == 0:
        # 检验通过，修改checked字段为1
        try:
            graph = db.session.query(Graph).filter(Graph.id == graph.id).first()
            if graph:
                graph.checked = 1
            db.session.add(graph)
            db.session.commit()
            return True
        except Exception as e:
            db.session.rollback()
            raise e
    return False

