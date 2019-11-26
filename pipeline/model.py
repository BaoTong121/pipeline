from sqlalchemy import Column, Integer, String, Text, ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

from .config import URL, DATABASE_DEBUG


STATE_WAITING = 0
STATE_PENDING = 1
STATE_RUNNING = 2
STATE_SUCCEED = 3
STATE_FAILED  = 4
STATE_FINISH  = 5

Base = declarative_base()

class Graph(Base):
    __tablename__ = 'graph'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(48), nullable=False, unique=True)
    desc = Column(String(100), nullable=True)
    checked = Column(Integer, nullable=False, default=0)
    sealed = Column(Integer, nullable=False, default=0)


    def __repr__(self):
        return "<Graph {} {}>".format(self.id, self.name)

    __str__ = __repr__

class Vertex(Base):
    __tablename__ = 'vertex'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(48), nullable=False)
    input = Column(Text, nullable=True)
    script = Column(Text, nullable=True)
    g_id = Column(Integer, ForeignKey('graph.id'))

    def __repr__(self):
        return "<Vertex {} {}>".format(self.id, self.name)

    __str__ = __repr__

class Edge(Base):
    __tablename__ = 'edge'

    id = Column(Integer, primary_key=True, autoincrement=True)
    tail = Column(Integer, ForeignKey('vertex.id'), nullable=False)
    head = Column(Integer, ForeignKey('vertex.id'), nullable=False)
    g_id = Column(Integer, ForeignKey('graph.id'), nullable=False)

    def __repr__(self):
        return "<Edge {} {}>".format(self.id, self.name)

    __str__ = __repr__

class Pipeline(Base):
    __tablename__ = 'pipeline'

    id = Column(Integer, primary_key=True, autoincrement=True)
    g_id = Column(Integer, ForeignKey('graph.id'), nullable=False)
    #current = Column(Integer, ForeignKey('vertex.id'), nullable=False)
    name = Column(String(48), nullable=False)
    state = Column(Integer, nullable=False, default=STATE_WAITING)
    desc = Column(String(200), nullable=True)

    tracks = relationship('Track', foreign_keys="Track.p_id")
    # pipeline = db.session.query(Pipeline).filter(Pipeline.id = 1).one()
    # pipeline.tracks [Track类型的对象]

    def __repr__(self):
        return "<Pipeline {} {}>".format(self.id, self.name)


    __str__ = __repr__

class Track(Base):
    __tablename__ = 'track'

    id = Column(Integer, primary_key=True, autoincrement=True)
    p_id = Column(Integer, ForeignKey('pipeline.id'), nullable=False)
    v_id = Column(Integer, ForeignKey('vertex.id'), nullable=False)
    input = Column(Text, nullable=True)
    script = Column(Text, nullable=True)
    output = Column(Text, nullable=True)
    state = Column(Integer, nullable=False, default=STATE_WAITING)

    vertex = relationship('Vertex') # t.vertex.name
    pipeline = relationship('Pipeline')# t.pipeline.name
    # tracks = db.session.query(Track).filter(Track.p_id = 1).all() []
    # track.vertex.name

    def __repr__(self):
        return "<Track {} {} {}>".format(self.id, self.p_id, self.v_id)

    __str__ = __repr__


# 装饰器
from functools import wraps

def singleton(cls):
    instance = None
    @wraps(cls)
    def wrapper(*args, **kwargs):
        nonlocal instance
        if not instance:
            instance = cls(*args, **kwargs)
        return instance
    return wrapper


@singleton
class Database:
    # def __new__(cls, *args, **kwargs):
    #     if hasattr(cls, '_instance'):
    #         return cls._instance
    #     cls._instance = super().__new__(cls)
    #
    #     cls._instance._engine = create_engine(args[0], **kwargs)
    #     cls._instance._session = sessionmaker(bind=cls._instance._engine)()
    #     return cls._instance

    def __init__(self, url, **kwargs):
        self._engine = create_engine(url, **kwargs)
        self._session = sessionmaker(bind=self._engine)()

    @property
    def session(self):
        return self._session

    @property
    def engine(self):
        return self._engine


    def create_all(self):
        Base.metadata.create_all(self._engine)


    def drop_all(self):
        Base.metadata.drop_all(self._engine)

    def __repr__(self):
        return "<{} {} {} {}>".format(self.__class__.__name__, id(self), id(self._engine), id(self._session))



db = Database(URL, echo=DATABASE_DEBUG)

