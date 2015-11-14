
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import make_transient
import uuid
from datetime import datetime
Base = declarative_base()


class LimType(Base):
    __tablename__ = 'lim._types'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)

    _registry = {}

    @classmethod
    def register(kls, name, cons):
        kls._registry[name] = cons

    @classmethod
    def add_types(kls, session):
        for name in kls._registry:
            if not session.query(LimType).filter_by(name=name).first():
                session.add(LimType(name=name))
    
    @classmethod
    def resolve(kls, name):
        return kls._registry[name]


def limtype(name):
    def wrapper(kls):
        limname = 'lim.'+name
        LimType.register(limname, kls)
        kls.__tablename__ = limname
        return kls
    return wrapper

@limtype('hack.message')
class Message(Base):
    __tablename__ = 'lim.hack.message' # FIXME: merge this into decorator or metaclass
    _uuid = sa.Column(sa.String, primary_key=True)
    _updated = sa.Column(sa.DateTime)
    _tombstone = sa.Column(sa.Boolean)
    text = sa.Column(sa.String)

    @classmethod
    def create_message(kls, text): # FIXME merge this into the constructor
        return kls(_uuid=str(uuid.uuid4()), _updated=datetime.now(), _tombstone=False, text=text)

    def merge(self, other):
        if other._updated != self._updated:
            a, b = (self, other) if self._updated < other._updated else (other, self)
            a._updated, a._tombstone, a._text = b._updated, b._tombstone, b._text
    
    @classmethod
    def list_all(kls, session):
        return session.query(kls).filter_by(_tombstone=False).all()


class LimStore:
    def __init__(self, session):
        self.session = session

    def sync(self, other):
        types = self.types & other.types
        for name in types:
            local = { o._uuid: o for o in self.objects(name) }
            local_uuids = set(local.keys())

            remote = { o._uuid: o for o in other.objects(name) }
            remote_uuids = set(remote.keys())

            for uuid in local_uuids | remote_uuids:
                if uuid not in remote_uuids:
                    o = local[uuid]
                    make_transient(o)
                    other._sync_put(o)
                elif uuid not in local_uuids:
                    o = remote[uuid]
                    make_transient(o)
                    self._sync_put(o)
                else: # obj in local and remote
                    lo, ro = local[uuid], remote[uuid]
                    lo.merge(ro)
                    ro.merge(lo)
                    self.sync_put(lo)
                    other.sync_put(ro)
        self.session.commit()
            
    def _sync_put(self, obj):
        self.session.add(obj)

    def objects(self, typename):
        kls = LimType.resolve(typename)
        return set(self.session.query(kls).all())

    @property
    def types(self):
        return { t.name for t in self.session.query(LimType).all() }

    @classmethod
    def make_engine(kls):
        return sa.create_engine('sqlite:///:memory:', echo=False)

    @classmethod
    def make_session(kls, engine):
        Base.metadata.create_all(engine)
        session = sessionmaker(bind=engine)()
        LimType.add_types(session)
        session.commit()
        return kls(session)

