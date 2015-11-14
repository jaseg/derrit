
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import make_transient
from sqlalchemy.sql.expression import func
import uuid
from datetime import datetime
Base = declarative_base()


class HostMeta(Base):
    __tablename__ = 'lim._host_meta'
    key = sa.Column(sa.String, primary_key=True)
    value = sa.Column(sa.String)

    @classmethod
    def get(kls, session, key):
        return session.query(kls).filter_by(key=key).first().value

    @classmethod
    def put(kls, session, key, value):
        obj = session.query(kls).filter_by(key=key).first()
        if obj:
            obj.value = value
        else:
            obj = kls(key=key, value=value)
        session.add(obj)

class SyncState(Base):
    __tablename__ = 'lim._sync_state'
    _id = sa.Column(sa.Integer, primary_key=True)
    host = sa.Column(sa.String)
    limtype = sa.Column(sa.String)
    version = sa.Column(sa.Integer)

    @classmethod
    def get_version(kls, session, host, limtype):
        st = session.query(kls).filter_by(host=host, limtype=limtype).first()
        return st.version if st is not None else -1

    @classmethod
    def bump_version(kls, session, host, limtype, version):
        st = session.query(kls).filter_by(host=host, limtype=limtype).first()
        if st:
            st.version = version
        else:
            st = SyncState(host=host, limtype=limtype, version=version)
        session.add(st)


class LimType(Base):
    __tablename__ = 'lim._types'
    _id = sa.Column(sa.Integer, primary_key=True)
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
    _id = sa.Column(sa.Integer, primary_key=True)
    _uuid = sa.Column(sa.String)
    _updated = sa.Column(sa.DateTime)
    _tombstone = sa.Column(sa.Boolean)
    _version = sa.Column(sa.Integer)
    text = sa.Column(sa.String)

    @classmethod
    def insert_message(kls, session, text): # FIXME merge this into the constructor
        inst = kls(_uuid=str(uuid.uuid4()), _updated=datetime.now(), _tombstone=False, text=text, _version=self.cur_ver(session))
        session.add(inst)
        return inst._uuid

    def bump(self, new_ver):
        return Message(_uuid=self._uuid, _updated=self._updated, _tombstone=self._tombstone, _version=new_ver, text=self.text)

    def merge(self, other, base):
        a, b = (self, other) if self._updated < other._updated else (other, self)
        a._updated, a._tombstone, a.text = b._updated, b._tombstone, b.text

    @classmethod
    def cur_ver(kls, session):
        ver = session.query(func.max(kls._version)).scalar()
        return ver if ver is not None else -1
    
    @classmethod
    def format_all(kls, session):
        return [m.text for m in session.query(kls).filter_by(_tombstone=False, _version=kls.cur_ver(session)).all()]

@limtype('hack.votes')
class Votes(Base):
    __tablename__ = 'lim.hack.votes'
    _id = sa.Column(sa.Integer, primary_key=True)
    _uuid = sa.Column(sa.String)
    _updated = sa.Column(sa.DateTime)
    _tombstone = sa.Column(sa.Boolean)
    _version = sa.Column(sa.Integer)
    value = sa.Column(sa.Integer)

    @classmethod
    def create_votes(kls, session): # FIXME merge this into the constructor
        inst = kls(_uuid=str(uuid.uuid4()), _updated=datetime.now(), _tombstone=False, value=0, _version=kls.cur_ver(session))
        session.add(inst)
        return inst._uuid
    
    def up(self):
        self.value += 1
        self._updated = datetime.now()

    def bump(self, new_ver):
        return Votes(_uuid=self._uuid, _updated=self._updated, _tombstone=self._tombstone, _version=new_ver, value=self.value)

    def prepare_merge(self, other, base):
        if self._updated < other._updated:
            self._updated, self._tombstone = other._updated, other._tombstone
        return other.value - base.value

    def apply_merge(self, delta):
        self.value += delta
    
    @classmethod
    def cur_ver(kls, session):
        ver = session.query(func.max(kls._version)).scalar()
        return ver if ver is not None else -1

    @classmethod
    def format_all(kls, session):
        return ['{}@{}: {}'.format(v._uuid, v._version, v.value) for v in session.query(kls).filter_by(_tombstone=False, _version=kls.cur_ver(session)).all()]


class LimStore:
    def __init__(self, session):
        self.session = session

    def sync(self, other):
        types = self.types & other.types
        other_id = other.host_id
        for name in types:
            new_ver = max(self.local_version(name), other.local_version(name))+1
            basever = self.get_version(other.host_id, name)

            # hackedyhack. within this lies almost infinite potential for optimization.
            for o in self.objects(name):
                self.session.add(o.bump(new_ver))
            for o in other.objects(name):
                other.session.add(o.bump(new_ver))

            self.bump_version(other.host_id, name, new_ver)
            other.bump_version(self.host_id, name, new_ver)
            new_ver += 1

            self.session.commit()
            other.session.commit()

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
                    lo, ro, base = local[uuid], remote[uuid], self.find_base(name, uuid, basever)
                    if lo._updated != ro._updated:
                        lot, rot = lo.prepare_merge(ro, base), ro.prepare_merge(lo, base)
                        lo.apply_merge(lot)
                        ro.apply_merge(rot)
                        self._sync_put(lo)
                        other._sync_put(ro)
            self.session.commit()
            other.session.commit()

            # hackedyhack. within this lies almost infinite potential for optimization.
            for o in self.objects(name):
                self.session.add(o.bump(new_ver))
            for o in other.objects(name):
                other.session.add(o.bump(new_ver))
            self.session.commit()
            other.session.commit()
            
    def _sync_put(self, obj):
        self.session.add(obj)

    @property
    def host_id(self):
        return HostMeta.get(self.session, 'host_id')

    def get_version(self, host, limtype):
        return SyncState.get_version(self.session, host, limtype)

    def local_version(self, limtype):
        return LimType.resolve(limtype).cur_ver(self.session)

    def bump_version(self, host, limtype, version):
        SyncState.bump_version(self.session, host, limtype, version)

    def objects(self, typename):
        kls = LimType.resolve(typename)
        return self.session.query(kls).filter_by(_version=kls.cur_ver(self.session)).all()
    
    def find_base(self, typename, uuid, basever):
        return self.session.query(LimType.resolve(typename)).filter_by(_uuid=uuid, _version=basever).first()
    
    def get_obj(self, typename, uuid):
        kls = LimType.resolve(typename)
        return self.session.query(kls).filter_by(_uuid=uuid, _version=kls.cur_ver(self.session)).first()

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
        HostMeta.put(session, 'host_id', str(uuid.uuid4()))
        LimType.add_types(session)
        session.commit()
        return kls(session)

