import abc
import datetime as dt
import enum
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

import src.slurm.slurm as slurm
from table import Table

"""
General concept example

1. From cluster pick a collection (via convenience functions)
2. From collection apply a filter (or via convenience functions)
3. From collection pick another collection (via ...)
4. Output to desired format

1. From cluster pick a collection (via ...)
2. From collection select id
3. Output to desired format
"""

VALUE_TYPE = Union[str, int, float, bool, dt.datetime, dt.timedelta]
DATA_VALUE_TYPE = TypeVar("DATA_VALUE_TYPE", str, VALUE_TYPE)
DATA = Dict[str, str]
DATA_T = Dict[str, DATA_VALUE_TYPE]
COLLECTION_T = Dict[str, DATA_T]
COMPONENTS_T = Dict[str, Union[int, float]]

FILTER_FN = Callable[[Any], bool]
FILTER = Dict[str, FILTER_FN]

STATE = "state"


class StateEnum(enum.Enum):
    pass


@enum.unique
class JobState(StateEnum):
    ALL = "all"
    ACTIVE = "active"
    RUNNING = "running"
    PENDING = "pending"
    BLOCKED = "blocked"
    INACTIVE = "inactive"
    COMPLETE = "complete"
    FAILED = "failed"


@enum.unique
class NodeState(StateEnum):
    ALL = "all"
    EXIST = "exist"
    UP = "up"
    ALLOCATED = "allocated"
    IDLE = "idle"
    DOWN = "down"
    MAINTENANCE = "maintenance"
    FAILURE = "failure"
    FUTURE = "future"


@enum.unique
class UserState(StateEnum):
    ALL = "all"
    ACTIVE = "active"
    RUNNING = "running"
    PENDING = "pending"
    INACTIVE = "inactive"


EnumT = TypeVar("EnumT", bound=StateEnum)


class Hierarchy(Generic[EnumT]):
    def __init__(self, _h: List[Dict[EnumT, int]]) -> None:
        self._h: List[Dict[EnumT, int]] = _h

    def is_super(self, lhs: EnumT, rhs: EnumT) -> bool:
        d = next((h for h in self._h if lhs in h and rhs in h), None)
        if d is None:
            return False
        elif d[lhs] <= d[rhs]:
            return True
        elif d[rhs] < d[lhs]:
            return False
        else:
            assert False


class Filter:
    def __init__(self, _f: FILTER) -> None:
        self._f: FILTER = _f

    def apply(self, _c: COLLECTION_T) -> COLLECTION_T:
        out: COLLECTION_T = {}
        for _id, row in _c.items():
            out[_id] = {k: self._f[k](v) if k in self._f else v for k, v in row.items()}
        return out

    @classmethod
    def from_state(cls, _h: Hierarchy[EnumT], _s: EnumT) -> "Filter":
        return cls({STATE: lambda x: _h.is_super(_s, x)})


class DataStore:
    def __init__(self) -> None:
        jobs = slurm.Scontrol().get_jobs()
        nodes = slurm.Scontrol().get_nodes()
        partitions = slurm.Scontrol().get_partitions()
        qoses = slurm.Sacctmgr().get_qoses()

        self._jobs: COLLECTION_T = {}
        self._nodes: COLLECTION_T = {}
        self._users: COLLECTION_T = {}
        self._qoses: COLLECTION_T = {}
        self._partitions: COLLECTION_T = {}

        # job to nodes: job[id][nodes]
        # job to partition: job[id][partition]
        # job to user: job[id][user]
        # job to components: jobs[id][components]

        self._node_to_jobs: Dict[str, str] = {}
        self._node_to_partitions: Dict[str, str] = {}
        # node to users: node[id].jobs().users()
        # node to components: nodes[id][components]

        # partition to jobs: partition[id][nodes].jobs()
        # partition to nodes: partition[id][nodes]
        # partition to qoses: partition[id][qoses]
        # partition to users: partition[id][nodes].users()
        # partition to components: partition[id].nodes().components()

        self._user_to_jobs: Dict[str, str] = {}
        self._user_to_nodes: Dict[str, str] = {}
        # user to components: user[id].jobs().components()

    def jobs(self) -> Set[str]:
        return set(self._jobs.keys())

    def nodes(self) -> Set[str]:
        return set(self._nodes.keys())

    def partitions(self) -> Set[str]:
        return set(self._partitions.keys())

    def qoses(self) -> Set[str]:
        return set(self._qoses.keys())

    def users(self) -> Set[str]:
        return set(self._partitions.keys())

    def get_job(self, _id: str) -> DATA_T:
        return self._jobs[_id]

    def get_jobs(self, _ids: Set[str]) -> COLLECTION_T:
        return self._subset(self._jobs, _ids)

    def get_node(self, _id: str) -> DATA_T:
        return self._nodes[_id]

    def get_nodes(self, _ids: Set[str]) -> COLLECTION_T:
        return self._subset(self._nodes, _ids)

    def get_partition(self, _id: str) -> DATA_T:
        return self._partitions[_id]

    def get_partitions(self, _ids: Set[str]) -> COLLECTION_T:
        return self._subset(self._partitions, _ids)

    def get_qos(self, _id: str) -> DATA_T:
        return self._qoses[_id]

    def get_qoses(self, _ids: Set[str]) -> COLLECTION_T:
        return self._subset(self._qoses, _ids)

    def get_user(self, _id: str) -> DATA_T:
        return self._users[_id]

    def get_users(self, _ids: Set[str]) -> COLLECTION_T:
        return self._subset(self._users, _ids)

    def filter_jobs(self, _f: Filter, _ids: Set[str]) -> Set[str]:
        _c = self.get_jobs(_ids)
        return self._filter_on(_f, _c)

    def filter_nodes(self, _f: Filter, _ids: Set[str]) -> Set[str]:
        _c = self.get_nodes(_ids)
        return self._filter_on(_f, _c)

    def filter_partitions(self, _f: Filter, _ids: Set[str]) -> Set[str]:
        _c = self.get_partitions(_ids)
        return self._filter_on(_f, _c)

    def filter_qoses(self, _f: Filter, _ids: Set[str]) -> Set[str]:
        _c = self.get_qoses(_ids)
        return self._filter_on(_f, _c)

    def filter_users(self, _f: Filter, _ids: Set[str]) -> Set[str]:
        _c = self.get_users(_ids)
        return self._filter_on(_f, _c)

    def job_to_nodes(self, _id: str) -> Set[str]:
        return set(self._jobs[_id]["nodes"])

    def job_to_partition(self, _id: str) -> str:
        return self._jobs[_id]["partition"]

    def job_to_user(self, _id: str) -> str:
        return self._jobs[_id]["user"]

    def job_to_components(self, _id: str) -> "Components":
        return Components.from_job(Job(self, _id, self.get_job(_id)))

    def node_to_jobs(self, _id: str) -> Set[str]:
        return set(self._node_to_jobs[_id])

    def node_to_partitions(self, _id: str) -> Set[str]:
        return set(self._node_to_partitions[_id])

    def node_to_users(self, _id) -> Set[str]:
        jobs = self.node_to_jobs(_id)
        return set(self.job_to_user(_job_id) for _job_id in jobs)

    def node_to_job_components(self, _id: str) -> "Components":
        jobs = self.node_to_jobs(_id)
        return self.sum_components(self.job_to_components, jobs)

    def node_to_hardware_components(self, _id: str) -> "Components":
        return Components.from_node(Node(self, _id, self.get_node(_id)))

    def partition_to_jobs(self, _id: str) -> Set[str]:
        nodes = self.partition_to_nodes(_id)
        out: Set[str] = set()
        [out.union(self.node_to_jobs(node)) for node in nodes]
        return out

    def partition_to_nodes(self, _id: str) -> Set[str]:
        return set(self._partitions[_id]["nodes"])

    def partition_to_qoses(self, _id: str) -> Set[str]:
        return set(self._partitions[_id]["qoses"])

    def partition_to_users(self, _id: str) -> Set[str]:
        jobs = self.partition_to_jobs(_id)
        return set(self.job_to_user(_job_id) for _job_id in jobs)

    def partition_to_job_components(self, _id: str) -> "Components":
        nodes = self.partition_to_nodes(_id)
        return self.sum_components(self.node_to_job_components, nodes)

    def partition_to_hardware_components(self, _id: str) -> "Components":
        nodes = self.partition_to_nodes(_id)
        return self.sum_components(self.node_to_hardware_components, nodes)

    def user_to_jobs(self, _id: str) -> Set[str]:
        return set(self._user_to_jobs[_id])

    def user_to_nodes(self, _id: str) -> Set[str]:
        return set(self._user_to_nodes[_id])

    def user_to_job_components(self, _id: str) -> "Components":
        jobs = self.user_to_jobs(_id)
        return self.sum_components(self.job_to_components, jobs)

    def transform_one_to_many(
        self, fn: Callable[[str], Set[str]], _ids: Set[str]
    ) -> Set[str]:
        target_ids: Set[str] = set()
        [target_ids.union(fn(source_id)) for source_id in _ids]
        return target_ids

    def transform_one_to_one(
        self, fn: Callable[[str], str], _ids: Set[str]
    ) -> Set[str]:
        target_ids: Set[str] = set()
        [target_ids.add(fn(source_id)) for source_id in _ids]
        return target_ids

    def sum_components(
        self, fn: Callable[[str], "Components"], _ids: Set[str]
    ) -> "Components":
        components: Components = Components.empty()
        for _id in _ids:
            components += fn(_id)
        return components

    def _subset(self, _collection: COLLECTION_T, _ids: Set[str]) -> COLLECTION_T:
        return {k: _collection[k] for k in _ids}

    def _filter_on(self, _f: Filter, _c: COLLECTION_T) -> Set[str]:
        fs = _f.apply(_c)
        out = set(fs.keys())
        return out


class Entity(abc.ABC):
    def __init__(self, _ds: DataStore, _id: str, _data: DATA_T) -> None:
        self._ds: DataStore = _ds
        self._id: str = _id
        self._data: DATA_T = _data

    @property
    def id(self) -> str:
        return self._id

    def __contains__(self, _k: str) -> bool:
        return _k in self._data

    def __getitem__(self, _k: str) -> Optional[DATA_VALUE_TYPE]:
        return self._data.get(_k, None)

    def items(self) -> Iterable[Tuple[str, Any]]:
        return iter(self._data.items())

    def keys(self) -> Iterable[str]:
        return iter(self._data.keys())

    def values(self) -> Iterable[Any]:
        return iter(self._data.values())

    def serialize(self, serializer) -> str:
        raise NotImplementedError

    # @abc.abstractmethod
    # def summarize(self) -> str:
    #     ...

    # @abc.abstractmethod
    # def pretty_print(self) -> str:
    #     ...

    def to_json(self) -> Any:
        ...


class Job(Entity):
    def Components(self) -> "Components":
        return self._ds.job_to_components(self._id)

    def Nodes(self) -> "Nodes":
        _ids = self._ds.job_to_nodes(self._id)
        return Nodes(self._ds, _ids)

    def User(self) -> "User":
        _id = self._ds.job_to_user(self._id)
        return User(self._ds, _id, self._ds.get_user(_id))

    def Partition(self) -> "Partition":
        _id = self._ds.job_to_partition(self._id)
        return Partition(self._ds, _id, self._ds.get_partition(_id))


class Node(Entity):
    def HardwareComponents(self) -> "Components":
        return self._ds.node_to_hardware_components(self._id)

    def JobComponents(self) -> "Components":
        return self._ds.node_to_job_components(self._id)

    def Jobs(self) -> "Jobs":
        _ids = self._ds.node_to_jobs(self._id)
        return Jobs(self._ds, _ids)

    def Users(self) -> "Users":
        _ids = self._ds.node_to_users(self._id)
        return Users(self._ds, _ids)

    def Partitions(self) -> "Partitions":
        _ids = self._ds.node_to_partitions(self._id)
        return Partitions(self._ds, _ids)


class User(Entity):
    def JobComponents(self) -> "Components":
        return self._ds.user_to_job_components(self._id)

    def Jobs(self) -> "Jobs":
        _ids = self._ds.user_to_jobs(self._id)
        return Jobs(self._ds, _ids)

    def Nodes(self) -> "Nodes":
        _ids = self._ds.user_to_nodes(self._id)
        return Nodes(self._ds, _ids)


class Partition(Entity):
    def HardwareComponents(self) -> "Components":
        return self._ds.partition_to_hardware_components(self._id)

    def JobComponents(self) -> "Components":
        return self._ds.partition_to_job_components(self._id)

    def Jobs(self) -> "Jobs":
        _ids = self._ds.partition_to_jobs(self._id)
        return Jobs(self._ds, _ids)

    def Nodes(self) -> "Nodes":
        _ids = self._ds.partition_to_nodes(self._id)
        return Nodes(self._ds, _ids)

    def Qoses(self) -> "Qoses":
        _ids = self._ds.partition_to_qoses(self._id)
        return Qoses(self._ds, _ids)

    def Users(self) -> "Users":
        _ids = self._ds.partition_to_users(self._id)
        return Users(self._ds, _ids)


class Qos(Entity):
    pass


class Cluster:
    def __init__(self, _ds: DataStore) -> None:
        self._ds: DataStore = _ds

    def jobs(self) -> "Jobs":
        return Jobs(self._ds, self._ds.jobs())

    def nodes(self) -> "Nodes":
        return Nodes(self._ds, self._ds.nodes())

    def users(self) -> "Users":
        return Users(self._ds, self._ds.users())

    def hardware_components(self) -> "Components":
        return self.nodes().hardware_components()

    def job_components(self) -> "Components":
        return self.nodes().job_components()

    def partitions(self) -> "Partitions":
        return Partitions(self._ds, self._ds.partitions())

    def qoses(self) -> "Qoses":
        return Qoses(self._ds, self._ds.qoses())


ChildT = TypeVar("ChildT", bound="Collection")


class Collection(abc.ABC):
    def __init__(self, _ds: DataStore, _ids: Set[str]):
        self._ds: DataStore = _ds
        self._ids: Set[str] = _ids

    @property
    @abc.abstractmethod
    def _HIERARCHY(self) -> Hierarchy[StateEnum]:
        ...

    def __len__(self) -> int:
        return len(self._ids)

    def __iter__(self) -> Iterator[str]:
        return iter(self._ids)

    # def to_table(self) -> Table:
    #     pass

    # @abc.abstractmethod
    # def summarize(self) -> str:
    #     ...

    # @abc.abstractmethod
    # def pretty_print(self) -> str:
    #     ...

    def to_json(self) -> Any:
        ...

    def from_ids(self: ChildT, _ids: Set[str]) -> ChildT:
        return type(self)(self._ds, _ids)

    def _filter_state(
        self: ChildT, _filter_fn: Callable[[Filter, Set[str]], Set[str]], _s: StateEnum
    ) -> ChildT:
        f = Filter.from_state(self._HIERARCHY, _s)
        ids = _filter_fn(f, self._ids)
        return self.from_ids(ids)


class Jobs(Collection):
    _HIERARCHY: Hierarchy[JobState] = Hierarchy[JobState](
        [
            {JobState.ALL: 0, JobState.ACTIVE: 1, JobState.RUNNING: 2},
            {JobState.ALL: 0, JobState.ACTIVE: 1, JobState.PENDING: 2},
            {JobState.ALL: 0, JobState.BLOCKED: 1},
            {JobState.ALL: 0, JobState.INACTIVE: 1, JobState.COMPLETE: 2},
            {JobState.ALL: 0, JobState.INACTIVE: 1, JobState.FAILED: 2},
        ]
    )

    def components(self) -> "Components":
        return self._ds.sum_components(self._ds.job_to_components, self._ids)

    def nodes(self) -> "Nodes":
        ids = self._ds.transform_one_to_many(self._ds.job_to_nodes, self._ids)
        return Nodes(self._ds, ids)

    def users(self) -> "Users":
        ids = self._ds.transform_one_to_one(self._ds.job_to_user, self._ids)
        return Users(self._ds, ids)

    def partitions(self) -> "Partitions":
        ids = self._ds.transform_one_to_one(self._ds.job_to_partition, self._ids)
        return Partitions(self._ds, ids)

    def active(self) -> "Jobs":
        return self._filter_job_state(JobState.ACTIVE)

    def running(self) -> "Jobs":
        return self._filter_job_state(JobState.RUNNING)

    def pending(self) -> "Jobs":
        return self._filter_job_state(JobState.PENDING)

    def blocked(self) -> "Jobs":
        return self._filter_job_state(JobState.BLOCKED)

    def inactive(self) -> "Jobs":
        return self._filter_job_state(JobState.INACTIVE)

    def complete(self) -> "Jobs":
        return self._filter_job_state(JobState.COMPLETE)

    def failed(self) -> "Jobs":
        return self._filter_job_state(JobState.FAILED)

    def _filter_job_state(self, _s: JobState) -> "Jobs":
        return self._filter_state(self._ds.filter_jobs, _s)


class Nodes(Collection):
    _HIERARCHY: Hierarchy[NodeState] = Hierarchy[NodeState](
        [
            {
                NodeState.ALL: 0,
                NodeState.EXIST: 1,
                NodeState.UP: 2,
                NodeState.ALLOCATED: 3,
            },
            {
                NodeState.ALL: 0,
                NodeState.EXIST: 1,
                NodeState.UP: 2,
                NodeState.IDLE: 3,
            },
            {
                NodeState.ALL: 0,
                NodeState.EXIST: 1,
                NodeState.DOWN: 2,
                NodeState.MAINTENANCE: 3,
            },
            {
                NodeState.ALL: 0,
                NodeState.EXIST: 1,
                NodeState.DOWN: 2,
                NodeState.FAILURE: 3,
            },
            {NodeState.ALL: 0, NodeState.FUTURE: 1},
        ]
    )

    def jobs(self) -> "Jobs":
        ids = self._ds.transform_one_to_many(self._ds.node_to_jobs, self._ids)
        return Jobs(self._ds, ids)

    def users(self) -> "Users":
        ids = self._ds.transform_one_to_many(self._ds.node_to_users, self._ids)
        return Users(self._ds, ids)

    def partitions(self) -> "Partitions":
        ids = self._ds.transform_one_to_many(self._ds.node_to_partitions, self._ids)
        return Partitions(self._ds, ids)

    def hardware_components(self) -> "Components":
        return self._ds.sum_components(self._ds.node_to_hardware_components, self._ids)

    def job_components(self) -> "Components":
        return self._ds.sum_components(self._ds.node_to_job_components, self._ids)

    def exist(self) -> "Nodes":
        return self._filter_node_state(NodeState.EXIST)

    def up(self) -> "Nodes":
        return self._filter_node_state(NodeState.UP)

    def allocated(self) -> "Nodes":
        return self._filter_node_state(NodeState.ALLOCATED)

    def idle(self) -> "Nodes":
        return self._filter_node_state(NodeState.IDLE)

    def down(self) -> "Nodes":
        return self._filter_node_state(NodeState.DOWN)

    def maintenance(self) -> "Nodes":
        return self._filter_node_state(NodeState.MAINTENANCE)

    def failure(self) -> "Nodes":
        return self._filter_node_state(NodeState.FAILURE)

    def future(self) -> "Nodes":
        return self._filter_node_state(NodeState.FUTURE)

    def _filter_node_state(self, _s: NodeState) -> "Nodes":
        return self._filter_state(self._ds.filter_nodes, _s)


class Partitions(Collection):
    _HIERARCHY: None = None

    def hardware_components(self) -> "Components":
        return self._ds.sum_components(
            self._ds.partition_to_hardware_components, self._ids
        )

    def job_components(self) -> "Components":
        return self._ds.sum_components(self._ds.partition_to_job_components, self._ids)

    def jobs(self) -> "Jobs":
        ids = self._ds.transform_one_to_many(self._ds.partition_to_jobs, self._ids)
        return Jobs(self._ds, ids)

    def nodes(self) -> "Nodes":
        ids = self._ds.transform_one_to_many(self._ds.partition_to_nodes, self._ids)
        return Nodes(self._ds, ids)

    def qoses(self) -> "Qoses":
        ids = self._ds.transform_one_to_many(self._ds.partition_to_qoses, self._ids)
        return Qoses(self._ds, ids)

    def users(self) -> "Users":
        ids = self._ds.transform_one_to_many(self._ds.partition_to_users, self._ids)
        return Users(self._ds, ids)


class Qoses(Collection):
    _HIERARCHY: None = None


class Users(Collection):
    _HIERARCHY: Hierarchy[UserState] = Hierarchy[UserState](
        [
            {
                UserState.ALL: 0,
                UserState.ACTIVE: 1,
                UserState.RUNNING: 2,
            },
            {
                UserState.ALL: 0,
                UserState.ACTIVE: 1,
                UserState.PENDING: 2,
            },
            {
                UserState.ALL: 0,
                UserState.INACTIVE: 1,
            },
        ]
    )

    def jobs(self) -> "Jobs":
        ids = self._ds.transform_one_to_many(self._ds.user_to_jobs, self._ids)
        return Jobs(self._ds, ids)

    def nodes(self) -> "Nodes":
        ids = self._ds.transform_one_to_many(self._ds.user_to_nodes, self._ids)
        return Nodes(self._ds, ids)

    def job_components(self) -> "Components":
        return self._ds.sum_components(self._ds.user_to_job_components, self._ids)

    def active(self) -> "Users":
        return self._filter_user_state(UserState.ACTIVE)

    def running(self) -> "Users":
        return self._filter_user_state(UserState.RUNNING)

    def pending(self) -> "Users":
        return self._filter_user_state(UserState.PENDING)

    def inactive(self) -> "Users":
        return self._filter_user_state(UserState.INACTIVE)

    def _filter_user_state(self, _s: UserState) -> "Users":
        return self._filter_state(self._ds.filter_users, _s)


class Components:
    def __init__(self, _d: Optional[DATA_T]) -> None:
        if _d is None:
            _d = {}

        self._d: DATA_T = _d

    def __add__(self, _other: "Components") -> "Components":
        all_keys = set(self._d.keys()) | _other._d.keys()
        out_d: DATA_T = {}
        for k in all_keys:
            vs = self._d.get(k, 0)
            os = _other._d.get(k, 0)
            out_d[k] = vs + os
        out = Components(out_d)
        return out

    @classmethod
    def empty(cls) -> "Components":
        return cls({})

    @classmethod
    def from_job(cls, job: Job) -> "Components":
        return cls(job["components"])

    @classmethod
    def from_node(cls, node: Node) -> "Components":
        return cls(node["components"])
