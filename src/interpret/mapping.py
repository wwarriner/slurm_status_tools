from src.functional import noop
from src.interpret._safe_convert import (
    type_cast_float_unsafe_to_none,
    type_cast_int_unsafe_to_none,
)
from src.interpret.data_structures import ExitCode, GresSpec, NTasksPerNBSC, ReqBSCT
from src.interpret.decode import (
    comma_separated_key_value_list,
    comma_separated_list,
    dont_care_int,
    int_bool,
    ns_int,
    separated_key_value,
    yes_no_bool,
)
from src.interpret.memory import (
    mb_to_bytes,
    memory_mbytes_to_bytes,
    unlimited_memory_mbytes_to_bytes,
)
from src.interpret.node import NodeList
from src.interpret.time import (
    duration_timedelta,
    minutes_timedelta,
    seconds_timedelta,
    timepoint_datetime,
)
from src.interpret.tres import TresSpec

"""
Dictionaries mapping keys to interpreter functions
All functions are passed a single string argument
Keys missing from dictionaries are skipped

noop -> str
_ignore -> None

_NOT_IMPLEMENTED are skipped
"""


def _ignore(*args, **kwargs) -> None:
    return None


_WRITE_ONLY = None
_NOT_IMPLEMENTED = None

# https://slurm.schedmd.com/scontrol.html#SECTION_JOBS---SPECIFICATIONS-FOR-SHOW-COMMAND
from_scontrol_jobs_show = {
    "allocnode:sid": _ignore,
    "batchflag": _ignore,
    "exitcode": ExitCode.from_string,
    "groupid": noop,
    "jobstate": noop,
    "nodelistindices": _ignore,
    "ntaskspern:b:s:c": NTasksPerNBSC.from_string,
    "preempteligibletime": timepoint_datetime,
    "preempttime": timepoint_datetime,
    "presustime": timepoint_datetime,
    "reason": noop,
    "reqb:s:c:t": ReqBSCT.from_string,
    "secspresuspend": seconds_timedelta,
    "socks/node": dont_care_int,
    "submittime": timepoint_datetime,
    "suspendtime": timepoint_datetime,
}

# https://slurm.schedmd.com/scontrol.html#SECTION_JOBS---SPECIFICATIONS-FOR-UPDATE-COMMAND
from_scontrol_jobs_update = {
    "account": noop,
    "admincomment": noop,
    "arraytaskthrottle": _WRITE_ONLY,
    "burstbuffer": _WRITE_ONLY,
    "clusters": _WRITE_ONLY,
    "clusterfeatures": _WRITE_ONLY,
    "comment": noop,
    "contiguous": int_bool,
    "corespec": int,
    "cpuspertask": int,
    "deadline": timepoint_datetime,  # todo accept "N/A"
    "delayboot": duration_timedelta,
    "dependency": _NOT_IMPLEMENTED,
    "eligibletime": timepoint_datetime,
    "endtime": timepoint_datetime,
    "excnodelist": NodeList.from_string,  # todo accept "(null)"
    "extra": noop,
    "features": noop,
    "gres": _NOT_IMPLEMENTED,
    "gres_idx": _NOT_IMPLEMENTED,
    "jobid": int,
    "licenses": _WRITE_ONLY,
    "mailtype": noop,
    "mailuser": noop,
    "mincpusnode": int,
    "minmemorycpu": _NOT_IMPLEMENTED,  # todo memory spec w/out "c/n"
    "minmemorynode": _NOT_IMPLEMENTED,  # todo memory spec w/out "c/n"
    "mintmpdisknode": memory_mbytes_to_bytes,
    "timemin": duration_timedelta,
    "jobname": noop,
    "name": _WRITE_ONLY,
    "nice": int,
    "numcpus": _NOT_IMPLEMENTED,  # todo int OR int range
    "numnodes": _NOT_IMPLEMENTED,  # todo int OR int range
    "numtasks": int,
    "oversubscribe": _NOT_IMPLEMENTED,  # todo lookup 18.09
    "partition": noop,
    "prefer": _WRITE_ONLY,
    "priority": int,
    "qos": noop,
    "reboot": int_bool,
    "reqcores": int,
    "reqnodelist": NodeList.from_string,
    "reqnodes": _NOT_IMPLEMENTED,  # todo int OR int range
    "reqprocs": int,
    "reqsockets": _WRITE_ONLY,
    "reqthreads": _WRITE_ONLY,
    "requeue": int_bool,
    "reservationname": noop,
    "resetaccruetime": _WRITE_ONLY,
    "sitefactor": int,
    "stderr": noop,  # todo pathlib
    "stdin": noop,  # todo pathlib
    "stdout": noop,  # todo pathlib
    "shared": _NOT_IMPLEMENTED,  # todo related to oversubscribe
    "starttime": timepoint_datetime,
    "switches": _NOT_IMPLEMENTED,  # todo complex
    "wait-for-switch": seconds_timedelta,
    "taskspernode": _WRITE_ONLY,
    "threadspec": int,
    "timelimit": duration_timedelta,
    "userid": str,
    "wckey": str,
    "workdir": noop,  # todo pathlib
}


from_scontrol_node_show = {
    "allocmem": memory_mbytes_to_bytes,
    "cpuload": type_cast_float_unsafe_to_none,
    "cpuspeclist": _NOT_IMPLEMENTED,  # todo what is the format?
    "freemem": memory_mbytes_to_bytes,
    "lastbusytime": timepoint_datetime,
    "memspeclimit": memory_mbytes_to_bytes,
    "realmemory": memory_mbytes_to_bytes,
    "state": noop,  # todo more intricate?
    "currentwatts": ns_int,
    "lowestjoules": ns_int,
    "consumedjoules": ns_int,
    "extsensorsjoules": ns_int,
    "extsensorswatts": ns_int,
    "extsensorstemp": ns_int,
}
from_scontrol_node_update = {
    "nodename": noop,
    "activefeatures": noop,
    "availablefeatures": noop,
    "comment": noop,
    "cpubind": noop,
    "extra": noop,
    "gres": GresSpec.from_string,
    "nodeaddr": _NOT_IMPLEMENTED,  # todo what is the format?
    "nodehostname": noop,
    "reason": noop,
    "resumeafter": seconds_timedelta,
    "state": noop,  # todo more intricate?
}


from_scontrol_partition_update = {
    "allocnodes": NodeList.from_string,  # TODO default is "ALL"
    "allowaccounts": comma_separated_list,  # TODO default is "ALL"
    "allowgroups": comma_separated_list,  # TODO defualt is "ALL"
    "allowqos": comma_separated_list,  # TODO default is "ALL"
    "alternate": noop,
    "cpubind": noop,
    "default": yes_no_bool,
    "defaulttime": duration_timedelta,
    "defmempercpu": mb_to_bytes,
    "defmempernode": mb_to_bytes,
    "denyaccounts": comma_separated_list,
    "denyqos": comma_separated_list,
    "disablerootjobs": yes_no_bool,
    "exclusiveuser": yes_no_bool,
    "gracetime": seconds_timedelta,
    "hidden": yes_no_bool,
    "jobdefaults": comma_separated_key_value_list,
    "maxcpuspernode": type_cast_int_unsafe_to_none,
    "lln": yes_no_bool,
    "maxmempercpu": mb_to_bytes,
    "maxmempernode": mb_to_bytes,
    "maxnodes": type_cast_int_unsafe_to_none,
    "maxtime": duration_timedelta,
    "minnodes": type_cast_int_unsafe_to_none,
    "maxcpuspersocket": type_cast_int_unsafe_to_none,
    "nodes": NodeList.from_string,
    "oversubscribe": noop,
    "overtimelimit": minutes_timedelta,
    "partitionname": noop,
    "powerdownonidle": noop,
    "preemptmode": noop,
    "priority": type_cast_int_unsafe_to_none,
    "priorityjobfactor": type_cast_int_unsafe_to_none,
    "prioritytier": type_cast_int_unsafe_to_none,
    "qos": noop,
    "reqresv": yes_no_bool,
    "rootonly": yes_no_bool,
    "shared": noop,
    "state": noop,  # todo more intricate?
    "tresbillingweights": _NOT_IMPLEMENTED,  # todo new class for billing weights?
}


from_sacctmgr_qos_show = {
    "description": noop,
    "gracetime": duration_timedelta,
    "grpjobs": type_cast_int_unsafe_to_none,
    "grpjobsaccrue": type_cast_int_unsafe_to_none,
    "grpsubmit": type_cast_int_unsafe_to_none,
    "grpsubmitjobs": type_cast_int_unsafe_to_none,
    "grptres": TresSpec.from_string,
    "grptresmins": TresSpec.from_string,  # todo generalize tres to hold differing values, will help with billing weights (float) and this (timedelta from minutes)
    "grpwall": duration_timedelta,
    "limitfactor": type_cast_float_unsafe_to_none,
    "maxjobsaccruepa": type_cast_int_unsafe_to_none,
    "maxjobsaccrueperaccount": type_cast_int_unsafe_to_none,
    "maxjobsaccruepu": type_cast_int_unsafe_to_none,
    "maxjobsaccrueperuser": type_cast_int_unsafe_to_none,
    "maxjobspa": type_cast_int_unsafe_to_none,
    "maxjobsperaccount": type_cast_int_unsafe_to_none,
    "maxjobspu": type_cast_int_unsafe_to_none,
    "maxjobsperuser": type_cast_int_unsafe_to_none,
    "maxtresmins": minutes_timedelta,
    "maxtres": TresSpec.from_string,
    "maxtresperjob": TresSpec.from_string,
    "maxtrespernode": TresSpec.from_string,
    "maxtrespu": TresSpec.from_string,
    "maxtresperuser": TresSpec.from_string,
    "maxsubmitjobspa": type_cast_int_unsafe_to_none,
    "maxsubmitjobsperaccount": type_cast_int_unsafe_to_none,
    "maxsubmitjobspu": type_cast_int_unsafe_to_none,
    "maxsubmitjobsperuser": type_cast_int_unsafe_to_none,
    "maxwall": duration_timedelta,
    "maxwalldurationperjob": duration_timedelta,
    "minpriothreshold": type_cast_int_unsafe_to_none,
    "mintres": TresSpec.from_string,
    "name": noop,
    "preempt": comma_separated_list,
    "preemptmode": noop,
    "priority": type_cast_int_unsafe_to_none,
    "usagefactor": type_cast_float_unsafe_to_none,
}


# TODO from_reservations
