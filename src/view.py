"""
sstatus <cmd>

general flags:
-f --format {ascii,csv,markdown,mediawiki}

# DEFINITIONS

Job States

BF,BOOT_FAIL
CA,CANCELLED
CD,COMPLETED
DL,DEADLINE
F,FAILED
NF,NODE_FAIL
OOM,OUT_OF_MEMORY
PD,PENDING
PR,PREEMPTED
R,RUNNING
RQ,REQUEUED
RS,RESIZING
RV,REVOKED
S,SUSPENDED
TO,TIMEOUT

Running states: (R)
Alive states: (<running>,<pending>)

Pending states: (PD,RQ,RS,S)
Failed states: (BF,DL,F,NF,OOM,PR,RV,TO)
Terminated states: (CA,<failed>)
Dead states: (<pending>,<failed>,<terminated>)


Node States

v<21
NoResp
ALLOC
ALLOCATED
COMPLETING
DOWN
DRAIN
FAIL
FAILING
FUTURE
IDLE
MAINT
MIXED
PERFCTRS/NPC
RESERVED
POWER_DOWN
POWER_UP
RESUME
UNDRAIN

Reserved states: (DRAIN,RESERVED,UNDRAIN)
Available states: (ALLOC,ALLOCATED,COMPLETING,IDLE,MIXED)
Alive states: (<available>,<reserved>)
Terminated states: (NoResp,DOWN,FAIL,FAILING,FUTURE,MAINT,PERFCTRS/NPC,POWER_DOWN,POWER_UP)


v>=21
ALLOCATED
DOWN
ERROR
FUTURE
IDLE
MIXED
UNKNOWN
w/flags: CLOUD,COMPLETING,DRAIN,DYNAMIC,INVALID_REG,MAINTENANCE,NOT_RESPONDING,PERFCTRS,POWER_DOWN,POWERED_DOWN,POWERING_DOWN,POWERING_UP,PLANNED,REBOOT_ISSUED,REBOOT_REQUESTED,RESERVED
<state>+<flag_1>+...+<flag_n>

Alive states: (ALLOCATED,MIXED)
Terminated states: (DOWN,ERROR,FUTURE,UNKNOWN)


User State Hierarchy
- exist
    - active
        - alloc(ated)
        - pending
    - inactive

Job State Hierarchy
- active
    - running
    - pending
- stuck (deadlocked, blocked, stalled, inhibited, arrested, trapped)
- inactive
    - complete
    - failed

Resource State Hierarchy
- exist
    - up
        - alloc(ated)
        - idle
    - down
        - maintenance
        - failure
- future

Node Types (ideally defined in AvailableFeatures in slurm.conf, but we can
provide an alternative definition for now)
    - DataTransfer
    - LargeMemory
    - CpuCompute
    - GpuCompute



Stateful Entities
- users
- jobs
- Resources
    - nodes
    - components={core,memory,gpu={names...}}

Accounts
- queue
- qos (only exists in relation to partitions)
- partitions

# THINGS WE WISH TO KNOW

What is the live state of the cluster?
- overall
    - active users: count(users.active)
    - active jobs: count(jobs.active)

    - exist nodes: count(nodes.exist)
    - allocated nodes: count(nodes.allocated)
    - idle nodes: count(nodes.idle)
    - down nodes: count(nodes.down)

    - exist components: sum(components.exist)
    - allocated components: sum(components.allocated)
    - occupancy components: allocated / exist
- queue
    - pending users: count(users.pending)
    - pending jobs: count(jobs.pending)
    - pending resources: sum(jobs.pending.resources)

What is the state of each node?
- for each node...
    - show `$id: 0000j   000% [##  ]c_000% [##  ]m_000% [# ]g_STATE [0]`
    - j=job
    - c=core one space per four cores
    - m=memory one space per 64GB
    - g=gpu one space per gpu
- overall...
    - reasons list
- sort by: id, j, c, m, g (ascend, descend)
- Use unicode for blocks to fill meters
    - 1/8 "U+258F"
    - 2/8 "U+258E"
    - 3/8 "U+258D"
    - 4/8 "U+258C"
    - 5/8 "U+258B"
    - 6/8 "U+258A"
    - 7/8 "U+2589"
    - 8/8 "U+2588"
- colors are possible also

What job ids are zombies?


- Summary of allocated resources
    - jobs.sum(alloc):components
    - nodes.sum(all):components
    - jobs.groupby(users).count(alloc)
    - jobs.count(alloc)
    - nodes.count(all)
    - nodes.count(idle)
    - nodes.count(dead)
- Summary of queued resources
    - jobs.sum(queue):cpu,memory,gpu
    - jobs.sum(avail)-jobs.sum(alloc):cpu,memory,gpu
    - jobs.sum(all):users
    - jobs.count(all)
- Summary of users
    - jobs.groupby(user)
        - count(queue):job
        - sum(alloc):cpu,memory,gpu
        - sum(queue):cpu,memory,gpu
- Summary of partitions
    - nodes.groupby(partition)
        - sum(alloc):cpu,memory,gpu
        - sum(avail):cpu,memory,gpu







# USERS

user $id
user job $id $days-ago

# JOBS

job $id

job past
job past -n $days-ago
job past $user
job past $user -n $days-ago

# NODES

node state
node state $id
node state -t gpu
node state -t cpu
node state -t largemem

node summary

node config
node config $id

partition state
partition state $id

partition config
partition config $id
"""
