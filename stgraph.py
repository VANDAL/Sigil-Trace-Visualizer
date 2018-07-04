"""Utilities for building DOT-format graphs from SynchroTraces (ST)

This module provides functionality for transforming workload data
from SynchroTraces into a graph with meta graph-nodes.
"""

from collections import namedtuple

NodeTypeFormats = {
    'Comm':    ', shape=diamond, color=green',
    'SysCall': ', shape=diamond, color=red',
    'Pthread': ', shape=box, color=royalblue',
    'Comp':    '',
}

sync_type = {
    1: 'LOCK',
    2: 'UNLOCK',
    3: 'CREATE',
    4: 'JOIN',
    5: 'BARRIER',
}

Sync = namedtuple('Sync', 'pth_ty pth_addr')
Comp = namedtuple('Comp', 'iops flops reads writes')
Comm = namedtuple('Comm', 'bytes')
SysCall = namedtuple('SysCall', 'bytes')


def is_sync(line):
    """Returns whether a line from a ST is a SYNC event"""
    return '^' in line


def is_comm(line):
    """Returns whether a line from a ST is a COMM event"""
    return '#' in line


def is_comp(line):
    """Returns whether a line from a ST is a COMP event"""
    return not (is_comm(line) or is_sync(line))


def create_sync(line):
    pth_ty, pth_addr = line.split(": ")[1].split('^')
    return Sync(pth_ty, pth_addr)


def create_comm(line):
    eid_tid, reads = line.split('#', 1)
    eid, tid = eid_tid.split(',')
    bytes_read = 0
    for read in reads.split('#'):
        eid_from, tid_from, bytes_from, bytes_to = \
            filter(None, read.split(' '))
        bytes_read += int(bytes_to) - int(bytes_from) + 1
    if eid == '30001':  # TODO ask ks499 what this is
        return SysCall(bytes_read)
    else:
        return Comm(bytes_read)


def create_comp(line):
    eid, tid, iops, flops, reads, writes = \
        line.split(' ')[0].strip().split(',')
    return Comp(iops, flops, reads, writes)


def stevent_from_line(line):
    if is_sync(line):
        return create_sync(line)
    elif is_comm(line):
        return create_comm(line)
    else:
        return create_comp(line)


class STNode:
    """A meta graph-node that can represent multiple ST events.

    This is a component for building a ST graph.
    An ST graph aggregates ST events (SYNC, COMM, COMP) into
    nodes, and graphs the nodes and edges between the nodes.
    """

    AggregateEvent = namedtuple('AggregateEvent', ['ev_tys',
                                                   'eid_range',
                                                   'pth_ty', 'pth_addr',
                                                   'iops', 'flops',
                                                   'reads', 'writes',
                                                   'comm_bytes'])

    def __init__(self):
        self.lastev = None
        self.aggev = STNode.AggregateEvent((), (0, 0),
                                           0, 0, 0, 0, 0, 0)

    def _merge(self, stevent):
        """Merge a ST Event into this node.

        The ST events are expected to be simple namedtuples.
        This prevents copying event data from being overly complex
        """
        if isinstance(stevent, Comp):
            pass
        elif isinstance(stevent, Comm):
            pass
        elif isinstance(stevent, Sync):
            pass
        else:
            raise('Cannot merge unknown event')
        self.lastev = type(stevent)

    def try_merge_all(self, stevent):
        """Merge any ST event (except SYNC events) into this node"""
        merged = False
        if not isinstance(stevent, Sync):
            self._merge(stevent)
            merged = True
        return merged

    def try_merge_same(self, stevent):
        """Merge a ST event into this node, only if it the same type
        as previously merged events"""
        merged = False
        if not isinstance(stevent, Sync):
            if self.lastev is None or isinstance(stevent, self.lastev):
                self._merge(stevent)
                merged = True
        return merged


class STGrapher:
    def __init__(self, gv, full_trace, sync, comm):
        self.gv = gv
        self.full_trace = full_trace
        self.sync = sync
        self.comm = comm

    def node_from_line(self, line):
        pass
