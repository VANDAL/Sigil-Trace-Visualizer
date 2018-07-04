#!/usr/bin/env python

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from os import path
import pygraphviz as pgv
from stgraph import STNode, STGrapher, stevent_from_line
import gzip
import itertools


def graph_trace(stg, trace,
                full_trace, start, end,
                merge_all, prod_cons):
    """
    Iterate over a trace of a thread for a workload,
    and generate aggregate event nodes.
    Only events between start and end are processed.
    """

    node = STNode()
    e = enumerate(trace)
    itertools.dropwhile(lambda idx_l: idx_l[0] < start, e)
    for _, line in itertools.takewhile(lambda idx_l: idx_l[0] < end, e):
        # the synchrotrace event
        ev = stevent_from_line(line)

        # record additional data if graphing producer-consumer relationships
        if prod_cons:
            pass

        # merge events into graph nodes
        merged = node.try_merge_all(ev) if merge_all \
            else node.try_merge_same(ev)

        # output the node and start a new one
        if not merged:
            stg.graph(node, ev)
            node = STNode()

    # graph the final node
    stg.graph(node, ev)


def graph_aux_thread_full(stg, trace):
    """
    Iterate over a trace of an auxiliary thread for a workload,
    and generate aggregate event nodes.
    Periodically the event nodes are flushed to a graph when
    they cannot be combined with consecutive events.
    """
    node = STNode()
    for line in trace:
        ev = stevent_from_line(line)
        merged = STNode.try_merge(current, previous)
        if merged:
            previous = merged
        else:
            stg.graph(current, previous)
            previous = current


def trace_path(trace_dir, tid):
    return path.join(trace_dir, 'sigil.events.out-{}.gz'.format(tid))


def graph_synchrotraces(stg, trace_dir,
                        threads, range_main, range_aux):
    """
    Generate a DOT representation of a SynchroTraceGen event trace.
    """
    for tid in range(1, threads+1):
        path = trace_path(trace_dir, tid)
        with gzip.open(path, 'r') as trace:
            if tid == 1:
                graph_main_thread(stg, trace, range_main)
            else:
                graph_aux_thread(stg, trace, range_aux)


def parse_args():
    parser = ArgumentParser(description='Graph generation for SynchroTraces'
                            ' (ST)',
                            formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('-i', required=True, dest='input',
                        help='directory of ST traces')
    parser.add_argument('-o', required=True, dest='output',
                        help='DOT output file')
    parser.add_argument('--main-event-start', type=int, default=0,
                        help='ST events BEFORE this index will be ignored'
                        ' (for the main thread)')
    parser.add_argument('--main-event-end', type=int, default=100,
                        help='ST events AFTER this index will be ignored'
                        ' (for the main thread)')
    parser.add_argument('--aux-event-start', type=int, default=0,
                        help='ST events BEFORE this index will be ignored'
                        ' (for other threads)')
    parser.add_argument('--aux-event-end', type=int, default=100,
                        help='ST events AFTER this index will be ignored'
                        ' (for other threads)')
    parser.add_argument('-f', '--full-trace', action='store_true',
                        default=False,
                        help='setting this to true ignores event-{min,max}'
                        ' values')
    parser.add_argument('-m', '--merge-all', action="store_true",
                        default=False,
                        help='merge all ST events except SYNC into graph'
                        ' nodes')
    parser.add_argument('-p', '--prod-cons', action='store_true',
                        default=False,
                        help='graph producer-consumer relationships between'
                        ' threads from COMM events')
    parser.add_argument('-t', '--threads', type=int, default=4,
                        help='the number of threads to expect in the workload')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    graph_synchrotraces(STGrapher(pgv.AGraph(args.output),
                                  args.full_trace,
                                  args.merge_all,
                                  args.prod_cons),
                        args.input,
                        args.threads,
                        (args.main_event_start, args.main_event_end),
                        (args.aux_event_start, args.aux_event_end))
