#! /usr/bin/env python

import os
import sys
from optparse import OptionParser
import pdb
import gzip

parser = OptionParser()
parser.add_option("-i", action="store", type="string", dest="input_directory")
parser.add_option("-o", action="store", type="string", dest="output_file")
parser.add_option("--master-event-min", action="store", type="int", dest="master_events_min", default=0)
parser.add_option("--event-min", action="store", type="int", dest="events_min", default=0)
parser.add_option("--master-event-max", action="store", type="int", dest="master_events_max", default=100)
parser.add_option("--event-max", action="store", type="int", dest="events_max", default=100)
parser.add_option("-t", action="store", type="int", dest="num_threads", default=4)
parser.add_option("--sync", action="store_true", dest="sync", default=False)
parser.add_option("--full_trace", action="store_true", dest="full_trace", default=False)
parser.add_option("--comm", action="store_true", dest="comm", default=False)
(options, args) = parser.parse_args()

def help():
    print "Usage: "+sys.argv[0]+" -o <output_file> -i <input_file>"
    sys.exit(0)

class Event:
    """Class of Sigil Events"""

    def __init__(self, event_type, event_id_range, 
            pthread_type, pthread_addr, iops, flops,
            mem_reads, mem_writes, comm_bytes):
        self.event_type = event_type
        self.event_id_range = event_id_range
        self.pthread_type = pthread_type
        self.pthread_addr = pthread_addr
        self.iops = iops
        self.flops = flops
        self.mem_reads = mem_reads
        self.mem_writes = mem_writes
        self.comm_bytes = comm_bytes

#Function to print nodes
def dump_node(dot_file, threadID, prevEvent, barrier_num):

    NodeTypeFormats = {
            'Comm'  : ', shape=diamond, color=green',
            'SysCall'  : ', shape=diamond, color=red',
            'Pthread' : ', shape=box, color=royalblue',
            'Comp'  : '',
    }

    PthreadTypeFormats = {
            1  : 'Mutex Lock',
            2  : 'Mutex Unlock',
            3  : 'Create',
            4  : 'Join',
            5  : 'Barrier',
    }

    # Dump the previous event to dot and switch
    if prevEvent.event_type == "Comp / Comm": # Option --sync
        # Combined dot type looks like:
        # ---------------------------
        # | Comp / Comm             |
        # | Iops, Flops Rs/Ws bytes |
        # ---------------------------

        dot_file.write('\t%s [ label=\"%s' %
                ("_" + str(threadID) + "_" + \
                    str(prevEvent.event_id_range[0]),
                    str(prevEvent.event_type)))
       # dot_file.write('\\n %s' %
       #         str(prevEvent.event_id_range[0]) + " " +
       #         str(prevEvent.event_id_range[1]))
        dot_file.write('\\n %s' %
                str(prevEvent.iops) + " " +
                str(prevEvent.flops) + " " +
                str(prevEvent.mem_reads) + " " +
                str(prevEvent.mem_writes) + " " +
                str(prevEvent.comm_bytes))
        dot_file.write('\" ];')
        dot_file.write('\n')

    if prevEvent.event_type == "Comp":

        # Comp dot type looks like:
        # ---------------------
        # | Comp              |
        # | Iops, Flops Rs/Ws |
        # ---------------------

        dot_file.write('\t%s [ label=\"%s' %
                ("_" + str(threadID) + "_" + \
                    str(prevEvent.event_id_range[0]),
                    str(prevEvent.event_type)))
       # dot_file.write('\\n %s' %
       #         str(prevEvent.event_id_range[0]) + " " +
       #         str(prevEvent.event_id_range[1]))
        dot_file.write('\\n %s' %
                str(prevEvent.iops) + " " +
                str(prevEvent.flops) + " " +
                str(prevEvent.mem_reads) + " " +
                str(prevEvent.mem_writes))
        dot_file.write('\" ];')
        dot_file.write('\n')

    elif prevEvent.event_type == "Comm":

        # Comm dot type looks like:
        # ---------------------
        # | Comm              |
        # | Bytes             |
        # ---------------------

        dot_file.write('\t%s [ label=\"%s' %
                ("_" + str(threadID) + "_" + \
                    str(prevEvent.event_id_range[0]),
                    str(prevEvent.event_type)))
        dot_file.write('\\n %s' %
                str(prevEvent.comm_bytes))
                #str(prevEvent.event_id_range[0]) + " " +
                #str(prevEvent.event_id_range[1]))

        try:
            dot_file.write('\" %s ];' %
                    NodeTypeFormats[prevEvent.event_type])
        except KeyError:
            print "Unsupported Event Type: %s" % prevEvent.event_type
            exit(-1)

        dot_file.write('\n')

    elif prevEvent.event_type == "SysCall":

        # Syscall dot type looks like:
        # ---------------------
        # | SysCall           |
        # | Bytes             |
        # ---------------------

        dot_file.write('\t%s [ label=\"%s' %
                ("_" + str(threadID) + "_" + \
                    str(prevEvent.event_id_range[0]),
                    str(prevEvent.event_type)))
        dot_file.write('\\n %s' %
                str(prevEvent.comm_bytes))
                #str(prevEvent.event_id_range[0]) + " " +
                #str(prevEvent.event_id_range[1]))

        try:
            dot_file.write('\" %s ];' %
                    NodeTypeFormats[prevEvent.event_type])
        except KeyError:
            print "Unsupported Event Type: %s" % prevEvent.event_type
            exit(-1)

        dot_file.write('\n')

    elif prevEvent.event_type == "Pthread":
        # Pthread dot type looks like:
        # ---------------------
        # | Pthread TYPE      |
        # | BAR ID or Mutex Lk|
        # ---------------------
        if prevEvent.pthread_type == 5: # Barrier
            dot_file.write('\t%s [ label=\"%s' %
                    ("barrier" + str(barrier_num),
                        str(
                        PthreadTypeFormats[int(prevEvent.pthread_type)])))
            dot_file.write('\\n %s' %
                    str(barrier_num))
            try:
                dot_file.write('\" %s ];' %
                        NodeTypeFormats[prevEvent.event_type])
            except KeyError:
                print "Unsupported Event Type: %s" % prevEvent.event_type
                exit(-1)

        elif (prevEvent.pthread_type == 1) or \
                (prevEvent.pthread_type == 2): # Mutex Lock
            dot_file.write('\t%s [ label=\"%s' %
                    ("_" + str(threadID) + "_" + \
                    str(prevEvent.event_id_range[0]),
                        str(PthreadTypeFormats[int(prevEvent.pthread_type)])))
            dot_file.write('\\n %s' %
                    str(prevEvent.pthread_addr))
            try:
                dot_file.write('\" %s ];' %
                        NodeTypeFormats[prevEvent.event_type])
            except KeyError:
                print "Unsupported Event Type: %s" % prevEvent.event_type
                exit(-1)

        elif (prevEvent.pthread_type == 3) or \
                (prevEvent.pthread_type == 4): # Create / Join

            dot_file.write('\t%s [ label=\"%s' %
                    ("_" + str(threadID) + "_" + \
                    str(prevEvent.event_id_range[0]),
                    str(prevEvent.event_type) + " " +
                        PthreadTypeFormats[int(prevEvent.pthread_type)]))

            dot_file.write('\\n')

            try:
                dot_file.write('\" %s ];' %
                        NodeTypeFormats[prevEvent.event_type])
            except KeyError:
                print "Unsupported Event Type: %s" % prevEvent.event_type
                exit(-1)

        dot_file.write('\n')

def dump_edge(dot_file, threadID, prevEvent, curEvent, pth_create_event, barrier_num, node_dep_id, events_min):

    #Take care of node 'dependencies'
    #Lots of weird corner cases

    #Slave thread's first node connects to a create from master
    if (prevEvent.event_id_range[0] == 0) and (threadID != 1):
        dot_file.write('\t%s -> %s [color=red];' %
                ("_1_" + str(pth_create_event[threadID-1]),
                "_" + str(threadID) + "_" +
                str(prevEvent.event_id_range[0])))
        dot_file.write('\n')

    if prevEvent.event_id_range[0] >= events_min:

        if int(curEvent.pthread_type) == 5:

            if node_dep_id != prevEvent.event_id_range[0]:
                dot_file.write('\t%s -> %s [color=black];' %
                    ("_" + str(threadID) + "_" + str(node_dep_id),
                        "_" +str(threadID) + "_" +
                        str(prevEvent.event_id_range[0])))

            dot_file.write('\n')
            dot_file.write('\t%s -> %s [color=black];' %
                ("_" +str(threadID) + \
                    "_" + str(prevEvent.event_id_range[0]),
                "barrier" + str(barrier_num)))

        elif int(prevEvent.pthread_type) == 5:
            dot_file.write('\t%s -> %s [color=black];' %
                ("barrier" + str(barrier_num),
                    "_" +str(threadID) + "_" + 
                    str(curEvent.event_id_range[0])))
            barrier_num += 1 #Finished barrier interface here

        else: #Normal case, no barrier
            if node_dep_id != prevEvent.event_id_range[0]:
                dot_file.write('\t%s -> %s [color=black];' %
                    ("_" +str(threadID) + "_" + str(node_dep_id),
                        "_" +str(threadID) + "_" +
                        str(prevEvent.event_id_range[0])))

        dot_file.write('\n')

def prod_consumer_comm(options,dot_file,pth_bar_event,master_thread_comp_comm):

    # Administrative work:
    input_dir = options.input_directory
    output_file = options.output_file
    num_threads = options.num_threads
    full_trace = options.full_trace
    # Set different event ranges for the master thread
    master_events_min = options.master_events_min
    master_events_max = options.master_events_max
    events_min = options.events_min
    events_max = options.events_max
    #Producer/Consumer Unordered Set:
    prod_cons_map = set()

    print "Starting P->C Edges"

    for threadID in xrange(1, num_threads+1):
        print "Starting Thread: " + str(threadID)
        input_file = input_dir + "/sigil.events.out-" + str(threadID) + ".gz"
        sigil_trace = gzip.open(input_file,'r')

        for lineNum, event in enumerate(sigil_trace, start=1):

            curEventNum = lineNum - 1

            if not full_trace:
                # Apply event ranges
                if threadID == 1:
                    if (curEventNum < master_events_min):
                        continue
                    elif curEventNum > master_events_max:
                        break
                else:
                    if (curEventNum < events_min):
                        continue
                    elif curEventNum > events_max:
                        break

            # Check if communication (#)
            if ('#' in event) and ( " 30001 " not in event):

                    num_reads = event.count('#')

                    # Can have combined reads in a comm event
                    for read in xrange(0,num_reads):
                        prodThreadID = int(event.split('#')[read+1].strip().split(' ')[0])
                        prodEventID = int(event.split('#')[read+1].strip().split(' ')[1])

                        #Get producer's combo event before barrier 0
                        if (prodThreadID == 1) and \
                                (prodEventID < pth_bar_event[prodThreadID-1][0][0]):

                            prod_combo_event = get_master_prod_combo_event( \
                                            prodEventID,master_thread_comp_comm)

                            #Get consumer's combo event
                            cons_combo_event = get_cons_combo_event(pth_bar_event,\
                                    threadID,event)
                            combo_str = str(prodThreadID) + "_" + \
                                    str(prod_combo_event) + \
                                    "_" + str(threadID) + "_" + \
                                    str(cons_combo_event)

                            # First P->C encounter of this type
                            if (combo_str not in prod_cons_map) \
                                    and (cons_combo_event is not "NULL") :

                                # Add prod/comm into set:
                                prod_cons_map.add(combo_str)
                                # Print P->C Relationship
                                #dot_file.write('\t%s -> %s [color=blue, style=dashed];' %
                                dot_file.write('\t%s -> %s [color=green];' %
                                        ("_" + str(prodThreadID) + "_" + \
                                                str(prod_combo_event),
                                        "_" + str(threadID) + "_" +
                                        str(cons_combo_event)))
                                dot_file.write('\n')
                            break

                        else:
                            # Search between which barriers that event falls between
                            # for the prodoucer thread:
                            for barrier in xrange(len(pth_bar_event[prodThreadID-1])-1):
                                #Link combo_event with producer before barrier event
                                if (barrier is not len(pth_bar_event[prodThreadID-1])-1) \
                                        and (pth_bar_event[prodThreadID-1][barrier][0] \
                                        < prodEventID < \
                                        pth_bar_event[prodThreadID-1][barrier+1][0]):

                                        #Get producer's combo event
                                        prod_combo_event = pth_bar_event[prodThreadID-1][barrier+1][1]
                                        #Get consumer's combo event
                                        cons_combo_event = get_cons_combo_event(pth_bar_event, threadID,event)
                                        combo_str = str(prodThreadID) + "_" + \
                                            str(prod_combo_event) + \
                                            "_" + str(threadID) + "_" + \
                                            str(cons_combo_event)

                                        # First P->C encounter of this type
                                        if (combo_str not in prod_cons_map) and \
                                                (cons_combo_event is not "NULL") :
                                            # Add prod/comm into set:
                                            prod_cons_map.add(combo_str)
                                            # Print P->C Relationship
                                            dot_file.write('\t%s -> %s [color=blue, style=dashed];' %
                                                    ("_" + str(prodThreadID) + "_" + \
                                                            str(prod_combo_event),
                                                    "_" + str(threadID) + "_" +
                                                    str(cons_combo_event)))
                                            dot_file.write('\n')
                                        break

def get_cons_combo_event(pth_bar_event,threadID,event):

    cons_event = int(event.split(',')[0])
    cons_combo_event = "NULL"
    if cons_event < pth_bar_event[threadID-1][0][0]:
        cons_combo_event = pth_bar_event[threadID-1][0][1]

    elif cons_event > pth_bar_event[threadID-1][len(pth_bar_event[threadID-1])-1][1]:
        pass
    else:
        for barrier in xrange(len(pth_bar_event[threadID-1])-1):

            if (barrier is not len(pth_bar_event[threadID-1])-1) \
                    and (pth_bar_event[threadID-1][barrier][0] \
                    < cons_event < pth_bar_event[threadID-1][barrier+1][0]):
                cons_combo_event = pth_bar_event[threadID-1][barrier+1][1]
                break

    return cons_combo_event

def get_master_prod_combo_event(prodEventID,master_thread_comp_comm):

    if prodEventID <= master_thread_comp_comm[0]:
        prod_combo_event = master_thread_comp_comm[0]

    elif prodEventID > master_thread_comp_comm[-1]:
        print master_thread_comp_comm[-1]
        prod_combo_event = master_thread_comp_comm[-1]
    else:
        for combo_event in xrange(len(master_thread_comp_comm)-1):

            if (combo_event is not len(master_thread_comp_comm)-1) \
                    and (master_thread_comp_comm[combo_event] \
                    < prodEventID < master_thread_comp_comm[combo_event+1]):
                prod_combo_event = master_thread_comp_comm[combo_event]
                break

    return prod_combo_event

def main():

    if ((len(sys.argv) > 15) or (len(sys.argv) == 1) ):
        help()

    # Administrative work:
    input_dir = options.input_directory
    output_file = options.output_file
    num_threads = options.num_threads
    sync = options.sync
    full_trace = options.full_trace
    comm = options.comm
    # Set different event ranges for the master and slave threads
    master_events_min = options.master_events_min
    master_events_max = options.master_events_max
    events_min = options.events_min
    events_max = options.events_max

    try:
        dot_file = open(output_file,'w')
        dot_file.write('digraph {\n')
    except IOError:
            print "Failed to open", output_file, " for writing"
            exit(-1)

    #Keep track of event numbers for create/join/barriers of threads
    pth_create_event = []
    pth_create_event.append("0") # Master thread NULL address
    pth_join_event = []
    pth_join_event.append("0") # Master thread NULL address
    pth_bar_event = [list([]) for _ in xrange(num_threads)]

    #List of comp/comm events prior to first barrier for Thread 0
    master_thread_comp_comm = []

    prevEvent = Event(event_type = "",
                event_id_range = [events_min,events_min],
                pthread_type = 0,
                pthread_addr = 0,
                iops = 0,
                flops = 0,
                mem_reads = 0,
                mem_writes = 0,
                comm_bytes = 0)

    for threadID in xrange(1, num_threads+1):

        #TODO Do a better implemenation of this:
        # Join slave threads to join event of master thread
        # At the start of thread loop because this is when
        # slave threads would have completed the post-processing
        #
        # 'thread 1 -> joins' are for slave threads, starting at
        # end of thread 2...etc
        if full_trace and ((threadID != 1) and (threadID !=2)):
            if pth_join_event != ["0"]:
                dot_file.write('\t%s -> %s [color=red];' %
                        ("_" + str(threadID) + "_" +
                        str(prevEvent.event_id_range[0]),
                        "_1_" + str(pth_join_event[threadID-2])))
                        # -2 bc thread 3 is the first thread to handle it,
                        # subtract thread 1->0, then subtract for previous thread
            dot_file.write('\n')

        print "Starting Thread: " + str(threadID)
        input_file = input_dir + "/sigil.events.out-" + str(threadID) + ".gz"
        sigil_trace = gzip.open(input_file,'r')

        prevEvent = Event(event_type = "",
                event_id_range = [events_min,events_min],
                pthread_type = 0,
                pthread_addr = 0,
                iops = 0,
                flops = 0,
                mem_reads = 0,
                mem_writes = 0,
                comm_bytes = 0)

        node_dep_id = events_min
        barrier_num = 1

        for lineNum, event in enumerate(sigil_trace, start=1):

            curEventNum = lineNum - 1

            if not full_trace:
                # Apply event ranges
                if threadID == 1:
                    if (curEventNum < master_events_min):
                        continue
                    elif curEventNum > master_events_max:
                        #Dump last event
                        dump_node(dot_file, threadID, prevEvent, barrier_num)
                        break
                else:
                    if (curEventNum < events_min):
                        continue
                    elif curEventNum > events_max:
                        #Dump last event
                        dump_node(dot_file, threadID, prevEvent, barrier_num)
                        break

            # First need to determine the type of event
            if '^' in event: # Check if pthread (^)
                # Pthread events:
                # 1) mutex lock
                # 2) mutex unlock
                # 3) create
                # 4) join
                # 5) barrier

                pthread_type_addr = event.split(": ")[1].split('^')

                # Log barrier event num and previous comp/comm event
                # so that we can add P->C links later
                if comm and (int(pthread_type_addr[0]) == 5):
                    pth_bar_event[threadID-1].append([curEventNum,prevEvent.event_id_range[0]])

                curEvent = Event(event_type = "Pthread",
                                 event_id_range = [curEventNum, curEventNum],
                                 pthread_type = int(pthread_type_addr[0]),
                                 pthread_addr = pthread_type_addr[1].strip(),
                                 iops = 0,
                                 flops = 0,
                                 mem_reads = 0,
                                 mem_writes = 0,
                                 comm_bytes = 0)

            elif '#' in event: # Check if communication (#)
                if " 30001 " in event: # System call mem transactions
                    num_reads = event.count('#')
                    total_read_bytes = 0

                    # Can have combined reads in a comm event
                    for read in xrange(0,num_reads):
                        read_bytes = \
                                int(event.split('#')[read+1].strip().split(' ')[3]) - \
                                   int(event.split('#')[read+1].strip().split(' ')[2])
                        total_read_bytes += read_bytes + 1
                        # Same address in each range, still get 1 byte
                        # Need to ask Sid about this... 1 byte from syscall?

                    curEvent = Event(event_type = "SysCall",
                                     event_id_range = [curEventNum, curEventNum],
                                     pthread_type = 0,
                                     pthread_addr = 0,
                                     iops = 0,
                                     flops = 0,
                                     mem_reads = 0,
                                     mem_writes = 0,
                                     comm_bytes = total_read_bytes)
                else:
                    num_reads = event.count('#')
                    total_read_bytes = 0

                    # Can have combined reads in a comm event
                    for read in xrange(0,num_reads):
                        read_bytes = \
                                int(event.split('#')[read+1].strip().split(' ')[3]) - \
                                   int(event.split('#')[read+1].strip().split(' ')[2])
                        total_read_bytes += read_bytes + 1

                    curEvent = Event(event_type = "Comm",
                                     event_id_range = [curEventNum, curEventNum],
                                     pthread_type = 0,
                                     pthread_addr = 0,
                                     iops = 0,
                                     flops = 0,
                                     mem_reads = 0,
                                     mem_writes = 0,
                                     comm_bytes = total_read_bytes)

            else: # Default: Computation
                comp_event_info = event.split(' ')[0].strip().split(',')
                curEvent = Event(event_type = "Comp",
                                 event_id_range = [curEventNum, curEventNum],
                                 pthread_type = 0,
                                 pthread_addr = 0,
                                 iops = int(comp_event_info[2]),
                                 flops = int(comp_event_info[3]),
                                 mem_reads = int(comp_event_info[4]),
                                 mem_writes = int(comp_event_info[5]),
                                 comm_bytes = 0)

            dump_event = False
            if not sync: # Combine events of the same type
                if prevEvent.event_type == curEvent.event_type:
                    if curEvent.event_type == "Comm":
                        prevEvent.event_id_range[1] = curEventNum
                        prevEvent.comm_bytes += curEvent.comm_bytes

                    elif curEvent.event_type == "Comp":
                        prevEvent.event_id_range[1] = curEventNum
                        prevEvent.iops += curEvent.iops
                        prevEvent.flops += curEvent.flops
                        prevEvent.mem_reads += curEvent.mem_reads
                        prevEvent.mem_writes += curEvent.mem_writes
                        prevEvent.comm_bytes += curEvent.comm_bytes
                else:
                    dump_event = True #Can't combine events, dump event!
            else: # Option to combine Comp, Comm, and SysCall
                if ((prevEvent.event_type != "Pthread") and \
                        (curEvent.event_type != "Pthread")):
                    prevEvent.event_type = "Comp / Comm"
                    prevEvent.event_id_range[1] = curEventNum
                    prevEvent.iops += curEvent.iops
                    prevEvent.flops += curEvent.flops
                    prevEvent.mem_reads += curEvent.mem_reads
                    prevEvent.mem_writes += curEvent.mem_writes
                    prevEvent.comm_bytes += curEvent.comm_bytes
                else: #If prevEvent or curEvent is pthreads, dump event!
                    dump_event = True 

            if dump_event :
                # Book Keeping for Thread 0, P->C
                if (prevEvent.event_type == "Comp / Comm") and \
                        comm and sync and (threadID == 1) and barrier_num is 1:
                    # Dump comp/comm events into a list
                        master_thread_comp_comm.append(prevEvent.event_id_range[0])

                # Dump Nodes
                dump_node(dot_file, threadID, prevEvent, barrier_num)

                # Book Keeping:
                if prevEvent.pthread_type == 3:
                    pth_create_event.append(prevEvent.event_id_range[0])
                elif prevEvent.pthread_type == 4:
                    pth_join_event.append(prevEvent.event_id_range[0])

                dump_edge(dot_file, threadID, prevEvent, curEvent, pth_create_event, barrier_num, node_dep_id, events_min) # Dump Edges

                # More Book Keeping (of prev nodes, for edges):
                # Corner case of first event of slave threads
                if (prevEvent.event_id_range[0] == 0) and (threadID != 1):
                    node_dep_id = prevEvent.event_id_range[0]
                elif prevEvent.pthread_type == 5:
                    barrier_num += 1
                    node_dep_id = curEvent.event_id_range[0]
                else:
                    node_dep_id = prevEvent.event_id_range[0]

                prevEvent = curEvent # Switch

                print barrier_num

    if comm and sync:
        prod_consumer_comm(options,dot_file,pth_bar_event,master_thread_comp_comm)

    dot_file.write('}\n')
    sigil_trace.close()
    dot_file.close()

main()
