#!/usr/bin/python

import time
import common
import common2
import argparse
import socket


##############
# Globals

# Stores global configuration variables
config = {
    "epoch": 0,

    # List of expired leases
    "expired": [],
}

log = {}

# Stores all server leases
leases = []

# Store all locks
locks = []

###################
# RPC implementations

# this replays all of the missing logs and brings the log of the view back up to date
# cmd_list will be in the form {PROPOSAL_NUMBER: {ORIGINAL MESSAGE}, PROPOSAL_NUMBER + 1: {ORIGINAL MESSAGE}...}
def replay(cmd_dict, addr):
    for proposal_num in cmd_dict:
        if cmd_dict[proposal_num]["cmd"] == "lock_get":
            result = lock_get(cmd_dict[proposal_num], 'Nonwe')
            print("successfully replayed the lock_get command for log entry %s" % proposal_num)
            log[proposal_num] = cmd_dict[proposal_num]
        elif cmd_dict[proposal_num]["cmd"] == "lock_release":
            result = lock_release(cmd_dict[proposal_num], 'Non1e')
            print("successfully replayed the lock_release command for log entry %s" % proposal_num)
            log[proposal_num] = cmd_dict[proposal_num]
        elif cmd_dict[proposal_num]["cmd"] == "heartbeat":
            result = server_lease(cmd_dict[proposal_num], addr)
            print("successfully replayed the heartbeat for log entry %s" % proposal_num)
            log[proposal_num] = cmd_dict[proposal_num]

# this will send the prepare and accept messages to the replicas to keep the logs up to date before
# responding to the initial rpc from either the client or server
def sync(replica_list, log, msg):
    quorom = (len(replica_list) / 2) + 1
    replicas_that_responded = {}
    for replica in replica_list:
        if replica != config["endpoint"]:
            host = replica.partition(":")[0]
            port = replica.partition(":")[2]
            res = common.send_receive(host, port, {
                "cmd": "prepare", 
                "new_log_entry": msg, 
                "proposal_num": len(log)})
            if "error" in res:
                print("Couldn't connect to %s:%s for a prepare msg" % (host, port))
                continue
            else:
                replicas_that_responded[replica] = res
                if res["missing_logs_to_vl"] != None:
                    replay(res["missing_logs_to_vl"], None)
        else:
            # if you are contacting yourself, just vote yes.
            replicas_that_responded[config["endpoint"]] = {
            "vote": "yes", 
            "current_log_length": len(log), 
            "missing_logs_to_vl": None, 
            "missing_logs_to_replica": None}


    # this counts the "yes" votes and puts them in the list
    if len([1 for rep in replicas_that_responded if replicas_that_responded[rep]["vote"] == "yes"]) >= quorom:
        log[len(log)] = msg
        missing_logs = {}
        for replica in replicas_that_responded:
            if replica != config["endpoint"]:
                host = replica.partition(":")[0]
                port = replica.partition(":")[2]
                # the LAST value represents the replicas last log entry
                # i have to compare this value with the length of my log
                # it will be less most likely
                if replicas_that_responded[replica]["missing_logs_to_replica"] != None:
                    last = replicas_that_responded[replica]["missing_logs_to_replica"]
                    while last <= len(log):
                        missing_logs[last] = log[last] 
                        last = last + 1
                    #now missing_logs should hold all of the commands that the replica did not have yet. 
                res = common.send_receive(host, port, {
                    "cmd": "accept", 
                    "new_log_entry": msg, 
                    "proposal_num": len(log), 
                    "missing_logs_to_replica": missing_logs})
                print res
                if "error" in res:
                    print("couldn't connect to %s:%s for an accept msg" % (host, port))
                    continue
                else:
                    print("successfully sent new_entry to %s:%s" % (host, port))
    else:
        return {"error": "failed to sync due to lack of quorom"}
    return {"ok": "something went wrong"}

# Try to acquire a lock
def lock_get(msg, addr):
    lockid = msg["lockid"]
    requestor = msg["requestor"]
    for lock in locks:
        if lock["lockid"] == lockid:
            if len(lock["queue"]) == 0:
                lock["queue"].append(requestor)
                return {"status": "granted"}
            elif lock["queue"][0] == requestor:
                return {"status": "granted"}
            else:
                if requestor not in lock["queue"]:
                    lock["queue"].append(requestor)
                return {"status": "retry"}
    else:
        # this lock doesn't exist yet
        locks.append({"lockid":lockid, "queue": [requestor]})
        return {"status": "granted"}

# Release a held lock, or remove oneself from waiting queue
def lock_release(msg, addr):
    lockid = msg["lockid"]
    requestor = msg["requestor"]
    for lock in locks:
        if lock["lockid"] == lockid:
            if requestor in lock["queue"]:
                lock["queue"].remove(requestor)
                return {"status": "ok"}
    else:
        return {"status": "unknown"}

# Manage requests for a server lease
def server_lease(msg, addr):
    lockid = "%s:%s" % (addr, msg["port"])
    print "got heartbeat from %s" % lockid
    requestor = msg["requestor"]

    remove_expired_leases()

    if msg["requestor"] in config["expired"]:
        return {"status": "deny"}

    for lease in leases:
        if lease["lockid"] == lockid:
            # lease is present

            if time.time() - lease["timestamp"] > common2.LOCK_LEASE:
                # lease expired
                if lease["requestor"] == requestor:
                    # server lost lease, then recovered, but we deny it
                    return {"status": "deny"}
                else:
                    # another server at same address is okay
                    lease["timestamp"] = time.time()
                    lease["requestor"] = requestor
                    config["epoch"] += 1
                    return {"status": "ok", "epoch": config["epoch"]}
            else:
                # lease still active
                if lease["requestor"] == requestor:
                    # refreshing ownership
                    lease["timestamp"] = msg["timestamp"]
                    return {"status": "ok", "epoch": config["epoch"]}
                else:
                    # locked by someone else
                    return {"status": "retry", "epoch": config["epoch"]}
    else:
        # lock not present yet
        leases.append({"lockid": lockid, "requestor": requestor, "timestamp": msg["timestamp"]})
        config["epoch"] += 1
        return {"status": "ok", "epoch": config["epoch"]}

# Check which leases have already expired
def remove_expired_leases():
    global leases
    expired = False
    new_leases = []
    for lease in leases:
        if time.time() - lease['timestamp'] <= common2.LOCK_LEASE:
            new_leases.append(lease)
        else:
            config["expired"].append(lease["requestor"])
            expired = True
    if expired:
        config["epoch"] += 1 
    leases = new_leases

# Output the set of currently active servers
def query_servers(msg, addr):
    servers = []
    remove_expired_leases()
    for lease in leases:
        ip = lease["lockid"]
        servers.append(ip)

    return {"result": servers, "epoch": config["epoch"]}

# Resolving an incoming prepare message in order to achieve consensus
# Considers all of the cases before voting
def prepare(msg, addr):
    global log
    proposal_num = msg["proposal_num"]
    if proposal_num == len(log):
        return {
        "vote": "yes",
        "current_log_length": len(log), 
        "missing_logs_to_vl": None, 
        "missing_logs_to_replica": None}
    elif proposal_num < len(log):
        missing_logs = {}
        while proposal_num < len(log):
            # should be <= because you want to add the last most log
            missing_logs[proposal_num] = log[proposal_num]
            proposal_num = proposal_num + 1
        return {
        "vote": "yes", 
        "current_log_length": len(log), 
        "missing_logs_to_vl": missing_logs, 
        "missing_logs_to_replica": None}
    elif proposal_num > len(log):
        return {"vote": "yes", 
        "current_log_length": len(log), 
        "missing_logs_to_vl": None, 
        "missing_logs_to_replica": len(log)}
    log[msg["proposal_num"]] = "promised"

#Check to see if they have not yet received a promise for a higher number. This might require
#that everytime you promise, you add that promise to the log in the form of proposal_num: promise
#and then whenever you actually replay those messages, you just update it to proposal_num: {"cmd": lock_get...}
def accept(msg, addr):
    if len(log) <= msg["proposal_num"]:
        if msg["missing_logs_to_replica"] != {}:
            replay(msg["missing_logs_to_replica"], addr)
        x = {}
        x[len(log)] = msg["new_log_entry"]
        replay(x, addr)
        return {"status": "Successful Accept"}
    return {"error": "Accepting error"}


def init(msg, addr):
    return {}

##############
# Main program

# RPC dispatcher invokes appropriate function
def handler(msg, addr):

    cmds = {
        "init": init,
        "heartbeat": server_lease,
        "query_servers": query_servers,
        "lock_get": lock_get,
        "lock_release": lock_release,
        "prepare": prepare,
        "accept": accept,
    }

    # this checks the rpc commands to see if they require the logs to be sync'd
    if msg["cmd"] == "lock_get" or msg["cmd"] == "lock_release" or msg["cmd"] == "heartbeat": 
        # global log
        # print log 
        msg["timestamp"] = time.time()
        x = sync(config["views"], log, msg)
        if "error" in x:
            return {"error": "sorry"}

    return cmds[msg["cmd"]](msg, addr)

# Server entry point
def main():
    hostname = socket.gethostname()
    defaultval = str(hostname) + ":" + str(39000) + "," + str(hostname) + ":" + str(39001) + "," + str(hostname) + ":" + str(39002)

    parser = argparse.ArgumentParser()
    parser.add_argument('--viewleader', default=defaultval)
    args = parser.parse_args()

    ####### Arranges the list of view replicas in the appropriate manner ###########
    view_replicas_list = sorted(args.viewleader.split(','))
    config["views"] = view_replicas_list[::-1]


    for port in range(common2.VIEWLEADER_LOW, common2.VIEWLEADER_HIGH):
        print "Trying to listen on %s..." % port
        host = socket.gethostname()
        endpoint = str(host) + ":" + str(port)
        # this is so that the view can refer to itself once it has started.
        if endpoint in args.viewleader:
            config["endpoint"] = endpoint
            result = common.listen(port, handler)
            print result
        else:
            print "Sorry, but this address %s wasn't on the supported view replicas list" % endpoint
            break
    print "Can't listen on any port, giving up"

if __name__ == "__main__":
    main()