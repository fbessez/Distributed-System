#!/usr/bin/python


import argparse
import common
import common2
import random
import time
import socket


# Client entry point
def main():
    hostname = socket.gethostname()
    defaultval = str(hostname) + ":" + str(39000) + "," + str(hostname) + ":" + str(39001) + "," + str(hostname) + ":" + str(39002)

    parser = argparse.ArgumentParser()
    parser.add_argument('--server', default='localhost')
    parser.add_argument('--viewleader', default=defaultval)

    subparsers = parser.add_subparsers(dest='cmd')

    parser_set = subparsers.add_parser('set')
    parser_set.add_argument('key', type=str)
    parser_set.add_argument('val', type=str)

    parser_get = subparsers.add_parser('get')
    parser_get.add_argument('key', type=str)

    parser_print = subparsers.add_parser('print')
    parser_print.add_argument('text', nargs="*")

    parser_query = subparsers.add_parser('query_all_keys')
    parser_server_query = subparsers.add_parser('query_servers')

    parser_lock_get = subparsers.add_parser('lock_get')
    parser_lock_get.add_argument('lockid', type=str)    
    parser_lock_get.add_argument('requestor', type=str)    

    parser_lock_get = subparsers.add_parser('lock_release')
    parser_lock_get.add_argument('lockid', type=str)    
    parser_lock_get.add_argument('requestor', type=str)    

    args = parser.parse_args()
    print(args)

    ####### Arranges the list of view replicas in the appropriate manner ###########
    view_replicas_list = sorted(args.viewleader.split(','))
    reversed_view_replicas_list = view_replicas_list[::-1]


    if args.cmd in ['query_servers', 'lock_get', 'lock_release']:
        while True:
            response = common.send_receive_list(reversed_view_replicas_list, vars(args))
            if response.get("status") == "retry":
                print "Waiting on lock %s..." % args.lockid
                time.sleep(5)
                continue
            else:
                break
        print response
    else:
        response = common.send_receive_range(args.server, common2.SERVER_LOW, common2.SERVER_HIGH, vars(args))
        print response

if __name__ == "__main__":
    main()    
