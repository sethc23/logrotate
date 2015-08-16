
from ipdb import set_trace as i_trace

from subprocess                             import Popen            as sub_popen
from subprocess                             import PIPE             as sub_PIPE
import shlex
import time
import signal                                                       as SIGNAL
import socket
import threading
import os


# define global variables for simulator
global server,client,listen,port,target,tty,verbose,end_test,config_file
server,client,listen,verbose,end_test       =   False,False,False,False,False
port                                        =   0
target                                      =   ""
tty                                         =   ""
config_file                                 =   'logrotate_config'


def get_console_output(obj_type='client'):
    global tty
    if not tty:
        cmd = 'env ps -o tty,cmd -e | grep \'logrotate_console.py %s\' | grep -v grep | column -t | cut -d \' \' -f1;' % obj_type
        p                                   =   sub_popen(cmd,stdout=sub_PIPE,shell=True)
        (_out, _err)                        =   p.communicate()
        tty                                 =   '/dev/%s' % _out.strip()
    return tty

def print_to_console(msg):
    obj_type                                =   'server' if server else 'client'
    tty                                     =   get_console_output(obj_type)

    cmd                                     =   'printf \'\n%s: %s\n\n\' "%s" > %s 2>&1' % (prefix,'%s',msg,tty)
    p                                       =   sub_popen(cmd,stdout=sub_PIPE,shell=True)
    (_out, _err)                            =   p.communicate()
    assert _out                            ==   ''
    assert _err                            is   None
    return

def run_simulation(client_socket=None,run_cfgs=''):
    global end_test

    def print_it(*msg):
        msgs                                =   msg if [list,tuple].count(type(msg)) else [msg]
        msg                                 =   ''.join(msgs)

        print_to_console(                       msg)
        # if client_socket:                       client_socket.send(msg+'<return>')
        # else:                                   print msg
    def signal_handler(signal,frame):
        global end_test
        print                                   '\nSimulation Ending...\n'
        end_test                            =   True

    if not client_socket:
        SIGNAL.signal(SIGNAL.SIGINT,signal_handler)


    if not run_cfgs:
        run_cfgs                            =   get_configs(client_socket=None,print_cfgs=False)
    run_dir                                 =   os.environ['PWD'] + '/runtime'
    cmds                                    =   [
                                                'which logrotate;',
                                                'env ps -ef -o tty,comm= | grep test_logrotate | column -t | cut -d \' \' -f1;',
                                                'mkdir -p %s;' % run_dir,
                                                ]

    p                                       =   sub_popen(' '.join(cmds),stdout=sub_PIPE,shell=True)
    (_out, _err)                            =   p.communicate()
    assert _out
    assert _err                            is   None

    _out                                    =   _out.split('\n')
    T                                       =   {'_dir'             :   os.environ['PWD'] + '/mock_log_dir',
                                                 'f_path'           :   os.environ['PWD'] + '/mock_log_dir/transactions.log',
                                                 'lr_path'          :   _out[0],
                                                 'tty'              :   '/dev/%s' % _out[1],
                                                 'lr_cfg'           :   '%s/log_r_config' % run_dir,
                                                 'lr_status'        :   '%s/log_r_status' % run_dir,
                                                 'cfg_insert'       :   ''.join(['\t%s\n' % it for it in run_cfgs]),
                                                 }

    runtime_cfg                             =   [
                                                '"%(_dir)s/*.log" {' % T,
                                                T['cfg_insert'],
                                                '}'
                                                ]

    with open(T['lr_cfg'],'w') as f:
        f.write(                                '\n'.join(runtime_cfg))

    logrotate_prep="""
        #!/bin/bash

        mkdir -p %(_dir)s
        rm -f %(_dir)s/*
        :> %(f_path)s

        """ % T

    ## Setup Simulation
    args                                    =   shlex.split('bash -lc \'%s\'' % logrotate_prep)
    lp                                      =   sub_popen(args)
    print_it(                                   'Press ctrl+c to end simulation\n')
    time.sleep(                                 1)

    list_dir_cmd                            =   shlex.split('bash -lc "ls -lAtr %(_dir)s | tail -n +2"' % T)
    lr_cmd                                  =   shlex.split('bash -lc " %(lr_path)s --state %(lr_status)s %(lr_cfg)s"' % T)

    while not end_test:
        with open(T['f_path'],"a+") as f:
            f.write(                            "TEST\n")
        p                                   =   sub_popen(list_dir_cmd,stdout=sub_PIPE)
        (_out,_err)                         =   p.communicate()
        print_it(                               '\n',_out.strip('\n'),'\n')
        p                                   =   sub_popen(lr_cmd,stdout=sub_PIPE)
        (_out,_err)                         =   p.communicate()

        time.sleep(                             1)

        if end_test:
            break

    end_test                                =   False
    return

def get_configs(client_socket=None,print_cfgs=False):
    if print_cfgs:
        msg                                 =   '\nBelow are the current configurations.\n'
        if client_socket:                       client_socket.send(msg+'<return>')
        else:                                   print msg
    with open(config_file,'r') as f:
        all_cfgs                            =   f.readlines()
    all_cfgs                                =   [it[:-1] for it in all_cfgs]
    run_cfgs                                =   []
    top_line                                =   '## -------- TOP -----------'
    bot_line                                =   '## -------- BOTTOM --------'
    T                                       =   {'tty'          :   get_console_output()}
    start,end                               =   False,False
    for it in all_cfgs:
        if start and not end:
            it                              =   it.strip('\t')
            if it != bot_line:
                if it.count('%(tty)s'):
                    it                      =   it%T
                if print_cfgs:
                    if client_socket:           client_socket.send(it+'<return>')
                    else:                       print it
                run_cfgs.append(                it)
            else:
                end                         =   True
                break
        if not start and it==top_line:
            start                           =   True
    if print_cfgs:
        msg                                 =   '\n\n'
        if client_socket:                       client_socket.send(msg+'<return>')
        else:                                   print msg
    return                                      run_cfgs

def iter_next_step(client_socket=None):
    msg                                     =   [
                                                "Please type a number for one of the following options and hit return:",
                                                "(1) Read and show configurations from file: %s" % config_file,
                                                "(2) Run test",
                                                "(3) Exit",
                                                ""
                                                ]
    while True:

        if client_socket:
            client_socket.send(                 '\n'.join(msg))
            next_step                       =   recv_client_socket(client_socket)
        else:
            print                               '\n'.join(msg)
            next_step                       =   raw_input("")

        if ['1','2','3'].count(next_step):
            if next_step=='1':              next_step='read configs'
            elif next_step=='2':            next_step='run test'
            elif next_step=='3':            next_step='exit'
            break
        else:
            msg                             =   [
                                                "Sorry, your selection was not recognized.",
                                                "Please try again or quit (Ctrl-c).",
                                                ""
                                                ]
    return next_step

def client_sender(buffer):
    global target,port,verbose

    client                                  =   socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        # connect to our target host
        client.connect(                         (target,int(port)))
        if len(buffer):                         client.send(buffer)
        while True:

            # now wait for data back and print
            recv_len                        =   1
            response                        =   ""
            while recv_len:

                data                        =   client.recv(4096)
                recv_len                    =   len(data)
                response                   +=   data
                if recv_len < 4096:             break

            if response.find('<return>')==len(response)-len('<return>'):
                print                           response[:-len('<return>')]
                client.send(                    '\n')

            else:
                print response

                # wait for more input
                buffer                      =   raw_input("")
                buffer                     +=   "\n"

                # send it off
                client.send(                    buffer)

    except:
        # tear down the connection
        client.close(                           )

def recv_client_socket(client_socket):
    cmd_buffer                              =   ""
    try:
        while "\n" not in cmd_buffer:
            cmd_buffer                     +=   client_socket.recv(1024)
    except Exception as e:
        print "ERROR:"
        print type(e)                       # the exception instance
        print e.args                        # arguments stored in .args
        print e                             # __str__ allows args to be printed directly
    return cmd_buffer.strip('\n')

def client_handler(client_socket):

    while True:

        next_step                           =   iter_next_step(client_socket) # returns one of ['read configs','run test','exit']

        if next_step=='read configs':
            run_cfgs                        =   get_configs(client_socket,print_cfgs=True)

        elif next_step=='run test':
            run_cfgs                        =   get_configs(client_socket,print_cfgs=False)
            worker_pid = os.fork()
            while not worker_pid:
                worker_pid                  =   os.fork()
            run_simulation(                     client_socket,run_cfgs)


        elif next_step=='exit':
            # client_socket.send(                 'exit\n')
            this_pid                        =   os.getpid()
            os.kill(                            this_pid, signal.SIGKILL)

def signal_handler(signal,frame):
    pid = os.getpid()
    print 'Received %s in process %s' % (signal,pid)
    server.close(                           )
    this_pid                            =   os.getpid()
    os.kill(                                this_pid, SIGNAL.SIGKILL)

SIGNAL.signal(                              SIGNAL.SIGINT,signal_handler)

def server_loop():
    global target,port,verbose

    server                                  =   socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(                                ('',int(port)))
    target,port                             =   server.getsockname()
    server.listen(                              5)
    if verbose:                                 print "[*] Listening on %s:%d"          %   (target,port)

    while True:
        client_socket, addr                 =   server.accept()
        if verbose:                             print "[*] Accepted connection from: %s:%d" % (addr[0],addr[1])

        # spin off a thread to handle our new client
        client_thread                       =   threading.Thread(target=client_handler, args=(client_socket,))
        client_thread.start(                    )


from sys                                    import argv
if __name__ == '__main__':

    print                                   ""

    if argv[1:] and argv[1]=='server':
        server                              =   True
        listen                              =   True
        verbose                             =   True
        if len(argv)==3:
            port                            =   argv[2]
        server_loop(                            )

    elif argv[1:] and argv[1]=='client':
        client                              =   True
        listen                              =   True
        port                                =   argv[2]
        client_sender(                          '')