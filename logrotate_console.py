
from subprocess                             import Popen            as sub_popen
from subprocess                             import PIPE             as sub_PIPE
import shlex
import time
import signal                                                       as SIGNAL
import os
import sys


# define global variables for simulator
global tty,verbose,end_test,config_file
verbose,end_test                            =   False,False
tty                                         =   ""
config_file                                 =   'logrotate_config'


def get_console_output():
    global tty
    if not tty:
        cmd = 'env ps -o tty,cmd -e | grep \'logrotate_console.py\' | grep -v grep | column -t | cut -d \' \' -f1;'
        p                                   =   sub_popen(cmd,stdout=sub_PIPE,shell=True)
        (_out, _err)                        =   p.communicate()
        tty                                 =   '/dev/%s' % _out.strip()
    return tty

def print_to_console(msg):
    tty                                     =   get_console_output(obj_type)

    cmd                                     =   'printf \'\n%s\n\n\' "%s" > %s 2>&1' % ('%s',msg,tty)
    p                                       =   sub_popen(cmd,stdout=sub_PIPE,shell=True)
    (_out, _err)                            =   p.communicate()
    assert _out                            ==   ''
    assert _err                            is   None
    return

def run_simulation(client_socket=None,run_cfgs=''):
    global end_test

    def signal_handler(signal,frame):
        global end_test
        print                                   '\nSimulation Ending...\n'
        end_test                            =   True

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
    print                                       'Press ctrl+c to end simulation\n'
    time.sleep(                                 1)

    list_dir_cmd                            =   shlex.split('bash -lc "ls -lAtr %(_dir)s | tail -n +2"' % T)
    lr_cmd                                  =   shlex.split('bash -lc " %(lr_path)s --state %(lr_status)s %(lr_cfg)s"' % T)

    while not end_test:
        with open(T['f_path'],"a+") as f:
            f.write(                            "TEST\n")
        p                                   =   sub_popen(list_dir_cmd,stdout=sub_PIPE)
        (_out,_err)                         =   p.communicate()
        print                                   '\n',_out.strip('\n'),'\n'
        p                                   =   sub_popen(lr_cmd,stdout=sub_PIPE)
        (_out,_err)                         =   p.communicate()

        time.sleep(                             1)

        if end_test:                            break

    end_test                                =   False
    return

def get_configs(client_socket=None,print_cfgs=False):
    if print_cfgs:                              print   '\nBelow are the current configurations.\n'
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
                    it                      =   it % T
                if print_cfgs:                  print it
                run_cfgs.append(                it)
            else:
                end                         =   True
                break
        if not start and it==top_line:
            start                           =   True
    if print_cfgs:                              print '\n\n'
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

        print                                   '\n'.join(msg)
        next_step                           =   raw_input("")

        if ['1','2','3'].count(next_step):
            if next_step=='1':                  next_step='read configs'
            elif next_step=='2':                next_step='run test'
            elif next_step=='3':                next_step='exit'
            break
        else:
            msg                             =   [
                                                "Sorry, your selection was not recognized.",
                                                "Please try again or quit (Ctrl-c).",
                                                ""
                                                ]
    return next_step

def main():
    while True:
        next_step                           =   iter_next_step() # returns one of ['read configs','run test','exit']

        if next_step=='read configs':
            run_cfgs                        =   get_configs(print_cfgs=True)

        elif next_step=='run test':
            run_cfgs                        =   get_configs(print_cfgs=False)
            run_simulation(                     run_cfgs)

        elif next_step=='exit':
            sys.exit(                           )

if __name__ == '__main__':
    print                                       ""
    main()