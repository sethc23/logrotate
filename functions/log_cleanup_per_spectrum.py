"""
    Step One -- Ideal Logging Days (Projected Forward)

    QUESTION:
        If record logging were to start on the first day of the year,
        what logs should be kept?

    ANSWER:
        Those logs falling on the days identified in the graph below,
        or (if no log falling on a day below) the earliest log before the next day.

    RESULT --> Ideal Log Spectrum (ILS)


    Step Two -- Identify What Logs to Delete (Projecting ILS Backward)

    QUESTION:
        Projecting ILS from today to one year ago,
        what logs in .pg_dump should be deleted?

    ANSWER:
        Those logs falling on the days identified in the graph below,
        or (if no log falling on a day below) the most recent log before the next older day.

"""

import                                          datetime            as dt
from subprocess                             import Popen            as sub_popen
from subprocess                             import PIPE             as sub_PIPE
from re                                     import findall          as re_findall
import                                          pandas              as pd
from os                                     import environ as os_environ
from sys                                    import path as py_path
py_path.append(                                 os_environ['HOME']+'/.scripts')
from System_Control                         import System_Reporter
SYS_R                                       =   System_Reporter()


def make_ideal_log_spectrum(N=None):
    if not N:
        N = dt.date.today() + dt.timedelta(days=+1)
    deltas = []
    for m in range(1,13):
        for w in range(1,5):
            for d in range(1,8):
                deltas.append(dt.timedelta(days=-1*m*w*d))

    x = sorted([N+delta for delta in deltas])

    # Remove any overlapping days
    df = pd.DataFrame(data={'dates':x})
    ndf = pd.DataFrame(data={'dates':df.dates.unique()}).sort('dates',ascending=False)
    ndf['dayofyear'] = ndf.dates.map(lambda d: int(d.strftime('%j')))

    # Map Ideal Log Days ("ILDs") to days of the year
    D = ndf.dayofyear.tolist()
    df = pd.DataFrame(data={'days':range(1,366)})
    df['log_days'] = df.days.map(lambda d: 0 if not D.count(d) else 1)
    df = df.sort('days',ascending=False)
    idx = df[df.log_days!=0].index.tolist()
    df['log_dates'] = None
    assert len(idx)==len(ndf)
    df.ix[idx,'log_dates'] = ndf.dates.tolist()
    return df

def make_demo_graph(backward=True):
    _dir = -1 if backward else 1
    N = dt.date(2015,1,1) + dt.timedelta(days=_dir)
    df = make_ideal_log_spectrum(N)

    fig = df.plot(x='days',y='log_days',kind='bar',figsize=(107,8)).get_figure()
    fig.savefig('/Volumes/mbp2/Users/admin/Desktop/plot.png')
    return df

def check_db_for_action():

    cmd =   """ select * from system_log
                where operation='rotate_pgdump'
                and stout='go';
            """
    df = pd.read_sql(                           cmd,SYS_R.T.sys_eng)
    if len(df):
        # from ipdb import set_trace as i_trace; i_trace()


        for idx,row in df.iterrows():
            logs                            =   row.parameters.split(',')
            cmds                            =   []
            for f in logs:
                cmds.append(                    'rm %s;' % f.strip())

            p                               =   sub_popen(cmds,stdout=sub_PIPE,shell=True)
            (_out, _err)                    =   p.communicate()
            assert not _out
            assert _err is None

            cmd =   """ update system_log set ended=now()
                        where uid = %d;
                    """ % row.uid

            SYS_R.T.conn.set_isolation_level(   0)
            SYS_R.T.cur.execute(                cmd)

            SYS_R._growl(                       'pgdump logrotate process finished')


def run_method(illustrate=True):
    """
        Every time log files are evaluated, keep a log file if:
            (1) the log file falls on a marked day as graphed above, or
            (2) there is no log file for a marked day "A" and
                a log file "F" is the closest in time
                between and including
                the day before X
                and
                the day after the next oldest marked day "B",
                i.e., B+1<= F <=A-1

        GOAL: find log closest to ILD
    """

    check_db_for_action()

    df = make_ideal_log_spectrum()

    # Drop non-log dates for now (and in practice, but will re-include for illustration)
    idx = df[df.log_dates.isnull()].index
    df = df.drop(idx,axis=0).reset_index(drop=True)

    # Conform Data Type
    df['log_dates'] = df.log_dates.map(lambda D: pd.to_datetime(D))

    ideal_log_dates = df.log_dates.tolist()

    # Get Logs
    p                                       =   sub_popen(['ls ~/.pg_dump'],stdout=sub_PIPE,shell=True)
    (_out, _err)                            =   p.communicate()
    logs                                    =   _out.strip('\n').split('\n')

    log_dates = map(lambda X: dt.datetime.strptime("%s/%s/%s" %
                                       re_findall(r'(\d{4})[_\.](\d{2})[_\.](\d{2})\.',X)[0],
                                       "%Y/%m/%d"),logs)
    log_dict                                =   dict(zip(map(lambda D: pd.to_datetime(D),log_dates),logs))
    lf = pd.DataFrame(data={'logs':pd.unique(log_dates)})

    # Find Intersecting Values
    initial_matches = pd.Series(pd.np.intersect1d(df.log_dates.values,lf.logs.values))

    # (1) the log file falls on a marked day
    lf['keep'] = lf.logs.where(lf.logs.isin(initial_matches))
    df['paired'] = df.log_dates.where(df.log_dates.isin(initial_matches))

    # (2) What is left?
    #    A. Check by getting date bounds of unclaimed logs,
    #       then counting how many remaining ILDs are not yet paired with a log.
    #    B. Iterate these remaining ILDs to match up with log,
    #       then discard any unmatched logs.


    #   (A)
    to_check = lf[lf.keep.isnull()]
    oldest_log,latest_log = to_check.logs.min(),to_check.logs.max()

    older_dates,earlier_dates = df[df.log_dates<oldest_log],df[latest_log<df.log_dates]
    assert len(older_dates)+len(earlier_dates)>=2

    next_older_date,prev_earlier_date = older_dates.iloc[0,:],earlier_dates.iloc[-1,:]
    idl_dates = df.ix[prev_earlier_date.name:next_older_date.name,:]

    #   (B)

    pt,last_idx = 0,idl_dates.index.tolist()[-1]
    for idx,row in idl_dates.iterrows():
        if idx==last_idx:
            break

        if pd.isnull(row.paired):
            A,B=row.log_dates,idl_dates.iloc[pt+1,:].log_dates
            possible_logs = lf[(lf.logs<A)&(B<lf.logs)]
            if len(possible_logs):
                res = possible_logs.sort('logs',ascending=False).iloc[0,:].logs
                D=row.to_dict()
                D.update({'paired':res})
                df[df.index==idx].update(D.values())

        pt+=1

    # Find Intersecting Values b/t Paired IDLs and Remaining Logs
    final_matches = pd.Series(pd.np.intersect1d(idl_dates.paired.values,lf.logs.values))

    lf.keep.update(lf.logs.where(lf.logs.isin(final_matches)))

    if illustrate:

        # SHOW ME THE RESULTS: [ what did we want, what did we get, what did we do ]
        # Plot these "IDLs" as blue vertical bars
        # Then Overlay all logs in yellow
        # Then Overlay all logs to be deleted in red

        start,end=lf.logs.max(),lf.logs.min()
        one_day = dt.timedelta(days=+1)
        res = pd.DataFrame({'dates':[start-(i*one_day) for i in range( (start-end).days )] })
        res['days'] = res.dates.map(lambda D: D.dayofyear)
        ndf = make_ideal_log_spectrum()
        ndf['log_dates'] = ndf.log_dates.map(lambda D: pd.to_datetime(D))
        all_log_dates = ndf.log_dates.tolist()
        res['IDLs'] = res.dates.map(lambda D: 0 if not all_log_dates.count(D) else 3)
        logs_to_keep = lf[lf.keep.notnull()].logs.tolist()
        logs_to_delete = lf[lf.keep.isnull()].logs.tolist()
        res['Keep'] = res.dates.map(lambda D: 0 if not logs_to_keep.count(D) else 2)
        res['Delete'] = res.dates.map(lambda D: 0 if not logs_to_delete.count(D) else 1)

        # Make Plot
        import pylab as plt

        fig = plt.figure()
        axes = fig.add_subplot(1,1,1)

        res.plot(x='days',y='IDLs',ylim=(0,6),ax=axes,kind='bar',figsize=(107,8),color='b')
        res.plot(x='days',y='Keep',ylim=(0,6),ax=axes,kind='bar',figsize=(107,8),color='y')
        res.plot(x='days',y='Delete',ylim=(0,6),ax=axes,kind='bar',figsize=(107,8),color='r')
        axes.invert_xaxis()
        fig.savefig('/Volumes/mbp2/Users/admin/Desktop/plot.png')

        log_files                           =   res[res.Delete==1].dates.map(log_dict).tolist()

        cmd =   """ insert into system_log (operation,started,parameters)
                    values ('rotate_pgdump',now(),'%s')
                """ % str(log_files).strip('[]').replace("'",'').replace(' ','')

        SYS_R.T.conn.set_isolation_level(               0)
        SYS_R.T.cur.execute(                            cmd)

        SYS_R._growl('check desktop for intended logrotate actions and update pgsql')



if __name__ == '__main__':
    print                                       ""
    run_method(                                 )