import json
import pandas as pd
import os, sys
import time
import threading
import re
import random, string
from flask import jsonify
from diskall import *


NULL = 0
completed_list = []
global startTime, endTime


def pipeline(arg):
    try:
        print("\n***** Started for",arg[2], "*****" ) 
        startp = time.time()
        df_Measurement = arg[0]
        measure_type = arg[1]
        host = arg[2]
        new_filename = arg[3]
        start_date = arg[4]

        ########### filter ##################
        json_dict = get_filtered_data(df_Measurement)
        
        ########### Processing ##############
        table_json_array = process(json_dict, measure_type)
        print("\n***** Transformed: host: {}: Total: {} for: {} *****".format(host, len(table_json_array), measure_type) )
        df = convert_to_csv(table_json_array)
        df = normalize(df, measure_type)
        df = restructure_for_sql(df, measure_type)
        
        ########### csv creation ##################
        # timestamp = str(datetime.now().strftime("%d%m%Y%H%M%S"))
        # outputpath = os.path.join(os.getcwd(), "output")
        # generated_path = os.path.join(outputpath, str(host))
        # if not os.path.exists(generated_path):
        #     os.makedirs(generated_path)
        #     print(f"{generated_path} folder created.")
        # else:
        #     print(f"{generated_path} folder exists.")
        # outputfile = os.path.join(generated_path, str(host)+"_"+ timestamp +"_"+ measure_type +".csv")

        # df.to_csv(outputfile)
        
        ########### insert to db ###########
        ingestData(df, measure_type)

        endTime = None
        process_msg = "ETL under process"
        status = "Running"
        if (measure_type == 'cpu_total'):
            status_percentage = "42%"
        elif (measure_type == 'disks'):
            status_percentage = "55%"
        elif (measure_type == 'networks'):
            status_percentage = "67%"
        elif (measure_type == 'proc_meminfo'):
            status_percentage = "80%"
        updateStatusTbl(new_filename, endTime, process_msg, status, status_percentage, start_date)
        
        print("\n-***** Completed: host: {}: total: {} for: {} in time s: {} *****".format(host, len(table_json_array), str(measure_type), time.time() -startp))
        list_val = host + "_" + measure_type
        completed_list.append(list_val)
        print("completed_list: ", completed_list)
        completed_list.clear()
        return 1
        
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(" line number: {}, error : {}".format( exc_tb.tb_lineno, e))
        return 0


def runInParallel(args_list):
    for arg in args_list:
        t = threading.Thread(target = pipeline, args = [arg])
        t.start()
        t.join()


def convert_to_json(file):
    with open(file) as f:
        dict_list = f.readlines()
    dict_list_final=[]
    for str_dict in dict_list:
        dict_list_final.append(json.loads(str_dict))
    return dict_list_final


def prep_time(txt):
    return txt.split('.')[0].replace('T',' ')


def load_with_tags(dict_list_final, uuid_key, benchmark_tag, host, workload, iteration, is_workload_data, tag, df_table_name):
    #Create a DF
    uuid_key = None
    df = pd.DataFrame(dict_list_final)
    print(len(df))
    df_table = pd.DataFrame(list(df[df_table_name].values))

    pk_val = []
    for i in range(len(df)):
        pk = 'wp_' + ''.join((random.choice(string.ascii_lowercase) for x in range(5))) + "_" + str(datetime.now().strftime("%d%m%Y%H%M%S"))
        pk_val.append(pk)

    if df_table_name=="cpu_total":
        df_table['systems'] = '/proc/stat'
        df_table['cpu'] = 'cpu'
    elif df_table_name=="proc_meminfo":
        df_table['systems'] = '/proc/meminfo'
    elif df_table_name=="networks":
        df_table['systems'] = '/proc/net/dev'
    elif df_table_name=="disks":
        df_table['systems'] = '/proc/diskstats'

    df_timestamp_val = pd.DataFrame(list(df['timestamp'].values))
    df_table['pkey'] = [_ for _ in pk_val]
    df_table['uuid'] = uuid_key
    df_table['time_stamp'] = df_timestamp_val['datetime'].apply(prep_time)
    df_table['benchmark_tag'] = benchmark_tag
    df_table['host'] = host
    df_table['workload'] = workload 
    df_table['iteration'] = iteration 
    df_table['tag'] = tag
    df_table['is_workload_data'] = is_workload_data
    df_table['upload_date'] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    return df_table


def get_filtered_data(df_table):
    df_tmp = df_table
    df_tmp.reset_index(drop=True, inplace=True)
    json_dict = df_tmp.to_dict('records')
    print("number of records:", len(json_dict))
    return json_dict


def convert_to_csv(transformed_array):
    dict_list = []
    for trans_dict in transformed_array:    
        for i in range(len(trans_dict.get('data'))):
            dict_list.append(trans_dict.get('data')[i].copy())
    df = pd.DataFrame(dict_list)
    return df


def normalization(df, attribute):
    # zi = (xi – min(x)) / (max(x) – min(x)) * 100
    # Dirty, delta_obytes, delta_ibytes
    minm = min(df[attribute].values)
    maxm = max(df[attribute].values)
    #df[attribute+'_Norm'] = df[attribute].apply(lambda x: (x - minm // (maxm - minm)) )
    df[attribute+'_norm'] = ((df[attribute] - minm ) / (maxm - minm) )*100
    return df


def normalize(df, measure_type):
    if measure_type == "proc_meminfo":
        attribute = "dirty"
        df = normalization(df, attribute)
    return df


def get_network_table(measurement):
    key_idx = 0
    tag_list = ['systems', 'uuid', 'pkey', 'time_stamp', 'benchmark_tag', 'host', 'workload', 'iteration', 'file_name', 'tag', 'is_workload_data', 'upload_date', 'lo']
    metric_columns = ['nics', 'rxbytes', 'rxpackets', 'rxerrs', 'rxdrop', 'rxfifo', 'rxframe', 'txbytes', 'txpackets', 'txerrs', 'txdrop', 'txfifo', 'txcolls', 'txcarrier']  
    metric_list = []
        
    for nic in measurement.keys():
        if nic not in tag_list:
            metric_list.append([nic] + list(measurement.get(nic).values()))

    return pd.DataFrame(metric_list, columns = metric_columns)


def get_disk_table(measurement):
    key_idx = 0
    tag_list = ['systems', 'uuid', 'pkey', 'time_stamp', 'benchmark_tag', 'host', 'workload', 'iteration', 'file_name', 'tag', 'is_workload_data', 'upload_date']
    metric_columns = ['disks', 'reads', 'rmerge', 'rkb', 'rmsec', 'writes', 'wmerge', 'wkb', 'wmsec', 'inflight', 'busy', 'backlog', 'xfers', 'bsize']  
    metric_list = []

    for disk in measurement.keys():
        if disk not in tag_list:
            if not re.match(r'loop\d', disk) and not re.match(r'sr\d', disk):
                metric_list.append([disk] + list(measurement.get(disk).values()))

    return pd.DataFrame(metric_list, columns = metric_columns)


def process(json_dict, measure_type):
    try:
        i = 0
        z = len(json_dict)
        table_json_array = []

        while i < z :
            table_json = {}

            #=========== cpu utilization ===========
            if measure_type =="cpu_total":
                df_table = pd.DataFrame()
                
                metric_columns = ['username', 'nice', 'system_value', 'idle', 'iowait', 'irq', 'softirq', 'guest', 'guestnice', 'steal']
                metric_list = [json_dict[i]['user'], json_dict[i]['nice'], json_dict[i]['sys'], json_dict[i]['idle'], json_dict[i]['iowait'], json_dict[i]['hardirq'], 
                                json_dict[i]['softirq'], json_dict[i]['guest'], json_dict[i]['guestnice'], json_dict[i]['steal']]
                
                df_table = pd.DataFrame([metric_list], columns = metric_columns)
            
                util_calc= df_table['irq'][0] + df_table['nice'][0] + df_table['softirq'][0] + df_table['system_value'][0] + df_table['username'][0] + df_table['guest'][0] + df_table['guestnice'][0] + df_table['steal'][0]
                df_table['utilization'] = round(util_calc, 3)
                df_table['idle'] = round(100 - df_table['utilization'] - df_table['iowait'][0], 3)

                df_table['cpu'] = json_dict[i]['cpu']
                df_table['systems'] = json_dict[i]['systems']

                df_table['timestamp'] = json_dict[i]['time_stamp']
                df_table['last_idle'] = NULL
                df_table['last_total'] = NULL
                df_table['total'] = NULL

            #=========== memory utilization ===========
            elif measure_type =="proc_meminfo":
                df_table = pd.DataFrame()

                metric_columns = ['active', 'Active(anon)', 'Active(file)', 'anonhugepages', 'anonpages', 'bounce', 'buffers', 'cached', 'commitlimit', 
                                'committed_as', 'directmap1g', 'directmap2m', 'directmap4k', 'dirty', 'hugepages_free', 'hugepages_rsvd', 
                                'hugepages_surp', 'hugepages_total', 'hugepagesize', 'inactive', 'Inactive(file)', 'Inactive(anon)', 'kernelstack', 'mapped', 
                                'memfree', 'memavailable', 'memtotal', 'mlocked', 'nfs_unstable', 'pagetables', 'percpu', 'shmem', 'slab', 'sreclaimable', 
                                'sunreclaim', 'swapcached', 'swapfree', 'swaptotal', 'unevictable', 'vmallocchunk', 'vmalloctotal', 'vmallocused', 'writeback', 
                                'writebacktmp', 'FileHugePages', 'FilePmdMapped', 'Hugetlb', 'KReclaimable', 'ShmemHugePages', 'ShmemPmdMapped']
                
                metric_list = [json_dict[i]['Active'], json_dict[i]['Active_anon'], json_dict[i]['Active_file'], json_dict[i]['AnonHugePages'], 
				                json_dict[i]['AnonPages'], json_dict[i]['Bounce'], json_dict[i]['Buffers'], json_dict[i]['Cached'], json_dict[i]['CommitLimit'], 
                                json_dict[i]['Committed_AS'], json_dict[i]['DirectMap1G'], json_dict[i]['DirectMap2M'], json_dict[i]['DirectMap4k'], json_dict[i]['Dirty'], 
                                 json_dict[i]['HugePages_Free'], json_dict[i]['HugePages_Rsvd'], json_dict[i]['HugePages_Surp'], 
                                json_dict[i]['HugePages_Total'], json_dict[i]['Hugepagesize'], json_dict[i]['Inactive'], json_dict[i]['Inactive_file'], json_dict[i]['Inactive_anon'], 
                                json_dict[i]['KernelStack'], json_dict[i]['Mapped'], json_dict[i]['MemFree'], json_dict[i]['MemAvailable'], json_dict[i]['MemTotal'], 
                                json_dict[i]['Mlocked'], json_dict[i]['NFS_Unstable'], json_dict[i]['PageTables'], json_dict[i]['Percpu'], json_dict[i]['Shmem'], 
                                json_dict[i]['Slab'], json_dict[i]['SReclaimable'], json_dict[i]['SUnreclaim'], json_dict[i]['SwapCached'], json_dict[i]['SwapFree'], 
                                json_dict[i]['SwapTotal'], json_dict[i]['Unevictable'], json_dict[i]['VmallocChunk'], json_dict[i]['VmallocTotal'], json_dict[i]['VmallocUsed'], 
                                json_dict[i]['Writeback'], json_dict[i]['WritebackTmp'], json_dict[i]['FileHugePages'], 
                                json_dict[i]['FilePmdMapped'], json_dict[i]['Hugetlb'], json_dict[i]['KReclaimable'], json_dict[i]['ShmemHugePages'], json_dict[i]['ShmemPmdMapped']]

                df_table = pd.DataFrame([metric_list], columns = metric_columns)

                df_table['utilization'] = ( int(df_table['memtotal'][0]) - int(df_table['memavailable'][0]) ) / int(df_table['memtotal'][0]) * 100
                cache_free = int(df_table['memfree'][0]) + int(df_table['buffers'][0]) + int(df_table['cached'][0])
                cache_used = int(df_table['memtotal'][0]) - (cache_free)
                df_table['cached_percent'] = (cache_used/(cache_used+cache_free)) * 100

                df_table['systems'] = json_dict[i]['systems']
                df_table['timestamp'] = json_dict[i]['time_stamp']

            #=========== network utilization ===========
            elif measure_type =="networks":
                df_table = pd.DataFrame()

                measurement = json_dict[i]
                df_table = get_network_table(measurement)

                df_table['delta_txbytes'] =  df_table['txbytes'] 
                df_table['delta_rxbytes'] = df_table['rxbytes']
                df_table['systems'] = json_dict[i]['systems']

                df_table['txbytes'] = NULL
                df_table['rxbytes'] = NULL
                df_table['timestamp'] = json_dict[i]['time_stamp']
                df_table['rxmulticast'] = NULL
                df_table['rxcompressed'] = NULL
                df_table['txcompressed'] = NULL
                df_table['last_txbytes'] = NULL
                df_table['last_rxbytes'] = NULL
                df_table['delta_txbytes_norm'] = NULL
                df_table['delta_rxbytes_norm'] = NULL


            #=========== disk utilization ===========
            elif measure_type =="disks":
                df_table = pd.DataFrame()

                measurement = json_dict[i]
                df_table = get_disk_table(measurement)

                df_table['systems'] = json_dict[i]['systems']
                df_table['timestamp'] = json_dict[i]['time_stamp']

                df_table['last_weighted_time_spent_ios'] = NULL
                df_table['last_time_spent_ios'] = NULL
                df_table['time_spent_ios'] = NULL
                df_table['weighted_time_spent_ios'] = NULL

                if (( df_table['rmsec'][0] + df_table['wmsec'][0] ) != 0):
                    df_table['weighted_percent'] = (df_table['rmerge'] + df_table['wmerge']) / (df_table['rmsec'] + df_table['wmsec'])
                else:
                    df_table['weighted_percent'] = 0

                df_table['busy_percent'] = df_table['busy']
                df_table['busy'] = NULL

            df_table['pkey'] = json_dict[i]['pkey']
            df_table['uuid'] = json_dict[i]['uuid']
            df_table['benchmark_tag'] = json_dict[i]['benchmark_tag']
            df_table['host'] = json_dict[i]['host']
            df_table['workload'] = json_dict[i]['workload']     
            df_table['iteration'] = json_dict[i]['iteration']
            df_table['is_workload_data'] = json_dict[i]['is_workload_data']
            
            df_table['time_stamp'] = json_dict[i]['time_stamp']
            df_table['upload_date'] = json_dict[i]['upload_date']

            df_table['tag'] = json_dict[i]['tag']

            table_json['data'] = df_table.to_dict('records')
            table_json_array.append(table_json)
                
            i = i+1
    
        return table_json_array
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(" line number: {}, error : {}".format( exc_tb.tb_lineno, e))
        return 0


def drop_cols_from_df(df1, col_list):
    for col in col_list:
        if col in df1.columns:
            df1.drop(columns=col, axis=1, inplace=True)
        else:
            print("Not Found:",col)
    return df1


def restructure_for_sql(df, measure_type):
    try:
        #==========filter the fixed attributes=======
        attr_common = ['tag', 'pkey', 'benchmark_tag', 'host', 'workload', 'iteration', 'time_stamp', 'is_workload_data']
        if measure_type == "cpu_total":
            dynamic_column = attr_common + ['cpu', 'systems']
            attr_list = ['timestamp', 'cpu', 'system_value', 'systems', 'last_idle', 'nice', 'irq', 'idle', 'last_total', 'utilization', 'iowait', 'username', 'total', 
            'softirq', 'upload_date', 'tag', 'benchmark_tag', 'host', 'workload', 'iteration', 'time_stamp', 'is_workload_data', 'guest', 'guestnice', 'steal', 'uuid', 'pkey']

        elif measure_type == "proc_meminfo":
            dynamic_column = attr_common + ['cached_percent', 'dirty_norm', 'systems']
            attr_list = ['timestamp', 'Inactive(anon)', 'unevictable', 'writebacktmp', 'memtotal', 'hugepages_surp', 'vmallocused', 'nfs_unstable', 
            'buffers', 'kernelstack', 'hugepages_rsvd', 'vmalloctotal', 'anonhugepages', 'systems', 'Inactive(file)', 'swaptotal', 'mapped', 'hugepagesize', 
            'memavailable', 'memfree', 'active', 'swapcached', 'directmap4k', 'utilization', 'commitlimit', 'shmem', 'sunreclaim', 
            'directmap2m', 'cached', 'hugepages_total', 'pagetables', 'swapfree', 'sreclaimable', 'inactive', 'vmallocchunk', 'percpu', 'Active(file)', 
            'mlocked', 'writeback', 'anonpages', 'hugepages_free', 'Active(anon)', 'directmap1g', 'slab', 'bounce', 'committed_as', 'dirty', 'upload_date',
            'tag', 'benchmark_tag', 'host', 'workload', 'iteration', 'time_stamp', 'cached_percent', 'dirty_norm', 'is_workload_data', 
            'FileHugePages', 'FilePmdMapped', 'Hugetlb', 'ShmemHugePages', 'ShmemPmdMapped', 'uuid', 'pkey']

        elif measure_type == "disks":
            dynamic_column = attr_common + ['disks', 'systems']
            attr_list = ['timestamp', 'disks', 'writes', 'busy_percent', 'reads', 'last_weighted_time_spent_ios', 'systems', 'last_time_spent_ios', 
            'time_spent_ios', 'weighted_time_spent_ios', 'weighted_percent', 'upload_date', 'tag', 'benchmark_tag', 'host', 'workload', 'iteration', 
            'time_stamp', 'is_workload_data', 'backlog', 'bsize', 'busy', 'inflight', 'rkb', 'rmerge', 'rmsec', 'wkb', 'wmerge', 'wmsec', 'xfers', 'uuid', 'pkey']

        elif measure_type == "networks":
            dynamic_column = attr_common+ ['delta_txbytes', 'delta_rxbytes', 'systems', 'nics']
            attr_list = ['timestamp', 'nics', 'rxerrs', 'txpackets', 'rxmulticast', 'rxdrop', 'txbytes', 'rxpackets', 'systems', 'rxcompressed',
            'txcompressed', 'txerrs', 'rxfifo', 'rxframe', 'txcarrier', 'txcolls', 'txdrop', 'txfifo', 'rxbytes', 'upload_date', 'tag', 
            'benchmark_tag', 'host', 'workload', 'iteration', 'time_stamp', 'last_txbytes', 'last_rxbytes', 'delta_txbytes', 'delta_rxbytes', 
            'delta_txbytes_norm', 'delta_rxbytes_norm', 'is_workload_data', 'uuid', 'pkey']
       
        #drop extra columns from json df
        extra_to_drop = set(df.columns) - set(attr_list)
        df = drop_cols_from_df(df, extra_to_drop)

        #get filtered df
        df = df[attr_list]
        #get dynamic df
        df_tmp = df[dynamic_column].copy()
        #remove from original df
        df = drop_cols_from_df(df, dynamic_column)
        #append df_tmp at the end of df columns
        #df_concat = pd.merge(df, df_tmp, on='common_column_name', how='outer')
        df_restuctured = pd.concat([df, df_tmp], axis=1)
        df_restuctured.rename(columns={'Unnamed: 0': 'rec_id'},inplace=True)
        print(measure_type, ":", df_restuctured.columns)
        if len(df) == len(df_restuctured):
            return df_restuctured
        else:
            raise Exception("Error in restructuring")
    except Exception as e:
        print("Error in restructuring:",e)