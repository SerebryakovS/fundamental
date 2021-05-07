#!/usr/bin/python3.8

import os
import sys
import json
import uuid
import dill
#import redis
import queue
import psutil
import signal
import random
import asyncio
import aiohttp
import threading as thrd
import multiprocessing as mp
AMP_SUCCESS = 0;
AMP_FAILURE =-1;
AMP_WAITING =-2;
#=================================================================
class AMP_object(object):
    def __init__():
        pass;
amp_object = {"url":None,"data":None,"method":None,"headers":None,
              "rest2rest":{"func":None,"args":None}};
#=================================================================
class AMP_process(mp.Process):
    def __init__(self,ext_Iqueue,qread_limit=50):
        signal.signal(signal.SIGTERM, self.amp_exit_gracefully);
        super(AMP_process, self).__init__();
        self.qreadl = qread_limit;
        self.ext_Iqueue = ext_Iqueue;
        self.control_list = list();
        self.dill_filename = 'amp_process_que_'+str(random.randint(1,1000000000))+'.pkl';
    #--------------------------------------------
    def amp_save_worker_state(self,requests_list):
        with open(self.dill_filename,'wb') as df:
            dill.dump(requests_list,df);
    #--------------------------------------------
    def amp_exit_gracefully(self,signum, frame):
        self.amp_save_worker_state(self.control_list);
        exit(AMP_SUCCESS);
    def amp_run_method(self, method_type):
        return { "GET" : self.amp_go_GET, "POST": self.amp_go_POST}[method_type];
    async def amp_go_GET(self,request,session: aiohttp.ClientSession):
        async with session.get((request['url']),headers=request['headers']) as response:
            if (self.amp_check_status(request,response) != 200):
                await asyncio.sleep(1);
            return await response.json();
    async def amp_go_POST(self,request,
                         session: aiohttp.ClientSession):
        async with session.post(request['url'],
                                data=request['data'],headers=request['headers']) as response:
            self.amp_check_status(request,response);
            return await response.json()
    def amp_check_status(self,request,response):
        inform_message = "";
        if response.status == 200:
            self.control_list.remove(request);
            self.amp_save_worker_state(self.control_list);
            inform_message = "[OK]:";
        else:
            if request['method'] == "GET":
                self.ext_Iqueue.put(request);
            else:
                self.control_list.remove(request);
            inform_message = "[FAILED]:";
        print("%s"%(inform_message));
        print("\tprocess: %d"%(os.getpid()));
        print("\trequest_id: %s"%(request['id']));
        print("\tmethod: %s"%(request['method']));
        print("\turl: %s"%(request['url']));
        print("\tresponse_code: %d"%(response.status));
        return response.status;
    #===========================================================EXTENSION
    #def amp_rest2rest(function):
        #async def wrapper_around(amp_object_01,amp_object_02):
            #return await function(*args);
        #return wrapper_around;
    #===========================================================EXTENSION
    async def amp_run_session(self):
        futures = [];
        futures_que = queue.Queue();
        while self.qreadl > 0 and not self.ext_Iqueue.empty():
            futures_que.put(self.ext_Iqueue.get());
        async with aiohttp.ClientSession() as session:
            requests_to_make = [];
            requests_methods = [];
            while not futures_que.empty():
                requests_to_make.append(futures_que.get());
                requests_methods.append(
                    self.amp_run_method(requests_to_make[-1]["method"])(
                        request=requests_to_make[-1],session=session));
                self.qreadl-=1;
            self.control_list = requests_to_make.copy();
            futures = await asyncio.gather(*requests_methods);
        for result_idx in range(len(futures)):
            try:
                json_obj = json.dumps(futures[result_idx]);
                #==========================================
                #with redis.Redis() as redis_storage:
                    #redis_storage.set(str(requests_to_make[result_idx]['id']),json_obj);
                if "rest2rest" in requests_to_make[result_idx]:
                    new_amp_obj = requests_to_make[result_idx]["rest2rest"]["func"](
                        futures[result_idx],*requests_to_make[result_idx]["rest2rest"]["args"]);
            except Exception as exception:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno,requests_to_make[result_idx]['id'],exception);
                print(futures[result_idx])
    def run(self):
        asyncio.run(self.amp_run_session());
#===========================AMP_REQUESTER
class AMP_requester(object):
    cpus_info = dict();
    pooler_status = False;
    def __init__(self,save_state=True):
        self.Iqueue = mp.Queue(); # input queue for storing new requests 
        self.cpu_count = psutil.cpu_count(); # max cpu count
        #with redis.Redis() as redis_storage: # clear redis database
            #redis_storage.flushdb();
        self.pooler_thrd = thrd.Thread();
        self.amp_is_saved_states = False;
        self.saved_states = [];
        for filename in os.listdir():
            if 'amp_process_que_' in filename:
                self.saved_states.append(filename);
        if len(self.saved_states) > 0:
            self.amp_is_saved_states = True;
    def amp_put_request(self,amp_object):
        request_id = uuid.uuid4().hex;
        amp_object.update({"id":request_id});
        self.Iqueue.put(amp_object);
        return request_id;
    def amp_waitend(self):
        self.pooler_thrd.join();
    #------------------------------------------------------
    def amp_restore_iqueue(self):
        ret_code = True
        if len(self.saved_states) < 0:
            raise Exception("couldn't find any saved state");
        for dill_filename in self.saved_states:
            try:
                requests_list = None;
                with open(dill_filename,"rb") as dfile:
                    requests_list = dill.load(dfile);
                while len(requests_list) > 0:
                    self.Iqueue.put(requests_list.pop());
            finally:
                os.remove(dill_filename);
        if self.Iqueue.empty():
            os.kill(os.getpid(),signal.SIGINT);
            ret_code = False;
        return ret_code;
    #------------------------------------------------------
    def amp_stop_system(self,sig,frame):
        print("terminating amp_pooler...\n");
        self.pooler_status = False;
    def amp_run(self,block=False):
        signal.signal(signal.SIGINT, self.amp_stop_system);
        self.pooler_status = True;
        if block is not True:
            self.pooler_thrd = thrd.Thread(target=self._amp_run,args=());
            self.pooler_thrd.start();
        else: self._amp_run();
    def _amp_run(self):
        running_cpus = [list() for i in range(self.cpu_count)]; 
        while self.pooler_status is True:
            #===========================================================JOIN_FINISHED
            for cpu_idx in range(len(running_cpus)):
                for proc in running_cpus[cpu_idx]:
                    if not proc.is_alive():
                        proc_index = running_cpus[cpu_idx].index(proc);
                        dead_proc = running_cpus[cpu_idx].pop(proc_index); 
                        dead_proc.join(timeout=0);
            #===========================================================JOIN_FINISHED
            cpus_percent = psutil.cpu_percent(interval=2.0,percpu=True);
            if not self.Iqueue.empty():
                min_util_cpu_idx = cpus_percent.index(min(cpus_percent));
                if min_util_cpu_idx >= 80.0:
                    continue;
                running_cpus[min_util_cpu_idx].append(mp.Process(
                    target=self.executor,args=(min_util_cpu_idx,self.Iqueue)));
                running_cpus[min_util_cpu_idx][-1].start();
        for cpu in running_cpus:
            for process in cpu:
                if type(process) == mp.Process:
                    process.terminate();
    def executor(self,cpu_idx,i_que):
        proc_pid = psutil.Process();
        proc_pid.cpu_affinity([cpu_idx]);
        print("PID=%d CPU:%d"%(os.getpid(),proc_pid.cpu_affinity()[0]));        
        amp_proc = AMP_process(i_que);
        amp_proc.start();
    
    
    

