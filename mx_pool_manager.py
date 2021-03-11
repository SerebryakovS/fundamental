#!/usr/bin/python
import mxnet
import multiprocessing as mp



class mx_pool_manager(object):
    def __init__(self):
        self.Iqueue = mp.Queue(); # input queue for storing new requests 
        self.Oqueue = mp.Queue(); # output queue for getting detection results
        self.avail_cpu_count = mp.cpu_count(); # max cpu count
        
    def add_data_batch(*packets):
        for packet in packets:
            self.Iqueue.put(packet);
        
        
    def run_preprocessing(self):
        while self.avail_cpu_count > 0 and not self.Iqueue.empty():
            data_packet = self.Iqueue.get();
            self.avail_cpu_count-=1;
        
        
        
if __name__ == "__main__":
    x = mx_pool_manager();
    print(x.cpu_count)
