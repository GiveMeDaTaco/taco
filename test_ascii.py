#!/usr/bin/env python3
import time, os
os.environ['LOADING_BAR_ASCII']='1'
from tlptaco.utils.loading_bar import ProgressManager
layers=[("L1",5),("L2",10)]
pm=ProgressManager(layers,units='steps',title='Test')
with pm:
    for _ in range(10):
        pm.update("L1",1)
        pm.update("L2",1)
        time.sleep(0.1)
