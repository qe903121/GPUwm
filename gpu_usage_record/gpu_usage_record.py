import os
import schedule
import time
import pandas as pd
import numpy as np
import argparse
from pynvml import *
from pynvml.smi import nvidia_smi
from datetime import datetime,timezone,timedelta
import threading


def getRateGPU(DeviceQuery):
    numGPU = len(DeviceQuery['gpu'])
    print('GPU numbers: ', len(DeviceQuery['gpu']))

    rateGPU = list()
    for i in range(numGPU):
        freeMEM = int(DeviceQuery['gpu'][i]['fb_memory_usage']['free'])
        totalMEM = int(DeviceQuery['gpu'][i]['fb_memory_usage']['total'])
        # print('GPU id:', i, ' Free:', freeMEM)
        rate = '{:.0f}'.format( (totalMEM-freeMEM) / totalMEM *100)
        rateGPU.append((i,rate))
        print('free GPU id:', i, ' usage rate:', rate)
    return rateGPU


def getDeviceInfo(require):
    if require == 'overview':
        nvmlInit()
        print("Driver Version:", nvmlSystemGetDriverVersion())
        deviceCount = nvmlDeviceGetCount()
        for i in range(deviceCount):
            handle = nvmlDeviceGetHandleByIndex(i)
            print("Device", i, ":", nvmlDeviceGetName(handle))

    elif require == 'nvsmi':
        nvsmi = nvidia_smi.getInstance()
        nvsmi = nvsmi.DeviceQuery('memory.free, memory.total')
        return nvsmi
        
    else:
        raise ValueError(require)


def getTime():
    dt1 = datetime.utcnow().replace(tzinfo=timezone.utc)
    dt2 = dt1.astimezone(timezone(timedelta(hours=8))) # 轉換時區 -> 東八區
    fullTime = dt2.strftime("%Y-%m-%d %H:%M:%S")
    Y = dt2.strftime("%Y")
    M = dt2.strftime("%m")
    D = dt2.strftime("%d")
    H = dt2.strftime("%H")
    M = dt2.strftime("%M")
    S = dt2.strftime("%S")
    return Y, M, D, H, M, S, fullTime

class rateGPUTable:
    def __init__(self):
        print('init rate GPU table')

    def readTable(self, path):
        self.taskTable = pd.read_excel(path)
        self.path = path
    
    # def checkTask(self):
    #     self.readEnq = [ i for i in range(self.taskTable.shape[0]) if self.taskTable.iloc[i].status==0 ]
    #     taskInfo = []
    #     for i in self.readEnq:
    #         taskInfo.append([i,self.taskTable.iloc[i]])
    #     return taskInfo
        
    def updataTable(self, index, item, value):
        self.taskTable.loc[index,item] = value
        # print(self.taskTable)
        self.taskTable.to_excel(self.path, index=False)


    def getIndex(self):
        return self.taskTable.index

    def showTable(self):
        print('\n'*2)
        print(self.taskTable)
        print('\n'*2)


# def routine():


if __name__ == '__main__': 

    rateGPUTable = rateGPUTable()
    rateGPUTable.readTable('./usageGPU.xlsx')
    rateGPUTable.showTable()
    index = rateGPUTable.getIndex()

    try:
        indexNow = index.to_numpy()[-1]
        print(indexNow)
    except:
        indexNow = 0


    routineH = ['00','03','06','09','12','15','18','21']

    while(True):
        Y, M, D, H, M, S, fullTime = getTime()

        print(fullTime)

        if str(H) in routineH and str(M) == '00' and str(S) == '00':
            indexNow = indexNow + 1

            deviceInfo = getDeviceInfo('nvsmi')
            rateGPU = getRateGPU(deviceInfo)
            for index, rate in rateGPU:
                rateGPUTable.updataTable(indexNow,'gpu'+ str(index), str(rate))
                rateGPUTable.updataTable(indexNow,'time',  str(H))
                rateGPUTable.updataTable(indexNow,'date',  str(fullTime))
            rateGPUTable.showTable()
        time.sleep(1)
