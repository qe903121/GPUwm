import os
import schedule
import time
from collections import deque
import pandas as pd
from pynvml import *
from pynvml.smi import nvidia_smi
import subprocess
import numpy as np
import argparse

parser = argparse.ArgumentParser(description="GPUWM")
parser.add_argument("--GPUthreshold", type=int, default=8000)
parser.add_argument("--routine", type=int, default=20)
parser.add_argument("--refreshGPU", type=int, default=20)
parser.add_argument("--ModelBulid", type=int, default=60)
args = parser.parse_args()

class taskQueue:
    def __init__(self):
        self.taskQue = deque()
        print('init task queue.')
    
    def enqueue(self, index, user, numGPU, condaEnv, path, command, outputLog, indexGPU):
        newTask = dict()
        newTask['index'] = index
        newTask['user'] = user
        newTask['numGPU'] = numGPU
        newTask['condaEnv'] = condaEnv
        newTask['path'] = path
        newTask['command'] = command
        newTask['output'] = outputLog
        newTask[indexGPU] = indexGPU
        self.taskQue.appendleft(newTask)

    def dequeue(self):
        return self.taskQue.pop()

    def is_empty(self):
        if self.taskQue:
            return False
        else:
            return True


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


def getFreeGPU(DeviceQuery, threshold):
    numGPU = len(DeviceQuery['gpu'])
    print('GPU numbers: ', len(DeviceQuery['gpu']))

    freeGPU = list()
    for i in range(numGPU):
        freeMEM = int(DeviceQuery['gpu'][i]['fb_memory_usage']['free'])
        # print('GPU id:', i, ' Free:', freeMEM)
        if freeMEM > threshold:
            freeGPU.append(i)
    print('free GPU id:', freeGPU, ' threshold:', threshold)
    return freeGPU



class taskTable:
    def __init__(self):
        print('init task table')

    def readTable(self, path):
        self.taskTable = pd.read_excel(path)
        self.path = path
    
    def checkTask(self):
        self.readEnq = [ i for i in range(self.taskTable.shape[0]) if self.taskTable.iloc[i].status==0 ]
        taskInfo = []
        for i in self.readEnq:
            taskInfo.append([i,self.taskTable.iloc[i]])
        return taskInfo
        
    def updataTable(self, index, item, value):
        self.taskTable.loc[index,item] = value
        # print(self.taskTable)
        self.taskTable.to_excel(self.path, index=False)

    def showTable(self):
        print('\n'*2)
        print(self.taskTable)
        print('\n'*2)


def assignTask(queue):
    deviceInfo = getDeviceInfo(require='nvsmi')
    freeGPU = getFreeGPU(deviceInfo, threshold=args.GPUthreshold)
    
    # get task info
    task = queue.dequeue()
    
    # waiting a free gpu
    while len(freeGPU) < task['numGPU']:
        deviceInfo = getDeviceInfo(require='nvsmi')
        freeGPU = getFreeGPU(deviceInfo, threshold=args.GPUthreshold)
        time.sleep(args.refreshGPU)

    # get gpu id
    usedId = ''
    for i in range(task['numGPU']):
        usedId =  str(freeGPU[i])
    taskTable.updataTable(index=task['index'], item='indexGPU', value=usedId)
    taskTable.showTable()

    # change work path
    os.chdir(task['path'])

    # run cmd
    cmd = '. ~/anaconda3/etc/profile.d/conda.sh\n conda activate {}\n export CUDA_VISIBLE_DEVICES={}\n {} > {}'
    cmd = cmd.format(
                    task['condaEnv'],
                    usedId,
                    task['command'],
                    task['output']
                    )
    subprocess.Popen(cmd, shell=True, stdout=None)
    taskTable.updataTable(index=task['index'], item='status', value='2')
    # waiting task build finish
    time.sleep(args.ModelBulid)


def routine():
    taskTable.readTable('GPUWM.xlsx')
    readEnq = taskTable.checkTask()

    if readEnq == []:
        print('There are no tasks to enqueue now.')
    else:
        for task in readEnq:
            task_index  = task[0]
            task_info = task[1]
            taskQue.enqueue(task_index,
                            task_info.user, task_info.numGPU, task_info.condaEnv, 
                            task_info.path, task_info.command, task_info.outputLog,
                            task_info.indexGPU)

            taskTable.updataTable(index=task_index, item='status', value='1')

    if not taskQue.is_empty():
        assignTask(taskQue)

    taskTable.showTable()
    return True 



if __name__ == '__main__': 

    # init device and excel table
    getDeviceInfo(require='overview')
    taskQue = taskQueue()
    taskTable = taskTable()

    refresh = True
    while refresh == True:
        refresh = routine()
        # waiting byte transmission if table updated
        time.sleep(args.routine)

    # schedule.every(1).seconds.do(routine)

    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)





