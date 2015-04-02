#!/usr/bin/env python

from collections import deque
import json
import os
import signal
import sys
import time
import datetime
from threading import Thread

try:
    from mesos.native import MesosExecutorDriver, MesosSchedulerDriver
    from mesos.interface import Executor, Scheduler
    from mesos.interface import mesos_pb2
except ImportError:
    from mesos import Executor, MesosExecutorDriver, MesosSchedulerDriver, Scheduler
    import mesos_pb2

import results
import task_state
import export_dot

TASK_CPUS = 0.1
TASK_MEM = 32
SHUTDOWN_TIMEOUT = 30  # in seconds
LEADING_ZEROS_COUNT = 5  # appended to task ID to facilitate lexicographical order
TASK_ATTEMPTS = 5  # how many times a task is attempted

FINDER_TASK_SUFFIX = "-fndr"
DEFINER_TASK_SUFFIX = "-rndr"

# See the Mesos Framework Development Guide:
# http://mesos.apache.org/documentation/latest/app-framework-development-guide

class RenderingFinder(Scheduler):
    def __init__(self, seedWord, maxDefinerTasks, finderExecutor, definerExecutor):
        print "ANAGRAMMER"
        print "======="
        print "seedWord: [%s]\n" % seedWord
        self.seedWord = seedWord
        self.finderExecutor  = finderExecutor
        self.definerExecutor = definerExecutor
        self.finderQueue = deque([seedWord])
        self.definerQueue = deque([seedWord])
        self.processedURLs = set([seedWord])
        self.finderResults = set([])
        self.definerResults = {}
        self.tasksCreated  = 0
        self.tasksRunning = 0
        self.tasksFailed = 0
        self.tasksRetrying = {}
        self.definerLimitReached = False
        self.maxDefinerTasks = maxDefinerTasks
        self.shuttingDown = False

    def registered(self, driver, frameworkId, masterInfo):
        print "Registered with framework ID [%s]" % frameworkId.value

    def makeTaskPrototype(self, offer):
        task = mesos_pb2.TaskInfo()
        tid = self.tasksCreated
        self.tasksCreated += 1
        task.task_id.value = str(tid).zfill(LEADING_ZEROS_COUNT)
        task.slave_id.value = offer.slave_id.value
        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = TASK_CPUS
        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = TASK_MEM
        return task

    def makeFinderTask(self, word, offer):
        task = self.makeTaskPrototype(offer)
        task.name = "finder task %s" % task.task_id.value
        task.task_id.value += FINDER_TASK_SUFFIX
        task.executor.MergeFrom(self.finderExecutor)
        task.data = str(word)
        return task

    def makeDefinerTask(self, url, offer):
        task = self.makeTaskPrototype(offer)
        task.name = "definer task %s" % task.task_id.value
        task.task_id.value += DEFINER_TASK_SUFFIX
        task.executor.MergeFrom(self.definerExecutor)
        task.data = str(url)
        return task

    def retryTask(self, task_id, url):
        if not url in self.tasksRetrying:
            self.tasksRetrying[url] = 1

        if self.tasksRetrying[url] < TASK_ATTEMPTS:
            self.tasksRetrying[url] += 1
            ordinal = lambda n: "%d%s" % (n, \
              "tsnrhtdd"[(n / 10 % 10 != 1) * (n % 10 < 4) * n % 10::4])
            print "%s try for \"%s\"" % \
              (ordinal(self.tasksRetrying[url]), url)

            # TODO(alex): replace this by checking TaskStatus.executor_id,
            # which is available in mesos 0.20
            if task_id.endswith(FINDER_TASK_SUFFIX):
              self.finderQueue.append(url)
            elif task_id.endswith(DEFINER_TASK_SUFFIX):
              self.definerQueue.append(url)
        else:
            self.tasksFailed += 1
            print "Task for \"%s\" cannot be completed, attempt limit reached" % url

    def printStatistics(self):
        print "Queue length: %d finder, %d definer; Tasks: %d running, %d failed" % (
          len(self.finderQueue), len(self.definerQueue), self.tasksRunning, self.tasksFailed
        )

    def maxTasksForOffer(self, offer):
        count = 0
        cpus = next(rsc.scalar.value for rsc in offer.resources if rsc.name == "cpus")
        mem = next(rsc.scalar.value for rsc in offer.resources if rsc.name == "mem")
        while cpus >= TASK_CPUS and mem >= TASK_MEM:
            count += 1
            cpus -= TASK_CPUS
            mem -= TASK_MEM
        return count

    def resourceOffers(self, driver, offers):
        self.printStatistics()

        if not self.finderQueue and not self.definerQueue and self.tasksRunning <= 0:
            print "Nothing to do, ANAGRAMMER is shutting down"
            hard_shutdown()

        for offer in offers:
            print "Got resource offer [%s]" % offer.id.value

            if self.shuttingDown:
                print "Shutting down: declining offer on [%s]" % offer.hostname
                driver.declineOffer(offer.id)
                continue

            maxTasks = self.maxTasksForOffer(offer)

            print "maxTasksForOffer: [%d]" % maxTasks

            tasks = []

            for i in range(maxTasks / 2):
                if self.finderQueue:
                    finderTaskWord = self.finderQueue.popleft()
                    task = self.makeFinderTask(finderTaskWord, offer)
                    tasks.append(task)
                if self.definerQueue:
                    definerTaskWord = self.definerQueue.popleft()
                    task = self.makeDefinerTask(definerTaskWord, offer)
                    tasks.append(task)

            if tasks:
                print "Accepting offer on [%s]" % offer.hostname
                driver.launchTasks(offer.id, tasks)
            else:
                print "Declining offer on [%s]" % offer.hostname
                driver.declineOffer(offer.id)

    def statusUpdate(self, driver, update):
        stateName = task_state.nameFor[update.state]
        print "Task [%s] is in state [%s]" % (update.task_id.value, stateName)

        if update.state == 1:  # Running
            self.tasksRunning += 1

        elif update.state == 3:  # Failed, retry
            print "Task [%s] failed with message \"%s\"" \
              % (update.task_id.value, update.message)
            self.tasksRunning -= 1
            if update.message != "Not a real word":
                self.retryTask(update.task_id.value, update.data)

        elif self.tasksRunning > 0 and update.state > 1: # Terminal state
            self.tasksRunning -= 1

    def frameworkMessage(self, driver, executorId, slaveId, message):
        o = json.loads(message)

        if executorId.value == finderExecutor.executor_id.value:
            result = results.FinderResult(o['taskId'], o['word'], o['anagrams'])
            for anagram in result.anagrams:
                edge = (result.word, anagram)
                print "Appending [%s] to finder results" % repr(edge)
                self.finderResults.add(edge)
                if not self.definerLimitReached and self.maxDefinerTasks > 0 and \
                  self.maxDefinerTasks <= len(self.processedURLs):
                    print "Definer task limit (%d) reached" % self.maxDefinerTasks
                    self.definerLimitReached = True
                if not anagram in self.processedURLs and not self.definerLimitReached:
                    print "Enqueueing [%s]" % anagram
                    self.definerQueue.append(anagram)
                    self.processedURLs.add(anagram)

        elif executorId.value == definerExecutor.executor_id.value:
            result = results.DefinerResult(o['taskId'], o['word'], o['definition'])
            print "Appending [%s] to definer results" % repr((result.word, result.definition))
            self.definerResults[result.word] = result.definition

def hard_shutdown():
    driver.stop()

def graceful_shutdown(signal, frame):
    print "anagrammer is shutting down"
    anagrammer.shuttingDown = True

    wait_started = datetime.datetime.now()
    while (anagrammer.tasksRunning > 0) and \
      (SHUTDOWN_TIMEOUT > (datetime.datetime.now() - wait_started).total_seconds()):
        time.sleep(1)

    if (anagrammer.tasksRunning > 0):
        print "Shutdown by timeout, %d task(s) have not completed" % anagrammer.tasksRunning

    hard_shutdown()

#
# Execution entry point:
#
if __name__ == "__main__":
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print "Usage: %s seedWord mesosMasterUrl [maxDefinerTasks]" % sys.argv[0]
        sys.exit(1)

    baseURI = "/home/vagrant/sandbox/mesosphere/ANAGRAMMER"
    suffixURI = "hostfiles"
    uris = [ "anagram_finder.py",
             "export_dot.py",
             "definer_executor.py",
             "results.py",
             "task_state.py" ]
    uris = [os.path.join(baseURI, suffixURI, uri) for uri in uris]

    finderExecutor = mesos_pb2.ExecutorInfo()
    finderExecutor.executor_id.value = "anagram-finder"
    finderExecutor.command.value = "python anagram_finder.py"

    for uri in uris:
        uri_proto = finderExecutor.command.uris.add()
        uri_proto.value = uri
        uri_proto.extract = False

    finderExecutor.name = "Finder"

    definerExecutor = mesos_pb2.ExecutorInfo()
    definerExecutor.executor_id.value = "definer-executor"
    definerExecutor.command.value = "python definer_executor.py --local"

    for uri in uris:
        uri_proto = definerExecutor.command.uris.add()
        uri_proto.value = uri
        uri_proto.extract = False

    definerExecutor.name = "Definer"

    framework = mesos_pb2.FrameworkInfo()
    framework.user = "" # Have Mesos fill in the current user.
    framework.name = "anagrammer"

    try: maxDefinerTasks = int(sys.argv[3])
    except: maxDefinerTasks = 0
    anagrammer = RenderingFinder(sys.argv[1], maxDefinerTasks, finderExecutor, definerExecutor)

    driver = MesosSchedulerDriver(anagrammer, framework, sys.argv[2])

    # driver.run() blocks; we run it in a separate thread
    def run_driver_async():
        status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1
        driver.stop()
        sys.exit(status)
    framework_thread = Thread(target = run_driver_async, args = ())
    framework_thread.start()

    print "(Listening for Ctrl-C)"
    signal.signal(signal.SIGINT, graceful_shutdown)
    while framework_thread.is_alive():
        time.sleep(1)

    export_dot.dot(anagrammer.finderResults, anagrammer.definerResults, "result.dot")
    print "Goodbye!"
    sys.exit(0)
