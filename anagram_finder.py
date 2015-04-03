#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import threading
from threading import Thread
import time

import urlparse, urllib, sys, string
from bs4 import BeautifulSoup

try:
    from mesos.native import MesosExecutorDriver, MesosSchedulerDriver
    from mesos.interface import Executor, Scheduler
    from mesos.interface import mesos_pb2
except ImportError:
    from mesos import Executor, MesosExecutorDriver, MesosSchedulerDriver, Scheduler
    import mesos_pb2

import results

class FinderExecutor(Executor):
    def registered(self, driver, executorInfo, frameworkInfo, slaveInfo):
      print "FinderExecutor registered"

    def reregistered(self, driver, slaveInfo):
      print "FinderExecutor reregistered"

    def disconnected(self, driver):
      print "FinderExecutor disconnected"

    def launchTask(self, driver, task):
        def run_task():
            print "Running finder task %s" % task.task_id.value
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_RUNNING
            driver.sendStatusUpdate(update)

            word = task.data
            url = "http://anagram-solver.net/%s" % word
            source = ""
            try:
                source = urllib.urlopen(url).read()
            except:
                error_msg = "Could not read resource at %s" % url
                update = mesos_pb2.TaskStatus()
                update.task_id.value = task.task_id.value
                update.state = mesos_pb2.TASK_FAILED
                update.message = error_msg
                update.data = url

                driver.sendStatusUpdate(update)
                print error_msg
                return

            soup = BeautifulSoup(source)

            anagrams = []
            try:
              for anagram in soup.find('ul').find_all('li'):
                  try:
                      anagram = anagram.find('a').string.lower()
                      anagram = reduce(lambda s,c: s.replace(c, ''), string.punctuation + ' ', anagram)
                      anagrams.append(anagram)
                  except:
                      print "in exception for word %s" % anagram
                      pass
            except:
              print "Could not fetch any anagrams from html"
              return

            res = results.FinderResult(
              task.task_id.value,
              word,
              anagrams
            )
            message = repr(res)
            driver.sendFrameworkMessage(message)

            print "Sending status update..."
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_FINISHED
            driver.sendStatusUpdate(update)
            print "Sent status update"
            return

        thread = threading.Thread(target=run_task)
        thread.start()

    def killTask(self, driver, taskId):
      shutdown(self, driver)

    def frameworkMessage(self, driver, message):
      print "Ignoring framework message: %s" % message

    def shutdown(self, driver):
      print "Shutting down"
      sys.exit(0)

    def error(self, error, message):
      pass

if __name__ == "__main__":
    print "Starting Launching Executor (LE)"
    driver = MesosExecutorDriver(FinderExecutor())
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)
