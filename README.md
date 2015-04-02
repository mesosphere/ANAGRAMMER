ANAGRAMMER :interrobang:
=====================

An anagram finder framework for [Apache Mesos](http://mesos.apache.org/).

See the [accompanying slides](http://mesosphere.github.io/oscon-mesos-2014/#/) for more context.

ANAGRAMMER consists of three main components:

- `CrawlExecutor` extends `mesos.Executor`
- `RenderExecutor` extends `mesos.Executor`
- `RenderingCrawler` extends `mesos.Scheduler` and launches tasks with the executors

## Quick Start with Vagrant

### Requirements

- [VirtualBox](http://www.virtualbox.org/) 4.1.18+
- [Vagrant](http://www.vagrantup.com/) 1.3+
- [git](http://git-scm.com/downloads) (command line tool)

### Start the `mesos-demo` VM

```bash
$ wget http://downloads.mesosphere.io/demo/mesoscon.box -O /tmp/mesoscon.box
$ vagrant box add --name mesos-demo /tmp/mesoscon.box
$ git clone https://github.com/mesosphere/ANAGRAMMER.git
$ cd ANAGRAMMER
$ vagrant up
```

Now that the VM is running, you can view the Mesos Web UI here:
[http://10.141.141.10:5050](http://10.141.141.10:5050)

You can see that 1 slave is registered and you've got some idle CPUs and Memory. So let's start the ANAGRAMMER!

### Run ANAGRAMMER in the `mesos-demo` VM
```bash
$ vagrant ssh
vagrant@mesos:~ $ cd sandbox/mesosphere/ANAGRAMMER/hostfiles
# Start the scheduler with the seed word, the mesos master ip and optionally a task limit
vagrant@mesos:hostfiles $ python anagrammer.py word 127.0.1.1:5050 42
# <Ctrl+C> to stop..

### Shutting down the `mesos-demo` VM

```bash
# Exit out of the VM
vagrant@mesos:hostfiles $ exit
# Stop the VM
$ vagrant halt
# To delete all traces of the vagrant machine
$ vagrant destroy
```

## Anagrammer Architecture (Follows RENDLER framework)

### Crawl Executor

- Interprets incoming tasks' `task.data` field as a word
- Fetches the anagrams for that word and extracts them from the document
- Sends a framework message to the scheduler containing the crawl result.

### Render Executor

- Interprets incoming tasks' `task.data` field as a word
- Fetches the anagram, and find the definition of it, if it's a real word
- Sends a framework message to the scheduler containing the render result.

### Intermediate Data Structures

We define some common data types to facilitate communication between the scheduler
and the executors.  Their default representation is JSON.

```python
results.CrawlResult(
    "1234",                                 # taskId
    "foo",                                  # word
    ["foo", "oof"]                          # anagrams
)
```

```python
results.RenderResult(
    "1234",                                 # taskId
    "foo",                                  # word
    "definition of foo"                     # definition
)
```

### Anagrammer Scheduler

#### Data Structures

- `crawlQueue`: list of words
- `renderQueue`: list of words
- `processedwords`: set or words
- `crawlResults`: list of word tuples
- `renderResults`: map of words to definitions

#### Scheduler Behavior

The scheduler accepts one word as a command-line parameter to seed the render
and crawl queues.

1. For each word, create a task in both the render queue and the crawl queue.

1. Upon receipt of a crawl result, add an element to the crawl results
   adjacency list.  Append to the render and crawl queues each word that is
   _not_ present in the set of processed words.  Add these enqueued words to
   the set of processed words.

1. Upon receipt of a render result, add an element to the render results map.

1. The crawl and render queues are drained in FCFS order at a rate determined
   by the resource offer stream.  When the queues are empty, the scheduler
   declines resource offers to make them available to other frameworks running
   on the cluster.

