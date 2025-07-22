### Overview

For the problem statement refer: http://nil.csail.mit.edu/6.5840/2023/labs/lab-mr.html

#### Solution

The idea here is to track progress in the coordinator using shared state variables. Workers need to operate independently on map and reduce tasks, while the coordinator, as its name suggests, handles all coordination responsibilities. To ensure we don't override the contents of intermediate files, locks and sleeping threads are necessary as well as temp files. Furthermore, we need to ensure that intermittent failures are managed by the coordinator and tasks are reassigned when necessary.

Since the filesystem resides on the same nodes where we're running the workers, coordination and failure tracking are slightly easier to manage. An added challenge would be to mimic real-life scenarios with distributed filesystems, but I am not spending money/time on that for now. 
