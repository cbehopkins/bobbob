I liked your idea of flushing behind yourself if keep in memory was false. i.e. if keep in memory is false then once you have visited the node (and if it is persisted) then remove it from the in memory treap. (i assume this would be as simple as calling Flush on the Node?) This is a good property for my use cases that if anything the memory usage will reduce after a walk. Could you please try and re-implement this.

Now the next step is to get the alternate Disk based iterator working.
Please (for now - while we are sketching this out) create this as an entirely seperate method that is called when we WalkInOder (i.e. the mode switch causes walk in order to call a different function/method that does the walk.).
What this method will do (for now) is first run Persist so that we can assume all nodes are on disk.
Please use this to prove that we have a reliable way of iterating the entire heap without loading the entirety of it into memory.

Finally We are now ready for the last piece: the default behaviour. It will use the In Memory walker to walk the in memory nodes. If it hits a node that is not in memory, then it will use the disk based iterator to walk that part of the tree. i.e. the In Memory iterator will yield from the DiskBased iterator. At come point the disk based iterator will be exhausted as that part of the tree will have been fully walked We resume the in memory iterator again.
This way we have the best of both worlds while not complicating the memory based iterator with the disk based concerns. Does that make sense?
