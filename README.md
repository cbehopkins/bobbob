# bobbob Project

Bobbob (Bunch Of Binary BlOBs) is a simple Go application that provides a binary file storage system. It allows users to create a binary file, write objects to it, and read objects from it.

Do you find yourself dealing with data structures that are larger than available memory?
Is working with these without using all your memory more important to you than performance? (i.e. are you willing to spend disk bandwidth to ensure you don't use physical memory)
Need lists that can grow to be larger than available memory?
Need a map storing lots of objects.
Don't want to implement a full database?

BunchOfBinaryBlOBs might be for you.

FWIW the name bobbob comes from when I can't think of a name for something I often use foo and bar like everyone else. But sometimes Bob seems to work better and of course [Bob was](https://galactanet.com/comic/view.php?strip=530)  [there too](https://galactanet.com/comic/view.php?strip=517)

The foundation is the Store type. At its most simple level it's trivial. A single file that you write []bytes to.
You write an []byte and get back an integer. This integer is the ObjectId - a unique value that can be used to access the bytes again in the future.

In the most trivial cases the ObjectId is the file offset that the bytes are stored to. (Told you it was a simple thing.)

The store also tracks the object locations and their sizes, policing the reads and writes so that one object does not interfere with others.

So far, so boring. But Store supports deletion of objects, so you can end up with holes in your used space.
The sensible thing to do is to therefore track those holes and rather than write new objects to the end of the file, use these holes for new objects.

Therefore the ObjectId is not actually the file offset (although it can often be).
It is in fact just a handle that we look up the actual file offset from. The file offset can change after a Free/Compact session and therefore there are safeguards in place.

Further Data structures are then built upon this foundation.
For arbitrary length lists we have Link and Chain. These work together as a doubly linked list to provide lists that are larger than can be fit into memory.

This comes in a sorted and unsorted variant. Fundamentally they are the same structure, but if one follows certain rules, you will always get a sorted list, if one follows a different set of rules it will be an unsorted list.

To be implemented is a map. I've had a lot of milage out of [gkvlite](https://github.com/steveyen/gkvlite).
I plan to steal a lot of the ideas from this project, but using the Store concept so that the File image can be compacted and doesnt continuously grow.

There is clearly much to be done...