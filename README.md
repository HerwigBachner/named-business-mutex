# Named Business Locks

A library which defines named business objects. Such an object works like a mutex to avoid in parallel 
execution of code. But it uses names instead of process or thread id's. So it's not a replacement for 
a normal mutex in a concurrency enviroment.

Inside a modern software architecture where microservices are distributed across multiple physical machines,
I would like to set up a locking mechanism that locks a named business object for a single access. 
The aim here is to protect the entire process that may run across multiple machines and services from multiple access. 
I call this objects Named Business Locks.

### Example

An ordering process over the Internet requires several steps for a user, which triggers subprocesses on the server.
The user uses a voucher code that he redeems. The voucher expires only when the order process is completed. However, 
the intermediate steps on the server already require the voucher code. It’s on the Named Business Locks to prevent 
that the user uses the voucher code for several processes at the same time.

It’s like a transaction, where only one process can be successful at the end, but I would like to point 
out the user from the very beginning that the voucher code is already in progress and can only be used once.

