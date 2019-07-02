/* Amie Corso, acorso
CIS 415 Project 2
This is my own work.
*/
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "diskdevice.h"
#include "diskdevice_full.h"
#include "sectordescriptor.h"
#include "sectordescriptorcreator.h"
#include "freesectordescriptorstore_full.h"
#include "voucher.h"
#include "BoundedBuffer.h"
#define BUFSIZE 10  // don't want to waste unused space
#define NUMVOUCHERS 30 // same number of SDs created

/* Forward declarations */
void blocking_write_sector(SectorDescriptor *sd, Voucher **v);
int nonblocking_write_sector(SectorDescriptor *sd, Voucher **v);
void blocking_read_sector(SectorDescriptor *sd, Voucher **v);
int nonblocking_read_sector(SectorDescriptor *sd, Voucher **v);
int redeem_voucher(Voucher *v, SectorDescriptor **sd);
void *write_to_disk(void *threadargs);
void *read_from_disk(void *threadargs);
Voucher *blocking_getfreevoucher(void);
Voucher *nonblocking_getfreevoucher(void);
void return_voucher(Voucher *v);

struct voucher {
    char type;            //determines whether this was a read ('r') or a write ('w')
    SectorDescriptor *SD; //Which SD is the voucher for?
    int complete;         //Has that SD  been processed yet?
    int successful;       //Was processing successful?
};

// Global variables
FreeSectorDescriptorStore *fsds_ptr; // for returning SDs after writing
BoundedBuffer *writebuf;  //the bounded buffer queue for SD writes
BoundedBuffer *readbuf;   //the bounded buffer queue for SD reads
Voucher vouchers[NUMVOUCHERS];  // array of voucher structs (fixed-size)
Voucher *freevouchers[NUMVOUCHERS];   // for keeping track of free vouchers (will operate like a LIFO stack)
int top = -1; // "top" of a lifo stack implemented in static array

// Mutexes and Condition variables
pthread_mutex_t wbuf_lock = PTHREAD_MUTEX_INITIALIZER; // protects the writing queue
pthread_mutex_t rbuf_lock = PTHREAD_MUTEX_INITIALIZER; // protects the reading queue
pthread_mutex_t freevouch_lock = PTHREAD_MUTEX_INITIALIZER; // protects the pile of free vouchers
pthread_mutex_t rwcompleted_lock = PTHREAD_MUTEX_INITIALIZER; // protects the voucher array while it gets updated

pthread_cond_t wbuf_changed = PTHREAD_COND_INITIALIZER; // ^^^ condition variables associated with the locks
pthread_cond_t rbuf_changed = PTHREAD_COND_INITIALIZER; // same order
pthread_cond_t freedup = PTHREAD_COND_INITIALIZER;
pthread_cond_t rwcompleted = PTHREAD_COND_INITIALIZER; 

// initializes the global data structures, creates SDs, creates threads
void init_disk_driver(DiskDevice *dd, void *mem_start, unsigned long mem_length,
		      FreeSectorDescriptorStore **fsds)
{
    //create fsds and put it in the variable for the code that called this init
    fsds_ptr = create_fsds();
    //load fsds with packet descriptors constructed from mem_start/mem_length
    create_free_sector_descriptors(fsds_ptr, mem_start, mem_length);
    *fsds = fsds_ptr; // return through fsds
    //create any buffers required by your threads
    writebuf = createBB(BUFSIZE);
    readbuf = createBB(BUFSIZE);
    // put ALL vouchers in the "Free" bucket
    int i;
    for (i = 0; i < NUMVOUCHERS; i++) {
	freevouchers[i] = &vouchers[i];
	top = i;
    }
    //create any threads required for implementation
    pthread_t writer_id, reader_id;
    // create writer thread
    if (pthread_create(&writer_id, NULL, &write_to_disk, (void *)dd) != 0) {
	fprintf(stderr, "Failed to create disk writer thread.\n");
    }
    // create reader thread
    if (pthread_create(&reader_id, NULL, &read_from_disk, (void *)dd) != 0) {
	fprintf(stderr, "Failed to create disk reader thread.\n");
    }
    // no need to join these because they should never return
}

// queues up an SD for writing - called by Apps
void blocking_write_sector(SectorDescriptor *sd, Voucher **v)
{   // we need to get a voucher first
    Voucher *voucher = blocking_getfreevoucher();
    // handles signaling of addition for us
    //fill out the voucher
    voucher->type = 'w';
    voucher->SD = sd;
    voucher->complete = 0; //incomplete
    voucher->successful = 0; //unknown success
    // once there is room in writebuf, enqueue the voucher/sd
    blockingWriteBB(writebuf, (void *)voucher);
    // return through v*
    *v = voucher;
    return;
}

// nonblocking queue up of SD for writing - called by Apps
int nonblocking_write_sector(SectorDescriptor *sd, Voucher **v)
{
// try to get a voucher, if you can, proceed, if you can't, return 0
    Voucher *voucher;
    if ((voucher = nonblocking_getfreevoucher()) == NULL) {
	return 0;
    }
    voucher->type = 'w';
    voucher->SD = sd;
    voucher->complete = 0;
    voucher->successful = 0;
    if (nonblockingWriteBB(writebuf, (void *)voucher)) {
	*v = voucher;
	return 1; // succesfully enqueued
    } 
    // if we weren't able to write, put the voucher back
    return_voucher(voucher);
    return 0;	
}

// queues up an SD for reading - called by Apps
void blocking_read_sector(SectorDescriptor *sd, Voucher **v)
{   Voucher *voucher = blocking_getfreevoucher();
    // once there is room in readbuf, enqueue the sd.
    //fill out the voucher and return through *v
    voucher->type = 'r';
    voucher->SD = sd;
    voucher->complete = 0; //incomplete
    voucher->successful = 0; //unknown success
    // handles signaling of addition for us
    blockingWriteBB(readbuf, (void *)voucher);
    *v = voucher;
    return;
}

// nonblocking - queues up an SD for reading - called by Apps
int nonblocking_read_sector(SectorDescriptor *sd, Voucher **v)
{
    // try to get a voucher, if you can, proceed, if you can't, return 0
    Voucher *voucher;
    if ((voucher = nonblocking_getfreevoucher()) == NULL) {
	return 0;
    }  // fill out the fields for this voucher
    voucher->type = 'r';
    voucher->SD = sd;
    voucher->complete = 0;
    voucher->successful = 0;
    // try to enqueue immediately
    if (nonblockingWriteBB(readbuf, (void *)voucher)) {
	*v = voucher;
	return 1; // succesfully enqueued
    } 
    // if we weren't able to write, put the voucher back
    return_voucher(voucher);
    return 0;	
}

// called by Apps to retrieve status of read/write (and SD in case of read)
int redeem_voucher(Voucher *v, SectorDescriptor **sd)
{   // return value is 1 if successful, 0 if not
    // the calling application is blocked until the read/write has completed
    // if a successful read, the associated SectorDescriptor is return in *sd
    pthread_mutex_lock(&rwcompleted_lock);// get the lock
    while (v->complete != 1) { // check the status of your voucher
	pthread_cond_wait(&rwcompleted, &rwcompleted_lock);// if it is not complete, wait on a rwcomplete
    }
    pthread_mutex_unlock(&rwcompleted_lock); // when it is complete, release the lock
    int successful = v->successful;
    if (v->type == 'w') { // was it a write??
        return_voucher(v); // don't need the sd, put the voucher back in free pool
	return successful;// return correct success indicator
    }
    else {  // or a read??
        *sd = v->SD; // return the sector descriptor in *sd
	return_voucher(v); // puts voucher pointer back in free pool
	return successful; // return the correct success indicator
    }
}

// Thread work function for pulling from writebuf and writing to disk
void *write_to_disk(void *threadargs)
{
    DiskDevice *dd = (DiskDevice *)threadargs;
    Voucher *v;
    int ddreturn;
    while (1) {
        v = (Voucher *)blockingReadBB(writebuf); // blocks until we have something to write
	SectorDescriptor *sd = v->SD;
	ddreturn = write_sector(dd, sd); // write to the disk and get the return val
	// put the SD back in the FSDS
	init_sector_descriptor(sd); // reset it to 0
	blocking_put_sd(fsds_ptr, sd); // then return it to the FSDS
	// populate voucher when it returns
	pthread_mutex_lock(&rwcompleted_lock); // get lock first, so that we are the only one modifying vouchers
	v->complete = 1;          // write has completed
	v->successful = ddreturn; // success depends on what the dd reported
	pthread_cond_broadcast(&rwcompleted); // voucher was updated!!
	pthread_mutex_unlock(&rwcompleted_lock);
    }
    return NULL;
}

// Thread work function for pulling from readbuf and getting read from disk
void *read_from_disk(void *threadargs)
{
    DiskDevice *dd = (DiskDevice *)threadargs;
    Voucher *v;
    int ddreturn;
    while (1) {
	v = (Voucher *)blockingReadBB(readbuf);
        SectorDescriptor *sd = v->SD; // pull sd* out of voucher
	ddreturn = read_sector(dd, sd); // write to the disk and get the return val
	// populate voucher when it returns
	// but need to lock the vouchers first!
	pthread_mutex_lock(&rwcompleted_lock);
	v->complete = 1;          // write has completed
	v->successful = ddreturn; // success depends on what the dd reported
	pthread_cond_broadcast(&rwcompleted);
	pthread_mutex_unlock(&rwcompleted_lock);
    }
    return NULL;
}

// for the acquisition of a free voucher, blocks while one becomes available
Voucher *blocking_getfreevoucher()
{
    //acquire the lock on the freevouchers
    pthread_mutex_lock(&freevouch_lock);
    // while there are no free vouchers..
    while (top == -1) {	// wait for a free voucher (on condition var)
	pthread_cond_wait(&freedup, &freevouch_lock);
    } // once we can have a free voucher, get the voucher...
    Voucher *v = freevouchers[top--]; // grab from top index and then decrement top
    pthread_mutex_unlock(&freevouch_lock);// release the lock on the freevouchers
    return v;
}

// for the acquisition of a free voucher, non-blocking (may fail)
Voucher *nonblocking_getfreevoucher()
{
    // acquire the lock on freevouchers
    pthread_mutex_lock(&freevouch_lock);
    if (top == -1) { // don't have any vouchers
        pthread_mutex_unlock(&freevouch_lock); // give up the lock on the free vouchers
	return NULL;
    } 
    Voucher *v = freevouchers[top--]; // grab from top index and then decrement top
    pthread_mutex_unlock(&freevouch_lock); // give up the lock on the free vouchers
    return v;
}

// to put a voucher back in the array of free vouchers
void return_voucher(Voucher *v)
{
    pthread_mutex_lock(&freevouch_lock);
    // clean out the voucher first?? redundant but safe
    v->SD = NULL;
    v->complete = 0;
    v->successful = 0;
    top++; // increment top
    freevouchers[top] = v; // put a pointer to voucher in freevouchers
    pthread_cond_broadcast(&freedup); // broadcast that there is another free voucher
    pthread_mutex_unlock(&freevouch_lock);
}

