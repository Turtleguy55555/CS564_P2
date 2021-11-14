/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include "buffer.h"

#include <iostream>
#include <memory>

#include "exceptions/bad_buffer_exception.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"

namespace badgerdb {

constexpr int HASHTABLE_SZ(int bufs) { return ((int)(bufs * 1.2) & -2) + 1; }






//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------
BufMgr::BufMgr(std::uint32_t bufs)
    : numBufs(bufs),
      hashTable(HASHTABLE_SZ(bufs)),
      bufDescTable(bufs),
      bufPool(bufs) {
    for (FrameId i = 0; i < bufs; i++) {
        bufDescTable[i].frameNo = i;
        bufDescTable[i].valid = false;
    }
    clockHand = bufs - 1;
}

/**
 * @brief Advance clock to next frame in the buffer pool
 * 
 */
void BufMgr::advanceClock() {
  clockHand = (clockHand + 1) % numBufs;
}

/**
 * @brief Allocate a free frame
 * 
 * @param frame frame to be allocated
 */

void BufMgr::allocBuf(FrameId& frame) {
    std::cout <<"allocBuf\n";
    advanceClock();
    // mark the starting clock hand position
    uint start = clockHand;
    // set to 0? if all clock positions have been traversed
    bool started = false;
    bool secondPass = false;
    bool temp = false;
    while(true){
        printf("\nstart: %u", start);
        printf(" clockHand: %u\n", clockHand);
        if(clockHand == start && started == true) {
            if (!secondPass) {
                printf("secondPass set to true");
                secondPass = true;
            }
        } else if (clockHand == (start + 1) % numBufs && secondPass && !temp) {
            temp = true;
            //throw BufferExceededException();
        } else if (clockHand == (start + 1) % numBufs && secondPass && temp) {
            throw BufferExceededException();
            return;
        }


        started = true;
        if(bufDescTable[clockHand].valid == true){
            // if refbit is 1 & page is valid, flip refbit to 0
           if(bufDescTable[clockHand].refbit == false) {
                // if page is pinned, skip this page
                if(bufDescTable[clockHand].pinCnt == 0) {
                    // if page is dirty & valid & unpinned, write
                    if(bufDescTable[clockHand].dirty == true) {
                        // if page is dirty, flush
                        //call set on the frame()
                        bufDescTable[clockHand].file.writePage(bufPool[clockHand]);
                        bufDescTable[clockHand].dirty = false;
                    }
                    // else, remove from buffer and continue
                    hashTable.remove(bufDescTable[clockHand].file,bufDescTable[clockHand].pageNo);
                    break;
                } else {
                    advanceClock();
                    continue;
                }
            } else {
                bufDescTable[clockHand].refbit = false;
                advanceClock();
                continue;
            }
        } else {
            break;
        }
    }
    // set frame
    bufDescTable[clockHand].Set(bufDescTable[clockHand].file,bufDescTable[clockHand].pageNo);
    frame = bufDescTable[clockHand].frameNo;
}

/**
 * @brief Reads the given page from the file into a frame and returns the pointer to page
 * If the requested page is already present in the buffer pool
 * pointer to that frame is returned, otherwise a new fame is 
 * allocated from the buffer pool for reading the page
 * @param file 
 * @param pageNo 
 * @param page 
 */
void BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {
    std::cout <<"readPage\n";
    FrameId frameNo;
    try {
        hashTable.lookup(file, pageNo, frameNo);
        bufDescTable[frameNo].refbit = true;
        bufDescTable[frameNo].pinCnt++;
        page = & bufPool[frameNo];
    } catch (const HashNotFoundException &) {
        allocBuf(frameNo);
        bufPool[frameNo] = file.readPage(pageNo);
        hashTable.insert(file, pageNo, frameNo);
        bufDescTable[frameNo].Set(file,pageNo);
        page = & bufPool[frameNo];
    }
}

void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {
    std::cout <<"unPinPage\n";
    FrameId frameNo;
    // check if page is found
    try {
        hashTable.lookup(file, pageNo, frameNo);
    } catch (const HashNotFoundException &) {
        return;
    }
    // check if pin count is already 0
    if (bufDescTable[frameNo].pinCnt <= 0) {
        throw PageNotPinnedException(file.filename(), bufDescTable[frameNo].pageNo, frameNo);
        return;
    }
    // decrement pc, set dirty
    bufDescTable[frameNo].pinCnt--;
    if (dirty == true) {
        bufDescTable[frameNo].dirty = true;
    }
}

void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {
    
    
    std::cout <<"allocPage\n";

    
    FrameId frame;
    allocBuf(frame); //get buffer pool frame
    
    
    //still need to set pageNo somehow...
    bufPool[frame] = file.allocatePage(); //allocate new page
    page = &bufPool[frame]; //set page
    pageNo = bufPool[frame].page_number();
    bufDescTable[frame].Set(file,pageNo); //set the frame
    hashTable.insert(file,pageNo,frame);//insert into hashtable
    
    
}

void BufMgr::flushFile(File& file) {
    std::cout <<"flushFile\n";
    for (uint32_t i = 0; i < numBufs; i++){
        if (bufDescTable[i].file == file) {
            // pincount exception
            if (bufDescTable[i].pinCnt > 0) {
                throw PagePinnedException(file.filename(), bufDescTable[i].pageNo, i);
            }
            // badbuffer exception
            if (bufDescTable[i].pageNo == Page::INVALID_NUMBER) {
                throw BadBufferException(i, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
            }
            // dirty
            if (bufDescTable[i].dirty == true) {
                bufDescTable[i].file.writePage(bufPool[i]);
                bufDescTable[i].dirty = false;
            }
            hashTable.remove(bufDescTable[i].file, bufDescTable[i].pageNo);
            bufDescTable[i].clear();
        }
    }
}

void BufMgr::disposePage(File& file, const PageId PageNo) {
    std::cout <<"disposePage\n";
    FrameId frameNo;
    file.deletePage(PageNo);
    try {
        hashTable.lookup(file, PageNo, frameNo);
        bufDescTable[frameNo].clear();
    } catch (const HashNotFoundException &) {
        return;
    }
    hashTable.remove(file, PageNo);
}

void BufMgr::printSelf(void) {
  std::cout <<"printSelf\n";
  int validFrames = 0;

  for (FrameId i = 0; i < numBufs; i++) {
    std::cout << "FrameNo:" << i << " ";
    bufDescTable[i].Print();

    if (bufDescTable[i].valid) validFrames++;
  }

  std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}  // namespace badgerdb
