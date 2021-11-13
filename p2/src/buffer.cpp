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
    // mark the starting clock hand position
    uint start = clockHand;
    // set to 0? if all clock positions have been traversed
    int flag = 1;
    while(true){
        if(clockHand == start && flag == 0) {
            throw BufferExceededException();
        }
        flag = 0;
        if(bufDescTable[clockHand].valid == true){
            // if refbit is 1 & page is valid, flip refbit to 0
            if(bufDescTable[clockHand].refbit == 0) {
                // if page is pinned, skip this page
                if(bufDescTable[clockHand].pinCnt == 0) {
                    // if page is dirty & valid & unpinned, write
                    if(bufDescTable[clockHand].dirty == true) {
                        // Simon TODO 
                        // if page is dirty, flush
                        //call set on the frame()
                        // TODO uncaught exception
                        bufDescTable[clockHand].file.writePage(bufPool[clockHand]);
                        try {
                            hashTable.remove(bufDescTable[clockHand].file,bufDescTable[clockHand].pageNo);
                        } catch (const HashNotFoundException &) {
                            // shouldn't have been here but here we are
                            printf("\n\nstarted from the top but now are here\n\n");
                        }
                    }
                    // else, simply continue
                    break;
                } else {
                    advanceClock();
                    continue;
                }
            } else {
                bufDescTable[clockHand].refbit = 1;
                advanceClock();
                continue;
            }
        } else {
            break;
        }
    }
    //std::cout<<"frames\n";
    bufDescTable[clockHand].Set(bufDescTable[clockHand].file,bufDescTable[clockHand].pageNo);
    frame = clockHand;
    //use frame:
    //remove if theres a valid page:
}

/**
void BufMgr::allocBuf(FrameId& frame) {
    std::cout <<"allocBuf\n";
    // mark the starting clock hand position
    uint start = clockHand;
    // set to 0? if all clock positions have been traversed
    int flag = 1;
    while(true){
        if(clockHand == start && flag == 0) {
            throw BufferExceededException();
        }
        flag = 0;
        if(bufDescTable[clockHand].valid == true){
            // if refbit is 1 & page is valid, flip refbit to 0
            if(bufDescTable[clockHand].refbit == 1) {
                bufDescTable[clockHand].refbit = 0;
                advanceClock();
                continue;
            }
            // if page is pinned, skip this page
            else if(bufDescTable[clockHand].pinCnt > 0) {
                advanceClock();
                continue;
            }
            // if page is dirty & valid & unpinned, write
            else if(bufDescTable[clockHand].dirty == true) {
                //flush page to disk
                //call set() on the frame
                // Simon TODO 
                // original hash remove, wrong
                bufDescTable[clockHand].Set(bufDescTable[clockHand].file,bufDescTable[clockHand].pageNo);
                break;
            }else{
                //call set on the frame()
                // TODO uncaught exception
                hashTable.remove(bufDescTable[clockHand].file,bufDescTable[clockHand].pageNo);
                bufDescTable[clockHand].Set(bufDescTable[clockHand].file,bufDescTable[clockHand].pageNo);
                break;
            }
        } else {
            bufDescTable[clockHand].Set(bufDescTable[clockHand].file,bufDescTable[clockHand].pageNo);
            break;
        }
    }
    //std::cout<<"frames\n";
    frame = bufDescTable[clockHand].frameNo;
    //use frame:
    //remove if theres a valid page:
}
*/

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
        bufDescTable[clockHand].refbit = 1;
        bufDescTable[frameNo].pinCnt++;
        page = & bufPool[frameNo];
    } catch (const HashNotFoundException &) {
        allocBuf(frameNo);
        file.readPage(pageNo);
        hashTable.insert(file, pageNo, frameNo);
        bufDescTable[clockHand].Set(file,pageNo);
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
    
    
    bufDescTable[clockHand].Set(file,pageNo); //set the frame
    //still need to set pageNo somehow...
    bufPool[frame] = file.allocatePage(); //allocate new page
    page = &bufPool[frame]; //set page
    pageNo = bufPool[frame].page_number();
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
                hashTable.remove(bufDescTable[i].file, bufDescTable[i].pageNo);
                bufDescTable[i].clear();
                bufDescTable[i].dirty = false;
            }
        }
    }
}

void BufMgr::disposePage(File& file, const PageId PageNo) {
    std::cout <<"disposePage\n";
    FrameId frameNo;
    file.deletePage(PageNo);
    try {
        hashTable.lookup(file, PageNo, frameNo);
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
