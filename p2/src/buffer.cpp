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
#include "exceptions/invalid_page_exception.h"
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
    advanceClock();
    // mark the starting clock hand position
    uint start = clockHand;
    // flag for whether the clock has started
    bool started = false;
    // flag for whether the clock is on the second pass
    bool secondPass = false;
    // flag for whether the clock is on the second pass + 1 buffer
    bool temp = false;
    while(true){
        // if clock has started and ran one revolution
        if(clockHand == start && started == true) {
            // if not second pass, set to true
            if (!secondPass) {
                secondPass = true;
            }
        // if clock hand ran 1 revolution + 1 buffer, set the temp flag
        } else if (clockHand == (start + 1) % numBufs && secondPass && !temp) {
            temp = true;
        // if clock hand ran 2 revolution + 1 buffer
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
                        //flushFile(bufDescTable[clockHand].file);
                        bufDescTable[clockHand].file.writePage(bufPool[clockHand]);
                        bufDescTable[clockHand].dirty = false;
                    }
                    // else, set frame, remove from buffer and continue
                    bufDescTable[clockHand].Set(bufDescTable[clockHand].file,bufDescTable[clockHand].pageNo);
                    hashTable.remove(bufDescTable[clockHand].file,bufDescTable[clockHand].pageNo);
                    break;
                } else {
                    advanceClock();
                    continue;
                }
            } else {
                // flip the refbit
                bufDescTable[clockHand].refbit = false;
                advanceClock();
                continue;
            }
        } else {
            // set frame
            bufDescTable[clockHand].Set(bufDescTable[clockHand].file,bufDescTable[clockHand].pageNo);
            break;
        }
    }
    // update frame
    frame = clockHand;
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
    FrameId frameNo;
    try {
        hashTable.lookup(file, pageNo, frameNo);
        bufDescTable[frameNo].refbit = true;
        bufDescTable[frameNo].pinCnt++;
        page = & bufPool[frameNo];
    } catch (const HashNotFoundException &) {
        try {
            file.readPage(pageNo);
        } catch (const InvalidPageException &) {
            throw InvalidPageException(pageNo, file.filename());
            return;
        }
        allocBuf(frameNo);
        bufPool[frameNo] = file.readPage(pageNo);
        hashTable.insert(file, pageNo, frameNo);
        bufDescTable[frameNo].Set(file,pageNo);
        page = & bufPool[frameNo];
    }
}

/**
 * @brief unpin a given page from the file. 
 * @param file 
 * @param pageNo 
 * @param dirty
 */
void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {
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


/**
 * @brief allocate a page to a given file 
 * @param file 
 * @param pageNo, set reference
 * @param page, set reference
 */
void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {
    FrameId frame;
    
    //still need to set pageNo somehow...
    Page newPage = file.allocatePage(); 
    pageNo = newPage.page_number();
    allocBuf(frame); //get buffer pool frame
    hashTable.insert(file,pageNo,frame);//insert into hashtable
    bufPool[frame] = newPage; //allocate new page
    bufDescTable[frame].Set(file,pageNo); //set the frame
    page = &bufPool[frame]; //set page
}

/**
 * @brief flush a file. Close all bufDescTable entries in the given file, and remove from hashTable
 * @param file 
 */
void BufMgr::flushFile(File& file) {
    for (uint32_t i = 0; i < numBufs; i++){
        if (bufDescTable[i].file == file) {
            // pincount needs to be == 0
            if (bufDescTable[i].pinCnt > 0) {
                throw PagePinnedException(file.filename(), bufDescTable[i].pageNo, i);
            }
            // badbuffer exception
            if (!bufDescTable[i].valid) {
                throw BadBufferException(i, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
            }
            // if the dirty bit is set, write back to disk first
            if (bufDescTable[i].dirty == true) {
                bufDescTable[i].file.writePage(bufPool[i]);
                bufDescTable[i].dirty = false;
            }
            hashTable.remove(bufDescTable[i].file, bufDescTable[i].pageNo);
            bufDescTable[i].clear();
        }
    }
    file.close();
}

/**
 * @brief dispose a page from a file. Remove from hashTable if needed
 * @param file 
 * @param pageNo
 */

void BufMgr::disposePage(File& file, const PageId PageNo) {
    FrameId frameNo;
    try {
        hashTable.lookup(file, PageNo, frameNo);
        hashTable.remove(file, PageNo);
        bufDescTable[frameNo].clear();
        file.deletePage(PageNo);
    } catch (const HashNotFoundException &) {
        file.deletePage(PageNo);
        return;
    }
}

void BufMgr::printSelf(void) {
  int validFrames = 0;

  for (FrameId i = 0; i < numBufs; i++) {
    std::cout << "FrameNo:" << i << " ";
    bufDescTable[i].Print();

    if (bufDescTable[i].valid) validFrames++;
  }

  std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}  // namespace badgerdb
