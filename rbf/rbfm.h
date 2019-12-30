#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <climits>

#include "pfm.h"

// Record ID
typedef struct {
    unsigned pageNum;    // page number
    unsigned slotNum;    // slot number in the page
} RID;

// Attribute
typedef enum {
    TypeInt = 0, TypeReal, TypeVarChar
} AttrType;

typedef unsigned AttrLength;

struct Attribute {
    std::string name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum {
    EQ_OP = 0, // no condition// =
    LT_OP,      // <
    LE_OP,      // <=
    GT_OP,      // >
    GE_OP,      // >=
    NE_OP,      // !=
    NO_OP       // no condition
} CompOp;


/********************************************************************
* The scan iterator is NOT required to be implemented for Project 1 *
********************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();

class RBFM_ScanIterator {
public:


    // Never keep the results in the memory. When getNextRecord() is called,
    // a satisfying record needs to be fetched from the file.
    // "data" follows the same format as RecordBasedFileManager::insertRecord().


    FileHandle fileHandle;
    std::vector<Attribute> recordDescriptor;
    std::string conditionAttribute;
    CompOp compOp;
    const void *value;
    std::vector<std::string> attributeNames;
    RC getNextRecord(RID &rid, void *data);// { return RBFM_EOF; };


    ~RBFM_ScanIterator() = default;
    bool compare(void *data1,const void *data2, CompOp compOp,AttrType type);

    void formatData(char* record,void* data);

    RC close() {
        fileHandle.myFile.close();
        return 0;
    };

    RID curRID;
};

class RecordBasedFileManager {
public:
    static RecordBasedFileManager &instance();                          // Access to the _rbf_manager instance

    RC createFile(const std::string &fileName);                         // Create a new record-based file

    RC destroyFile(const std::string &fileName);                        // Destroy a record-based file

    RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a record-based file

    RC closeFile(FileHandle &fileHandle);                               // Close a record-based file

    //  Format of the data passed into the function is the following:
    //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
    //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
    //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
    //     Each bit represents whether each field value is null or not.
    //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
    //     If k-th bit from the left is set to 0, k-th field contains non-null values.
    //     If there are more than 8 fields, then you need to find the corresponding byte first,
    //     then find a corresponding bit inside that byte.
    //  2) Actual data is a concatenation of values of the attributes.
    //  3) For Int and Real: use 4 bytes to store the value;
    //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
    //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
    // For example, refer to the Q8 of Project 1 wiki page.

    // Insert a record into a file
    RC insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data, RID &rid);

    // Read a record identified by the given rid.
    RC readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid, void *data);

    // Print the record that is passed to this utility method.
    // This method will be mainly used for debugging/testing.
    // The format is as follows:
    // field1-name: field1-value  field2-name: field2-value ... \n
    // (e.g., age: 24  height: 6.1  salary: 9000
    //        age: NULL  height: 7.5  salary: 7500)
    RC printRecord(const std::vector<Attribute> &recordDescriptor, const void *data);

    /*****************************************************************************************************
    * IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) *
    * are NOT required to be implemented for Project 1                                                   *
    *****************************************************************************************************/
    // Delete a record identified by the given rid.
    RC deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid);

    // Assume the RID does not change after an update
    RC updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                    const RID &rid);

    // Read an attribute given its name and the rid.
    RC readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid,
                     const std::string &attributeName, void *data);

    // Scan returns an iterator to allow the caller to go through the results one by one.
    RC scan(FileHandle &fileHandle,
            const std::vector<Attribute> &recordDescriptor,
            const std::string &conditionAttribute,
            const CompOp compOp,                  // comparision type such as "<" and "="
            const void *value,                    // used in the comparison
            const std::vector<std::string> &attributeNames, // a list of projected attributes
            RBFM_ScanIterator &rbfm_ScanIterator);




protected:
    RecordBasedFileManager();                                                   // Prevent construction
    ~RecordBasedFileManager();                                                  // Prevent unwanted destruction
    RecordBasedFileManager(const RecordBasedFileManager &);                     // Prevent construction by copying
    RecordBasedFileManager &operator=(const RecordBasedFileManager &);          // Prevent assignment


private:
    PagedFileManager &pfm=PagedFileManager::instance();
    static RecordBasedFileManager *_rbf_manager;
public:
    //create page
    RC creatEmptyPage(FileHandle &fileHandle);
    //get page free space
    unsigned getPageFreeSpace(FileHandle &fileHandle, unsigned pageNum);//under assumption of adding a new slot
    //insert record to a page
    RC insertRecordToPage(FileHandle &fileHandle,unsigned pageNum,char* record,const unsigned recordSize,unsigned &slotNum);
    //get record according to slot id
    RC getRecordFromPage(const char* pageData,unsigned short slotNum,char* record);//the space of the record should be set before the function
    //data to record 带8byte metadata
    RC data2Record(const std::vector<Attribute> &recordDescriptor,const void* data,char* record,unsigned short &recordSize);
    //record to data 带带带带带带 8byte metadata
    RC record2Data(const std::vector<Attribute> &recordDescriptor,const char* record,void* data);//malloc before function
    //get basic page info
    RC getPageInfo(const void* pageData,unsigned short &freeSpaceOffset,unsigned short &numOfSlots);
    //set page Info
    RC setPageInfo(void* pageData,unsigned short freeSpaceOffset, unsigned short numOfSlots);
    //get slot info according to slot number
    RC getSlotInfo(const void* pageData,unsigned short slotNum,unsigned short &recordLength,unsigned short &recordOffset);
    //set Slot Info
    RC setSlotInfo(void* pageData,unsigned short slotNum,unsigned short recordLength, unsigned short recordOffset);
    //create new slot
    RC createNewSlot(void* pageData,unsigned short recordLength,unsigned short recordOffset,unsigned numOfSlots);
    //move record trunc left for some bytes  [start,end)
    RC lmoveRecordTrunc(void* pageData,unsigned short start, unsigned short end, unsigned short bytesToMove);
    //updateSlotsOffsetAfterDelete update offset to be offset-len, where offset > minOffset
    RC updateSlotsOffsetAfterDelete(void* pageData,unsigned short slotCount, unsigned short minOffset, unsigned short len);

    //record header will be read and removed record type: 0->linkto 1->linkfrom
    // 2->both to and from,in this case the rid is pointer to child
    // -1=normal reocrd
    // 0-> 0x0000
    // 1-> 0xffffprint
    // 2-> 0x00ff
    void handleRecordHeader(char* record,int &recordtype,RID &linkrid);

    void setRecordType(char* record,int recordType,RID linkrid);

    //get the real rid given rid,  recordType return the type of current Node
    //0->linkto 1->linkfrom
    // 2->both to and from,in this case the rid is pointer to child
    // -1=normal reocrd
    RID getRealRID(FileHandle &fileHandle,const std::vector<Attribute> &recordDescriptor, const RID &rid, int &reocrdType);

    RC insertRecordToPageGivenRID(FileHandle &fileHandle,unsigned pageNum,char* record,const unsigned recordSize,RID rid);
};

#endif