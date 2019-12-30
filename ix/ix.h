#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"
#include "../rbf/pfm.h"
using namespace std;
# define IX_EOF (-1)  // end of the index scan

class IX_ScanIterator;

class IXFileHandle;

class IndexManager {

public:
    static IndexManager &instance();

    // Create an index file.
    RC createFile(const std::string &fileName);

    // Delete an index file.
    RC destroyFile(const std::string &fileName);

    // Open an index and return an ixFileHandle.
    RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

    // Close an ixFileHandle for an index.
    RC closeFile(IXFileHandle &ixFileHandle);

    // Insert an entry into the given index that is indicated by the given ixFileHandle.
    RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);


    // Delete an entry from the given index that is indicated by the given ixFileHandle.
    RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

    // Initialize and IX_ScanIterator to support a range search
    RC scan(IXFileHandle &ixFileHandle,
            const Attribute &attribute,
            const void *lowKey,
            const void *highKey,
            bool lowKeyInclusive,
            bool highKeyInclusive,
            IX_ScanIterator &ix_ScanIterator);

    // Print the B+ tree in pre-order (in a JSON record format)
    void printBtree(IXFileHandle &ixFileHandle, const Attribute &attribute) const;

private:
    RC compareVarchar(void *pageData, const void *key, const short int beginOffset,

                         const short int endOffset, bool &comparison, bool &equal);


    RC nonleafSearch(IXFileHandle &ixFileHandle, void *pageData, PageNum &page, char &leafIndicator,

                       const AttrType type, const void *key, vector<PageNum> &trace, bool equalflag, int &index);

    RC searchLeaf(void *pageData, const AttrType &type, const void *key, short int &moveBegin, short int &moveEnd, int &index);
    RC leafPageDelete(void *pageData, short int moveBegin, short int moveEnd, int len, int index, AttrType type);

    RC splitLeaf(IXFileHandle &ixFileHandle, void *pageData, void *newPage, AttrType type, vector<PageNum> &trace, void *midKey);


    RC splitIndex(IXFileHandle &ixFileHandle, void *pageData, void *newPage, AttrType type,

                  vector<PageNum> &trace, void *leftPageLastKey, void *rightPageFirstKey);


    RC insertLeafPage(void *pageData, const void *key, AttrType type, const RID &rid);


    RC indexPageInsert(IXFileHandle &ixFileHandle, void *pageData, const void *key, AttrType type, PageNum newPageNumber);


    RC indexPageUpdate(IXFileHandle &ixFileHandle, void *pageData, const void *key, AttrType type, void *midKey, bool leftOrRight, PageNum newPageNumber);


    RC createRoot(IXFileHandle &ixFileHandle, void *midKey, AttrType type, PageNum left, PageNum right);


    bool compare(const void *m1, void *m2, AttrType type);




    string treeSerialize(IXFileHandle &ixFileHandle, const AttrType type, PageNum root, void *pageData) const;





protected:
    IndexManager() = default;                                                   // Prevent construction
    ~IndexManager() = default;                                                  // Prevent unwanted destruction
    IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
    IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment

};
class IX_ScanIterator {

public:
    AttrType type;

// Constructor

    IX_ScanIterator();


    // Destructor

    ~IX_ScanIterator();


    //save the RID (heap file index) of each satisfied leaf entry

    vector<RID> returnedRID;

    //save the key of each satisfied leaf entry

    vector<vector<char> > returnedKey;


    // Get next matching entry

    RC getNextEntry(RID &rid, void *key);


    // Terminate index scan

    RC close();


private:

    int index;

};


class IXFileHandle {
public:

    // variables to keep counter for each operation
//    unsigned ixReadPageCounter;
//    unsigned ixWritePageCounter;
//    unsigned ixAppendPageCounter;
//    unsigned RootPage;
    FileHandle fileHandle;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();


    // Put the current counter values of associated PF FileHandles into variables
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

};

#endif
