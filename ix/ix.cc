#include "ix.h"
#include<iostream>
#include<cstring>
using namespace std;

IndexManager &IndexManager::instance() {
    static IndexManager _index_manager = IndexManager();
    return _index_manager;
}

RC IndexManager::createFile(const std::string &fileName) {

    return PagedFileManager::instance().createFile(fileName);
}

RC IndexManager::destroyFile(const std::string &fileName) {
    return PagedFileManager::instance().destroyFile(fileName);
}

RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {

    return PagedFileManager::instance().openFile(fileName,ixFileHandle.fileHandle);



}

RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
    return PagedFileManager::instance().closeFile(ixFileHandle.fileHandle);
}

RC IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
    int rootpage=0;
    //handle the init when no page
    if(ixFileHandle.fileHandle.getNumberOfPages() == 0){
        ixFileHandle.fileHandle.RootPage=rootpage;
        void* leafPage= malloc(4096);
        char leafIndicator='\xFF';
        short int nodeCount=0;
        short int freeSpaceCount=4096-5-4;
        int nextPage=-1;
        memcpy(leafPage, &leafIndicator, 1);
        memcpy((char *)leafPage + 1, &nodeCount, 2);
        memcpy((char *)leafPage + 3, &freeSpaceCount, 2);
        memcpy((char *)leafPage + 4092, &nextPage, 4);
        insertLeafPage(leafPage, key, attribute.type, rid);
        ixFileHandle.fileHandle.appendPage(leafPage);
        free(leafPage);
        return 0;
    }
    vector<PageNum> pageTrack;

    void* pageData=malloc(4096);
    //only one leaf page
    PageNum page=0;
    if(ixFileHandle.fileHandle.getNumberOfPages()==1){
        page=0;
        pageTrack.push_back(0);
        ixFileHandle.fileHandle.readPage(0,pageData);
    }else{
        PageNum root=ixFileHandle.fileHandle.RootPage;
        pageTrack.push_back(root);
        ixFileHandle.fileHandle.readPage(root,pageData);
        char leafIndicator=*(char*)pageData;
        while(leafIndicator == '\x00'){

            int index;
            nonleafSearch(ixFileHandle, pageData, page, leafIndicator, attribute.type, key, pageTrack, true, index);
        }
    }

    if(attribute.type==TypeVarChar){

        int keyLen=*(int*)(char*)key;
        if(*(short int *)((char*)pageData+3)>=keyLen+ 8+2){
            insertLeafPage(pageData,key,attribute.type,rid);
            ixFileHandle.fileHandle.writePage(page,pageData);
            free(pageData);
            return 0;
        }
        //the current leafpage is full, split page
        void *newPage = malloc(4096);
        void *midKey = malloc(4096);
        void *leftKey = malloc(4096);
        void *rightKey = malloc(4096);

        splitLeaf(ixFileHandle,pageData,newPage,attribute.type,pageTrack,midKey);

        if(compare(key,midKey,attribute.type)){//key>=midKey
            insertLeafPage(newPage,key,attribute.type,rid);
        }else{
            //insert into the left
            insertLeafPage(pageData,key,attribute.type,rid);
        }

        ixFileHandle.fileHandle.writePage(page,pageData);
        ixFileHandle.fileHandle.appendPage(newPage);
        PageNum newPageNum=ixFileHandle.fileHandle.getNumberOfPages()-1;
        if(pageTrack.size()==0){
            createRoot(ixFileHandle,midKey,attribute.type,page,newPageNum);
        }else{
            page=pageTrack.back();
            ixFileHandle.fileHandle.readPage(page,pageData);
            short int freeSpace=*(short int *)((char *)pageData + 3);
            int midKeyLen=*(int*)midKey;
            if(midKeyLen+6 <= freeSpace){
                indexPageInsert(ixFileHandle, pageData, midKey, attribute.type, newPageNum);
                ixFileHandle.fileHandle.writePage(page, pageData);
            }else{
                while (midKeyLen + 6 > freeSpace){
                    
                    splitIndex(ixFileHandle,pageData,newPage,attribute.type,pageTrack,leftKey,rightKey);
                    if (!compare(midKey, leftKey, attribute.type)) {//midkey < left
                        indexPageUpdate(ixFileHandle, pageData, midKey, attribute.type, midKey, true, newPageNum);
                    }

                    else if (compare(key, rightKey, attribute.type)) {//midkey >=right
                        indexPageUpdate(ixFileHandle, newPage, midKey, attribute.type, midKey, false, newPageNum);
                    }

                    else {

                    }

                    ixFileHandle.fileHandle.writePage(page,pageData);
                    ixFileHandle.fileHandle.appendPage(newPage);
                    if (pageTrack.empty()) {

                        createRoot(ixFileHandle, midKey, attribute.type, page, ixFileHandle.fileHandle.getNumberOfPages()-1);
                        break;
                    }
                    else {
                        ixFileHandle.fileHandle.readPage(page, pageData);
                        pageTrack.pop_back();
                        freeSpace = *(short int *)((char *)pageData + 3);
                        if (midKeyLen + 6 <= freeSpace) {
                            // Insert the new Key in the index page

                            indexPageInsert(ixFileHandle, pageData, midKey, attribute.type, ixFileHandle.fileHandle.getNumberOfPages()-1);
                            ixFileHandle.fileHandle.writePage(page, pageData);
                            break;
                        }
                    }
                }
            }
        }
        

        free(newPage);
        free(midKey);
        free(leftKey);
        free(rightKey);

    }else{
        //type float and int

        if(*(short int *)((char*)pageData+3)>= 12){
            insertLeafPage(pageData,key,attribute.type,rid);
            ixFileHandle.fileHandle.writePage(page,pageData);
            free(pageData);
            return 0;
        }

        //the current leafpage is full, split page
        void *newPage = malloc(4096);
        memset(newPage, 0, 4096);
        void *midKey = malloc(4096);
        void *leftKey = malloc(4096);
        void *rightKey = malloc(4096);

        splitLeaf(ixFileHandle,pageData,newPage,attribute.type,pageTrack,midKey);
        PageNum newPageNum = ixFileHandle.fileHandle.getNumberOfPages();

        if(compare(key,midKey,attribute.type)){//key>=midKey
            insertLeafPage(newPage,key,attribute.type,rid);
        }else{
            //insert into the left
            insertLeafPage(pageData,key,attribute.type,rid);
        }

        ixFileHandle.fileHandle.writePage(page,pageData);
        ixFileHandle.fileHandle.appendPage(newPage);
        if(pageTrack.size()==0){
            createRoot(ixFileHandle,midKey,attribute.type,page,newPageNum);
        }else{
            PageNum page=pageTrack.back();
            ixFileHandle.fileHandle.readPage(page,pageData);
            short int freeSpace=*(short int *)((char *)pageData + 3);
            PageNum oldIndexPageNum = page;
            if(freeSpace>=8){

                indexPageInsert(ixFileHandle, pageData, midKey, attribute.type, newPageNum);
                ixFileHandle.fileHandle.writePage(page, pageData);
            }else{
                while (freeSpace<8){
                    oldIndexPageNum = page;
                    void* tmpkey=malloc(4096);
                    memcpy(tmpkey, midKey, 4);
                    splitIndex(ixFileHandle,pageData,newPage,attribute.type,pageTrack,leftKey,rightKey);
                    if (!compare(tmpkey, leftKey, attribute.type)) {//midkey < left
                        indexPageUpdate(ixFileHandle, pageData, tmpkey, attribute.type, midKey, true, newPageNum);
                    }

                    else if (compare(tmpkey, rightKey, attribute.type)) {//midkey >=right
                        indexPageUpdate(ixFileHandle, newPage, tmpkey, attribute.type, midKey, false, newPageNum);
                    }

                    else {
                        memcpy(midKey, tmpkey, 4);
                    }

                    ixFileHandle.fileHandle.writePage(page,pageData);
                    ixFileHandle.fileHandle.appendPage(newPage);
                    if (pageTrack.empty()) {

                        createRoot(ixFileHandle, midKey, attribute.type,oldIndexPageNum, ixFileHandle.fileHandle.getNumberOfPages()-1);
                        break;
                    }
                    else {
                        page=pageTrack.back();
                        ixFileHandle.fileHandle.readPage(page, pageData);
                        pageTrack.pop_back();
                        freeSpace = *(short int *)((char *)pageData + 3);
                        if (freeSpace>=8) {
                            indexPageInsert(ixFileHandle, pageData, midKey, attribute.type, ixFileHandle.fileHandle.getNumberOfPages()-1);
                            ixFileHandle.fileHandle.writePage(page, pageData);
                            break;
                        }
                    }
                }
            }
        }
        

        free(newPage);
        free(midKey);
        free(leftKey);
        free(rightKey);
    }

    free(pageData);
    return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    void *pageData = malloc(4096);
    memset(pageData, 0, 4096);
    int root = 0, index = 0;
    short int moveBeginIndex, moveEndIndex;
    PageNum page;
    if (ixFileHandle.fileHandle.getNumberOfPages() == 0) {
        free(pageData);
        return -1;
    }
    else {
        if (ixFileHandle.fileHandle.getNumberOfPages() != 1) {
            root=ixFileHandle.fileHandle.RootPage;
            ixFileHandle.fileHandle.readPage(root, pageData);
            char leafIndicator = '\x00';
            int index;
            vector<PageNum> trace;
            while (leafIndicator != '\xFF') {
                nonleafSearch(ixFileHandle, pageData, page, leafIndicator, attribute.type, key, trace, true, index);
            }
        }
        else {
            page = 0;
        }
        ixFileHandle.fileHandle.readPage(page, pageData);
        short int NodesNum = *(short int *)((char *)pageData + 1);
        searchLeaf(pageData, attribute.type, key, moveBeginIndex, moveEndIndex, index);
        if (moveBeginIndex == moveEndIndex) {
            free(pageData);
            return -1;
        }
        if (attribute.type == TypeVarChar) {
            short int end = *(short int *)((char *)pageData + 4090 - 2 * index) - 8;
            void *nextString = malloc(end - moveBeginIndex);
            memcpy(nextString, (char *)pageData + moveBeginIndex, end - moveBeginIndex);
            if (memcmp(nextString, (char *)key + 4, end - moveBeginIndex) != 0) {
                free(pageData);
                free(nextString);
                return -1;
            }
            int Length = end - moveBeginIndex;
            unsigned int pageNum, slotNum;
            pageNum = *(unsigned int *)((char *)pageData + end);
            slotNum = *(unsigned int *)((char *)pageData + end + 4);
            while (pageNum != rid.pageNum || slotNum != rid.slotNum) {
                index ++;
                if (index >= NodesNum) {
                    free(pageData);
                    free(nextString);
                    return -1;
                }
                moveBeginIndex = end + 8;
                end = *(short int *)((char *)pageData + 4090 - 2 * index);
                if (end - moveBeginIndex != Length) {
                    free(pageData);
                    free(nextString);
                    return -1;
                }
                memcpy(nextString, (char *)pageData + moveBeginIndex, end - moveBeginIndex);
                if (memcmp(nextString, (char *)key + 4, end - moveBeginIndex) != 0) {
                    free(pageData);
                    free(nextString);
                    return -1;
                }
                pageNum = *(unsigned int *)((char *)pageData + end);
                slotNum = *(unsigned int *)((char *)pageData + end + 4);
            }
            leafPageDelete(pageData, end + 8, moveEndIndex, Length, index, attribute.type);
            free(nextString);
        }
        else {
            void *nextKey = malloc(4);
            memcpy(nextKey, (char *)pageData + moveBeginIndex, 4);
            if (memcmp(nextKey, key, 4) != 0) {
                free(pageData);
                free(nextKey);
                return -1;
            }
            unsigned int pageNum, slotNum;
            pageNum = *(unsigned int *)((char *)pageData + moveBeginIndex + 4);
            slotNum = *(unsigned int *)((char *)pageData + moveBeginIndex + 8);
            while (pageNum != rid.pageNum || slotNum != rid.slotNum) {
                index ++;
                if (index >= NodesNum) {
                    free(pageData);
                    free(nextKey);
                    return -1;
                }
                moveBeginIndex += 12;
                memcpy(nextKey, (char *)pageData + moveBeginIndex, 4);
                if (memcmp(nextKey, key, 4) != 0) {
                    free(pageData);
                    free(nextKey);
                    return -1;
                }
                pageNum = *(unsigned int *)((char *)pageData + moveBeginIndex + 4);
                slotNum = *(unsigned int *)((char *)pageData + moveBeginIndex + 8);
            }
            leafPageDelete(pageData, moveBeginIndex + 12, moveEndIndex, 4, index, attribute.type);
            free(nextKey);
        }
        ixFileHandle.fileHandle.writePage(page, pageData);
    }
    free(pageData);
    return 0;
}

RC IndexManager::leafPageDelete(void *pageData, short int moveBeginIndex, short int moveEndIndex, int Length, int index, AttrType type)
{
    short int freePages = *(short int *)((char *)pageData + 3);
    short int NodesNum = *(short int *)((char *)pageData + 1);
    if (moveBeginIndex == moveEndIndex) {
        memset((char *)pageData + moveBeginIndex - 8 - Length, 0, 8 + Length);
        if (type == TypeVarChar)
            memset((char *)pageData + 4092 - 2 * NodesNum, 0, 2);
    }
    else {
        void *data = malloc(moveEndIndex - moveBeginIndex);
        memcpy(data, (char *)pageData + moveBeginIndex, moveEndIndex - moveBeginIndex);
        memcpy((char *)pageData + moveBeginIndex - 8 - Length, data, moveEndIndex - moveBeginIndex);
        free(data);
        if (type == TypeVarChar) {
            moveBeginIndex = 4092 - 2 * NodesNum;
            moveEndIndex = 4090 - 2 * index;
            void *directory = malloc(moveEndIndex - moveBeginIndex);
            memcpy(directory, (char *)pageData + moveBeginIndex, moveEndIndex - moveBeginIndex);
            memset((char *)pageData + moveBeginIndex, 0, moveEndIndex - moveBeginIndex);
            for (int i = 0; i < (moveEndIndex - moveBeginIndex) / 2; ++ i) {
                short int offset = *(short int *)((char *)directory + 2 * i);
                offset -= 8 + Length;
                memcpy((char *)directory + 2 * i, &offset, 2);
            }
            memcpy((char *)pageData + moveBeginIndex + 2, directory, moveEndIndex - moveBeginIndex);
            free(directory);
        }
    }
    NodesNum --;
    if (type == TypeVarChar)
        freePages += 10 + Length;
    else
        freePages += 12;
    memcpy((char *)pageData + 3, &freePages, 2);
    memcpy((char *)pageData + 1, &NodesNum, 2);
    return 0;
}


RC IndexManager::scan(IXFileHandle &ixFileHandle,
                      const Attribute &attribute,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      IX_ScanIterator &ix_ScanIterator) {
    {
        AttrType type = attribute.type;
        ix_ScanIterator.type = type;
        int rootPage;
        if (!ixFileHandle.fileHandle.myFile.is_open()) {
            return -1;
        }
        //int situation
        if (type == TypeInt) {
            if (lowKey != NULL && highKey != NULL && *(int *)(char *)lowKey == *(int *)(char *)highKey && !lowKeyInclusive && !highKeyInclusive) {
                return 0;
            }
            rootPage=ixFileHandle.fileHandle.RootPage;
            void * pageData = malloc(4096);
            ixFileHandle.fileHandle.readPage(rootPage, pageData);
            unsigned page = (unsigned) rootPage;
            char leafIndicator = *(char *)pageData;
            vector<PageNum> trace;
            bool freePages = true;
            int index;
            while (leafIndicator == '\x00') {
                nonleafSearch(ixFileHandle, pageData, page, leafIndicator, type, lowKey, trace, freePages, index);
            }
            short int moveBeginIndex, moveEndIndex;
            index = 0;
            searchLeaf(pageData, type, lowKey, moveBeginIndex, moveEndIndex, index);

            int nextPageNum = 0;
            int offset = moveBeginIndex;
            RID rid;
            short int NodesNum = *(short int *)((char *)pageData + 1);
            if (!lowKeyInclusive) {
                while (*(int *)((char *)pageData + offset) == *(int *)(char *)lowKey) {
                    index++;
                    offset += 12;
                }
                if (index + 1 > NodesNum) {
                    nextPageNum = *(int *)((char *)pageData + 4096 - 4);
                    if (nextPageNum == -1) {
                        return 0;
                    }
                    ixFileHandle.fileHandle.readPage(nextPageNum, pageData);
                    NodesNum = *(short int *)((char *)pageData + 1);
                    offset = 5;
                    index = 0;
                }
            }
            if (highKey == NULL) {
                while (1) {
                    if (index + 1 <= (int)NodesNum) {
                        rid.pageNum = *(unsigned *)((char *)pageData + offset + 4);
                        rid.slotNum = *(unsigned *)((char *)pageData + offset + 8);
                        ix_ScanIterator.returnedRID.push_back(rid);
                        vector<char> vKey;
                        for (int i = 0; i < 4; i++) {
                            char ch = *((char *)pageData + offset + i);
                            vKey.push_back(ch);
                        }
                        ix_ScanIterator.returnedKey.push_back(vKey);
                        offset += 12;
                        index++;
                    }
                    else {
                        nextPageNum = *(int *)((char *)pageData + 4096 - 4);//TODO:死循环罪魁祸首
                        if (nextPageNum == -1) {
                            free(pageData);
                            return 0; 
                        }
                        ixFileHandle.fileHandle.readPage(nextPageNum, pageData);
                        NodesNum = *(short int *)((char *)pageData + 1);
                        offset = 5;
                        index = 0;
                    }
                }
            }
            else {
                while (*(int *)((char *)pageData + offset) < *(int *)((char *)highKey)) {
                    if (index + 1 <= NodesNum) {
                        rid.pageNum = *(unsigned *)((char *)pageData + offset + 4);
                        rid.slotNum = *(unsigned *)((char *)pageData + offset + 8);
                        ix_ScanIterator.returnedRID.push_back(rid);
                        vector<char> vKey;
                        for (int i = 0; i < 4; i++) {
                            char ch = *((char *)pageData + offset + i);
                            vKey.push_back(ch);
                        }

                        void * data = malloc(4);
                        memset(data, 0, 4);
                        for (unsigned int i = 0; i < vKey.size(); i++) {
                            memcpy((char *)data + i, &vKey[i], 1);
                        }
                        free(data);

                        ix_ScanIterator.returnedKey.push_back(vKey);
                        offset += 12;
                        index++;
                    }
                    else {
                        nextPageNum = *(int *)((char *)pageData + 4096 - 4);
                        if (nextPageNum == -1) {
                            free(pageData);
                            return 0;
                        }
                        ixFileHandle.fileHandle.readPage(nextPageNum, pageData);
                        NodesNum = *(short int *)((char *)pageData + 1);
                        offset = 5;
                        index = 0;
                    }

                }
                if (highKeyInclusive) {
                    while (*(int *)((char *)pageData + offset) == *(int *)((char *)highKey)) {
                        if (index + 1 <= NodesNum) {
                            rid.pageNum = *(unsigned *)((char *)pageData + offset + 4);
                            rid.slotNum = *(unsigned *)((char *)pageData + offset + 8);
                            ix_ScanIterator.returnedRID.push_back(rid);
                            vector<char> vKey;
                            for (int i = 0; i < 4; i++) {
                                char ch = *((char *)pageData + offset + i);
                                vKey.push_back(ch);
                            }
                            ix_ScanIterator.returnedKey.push_back(vKey);
                            offset += 12;
                            index++;
                        }
                        else{
                            nextPageNum = *(int *)((char *)pageData + 4096 - 4);
                            if (nextPageNum == -1) {
                                return 0;
                            }
                            ixFileHandle.fileHandle.readPage(nextPageNum, pageData);
                            NodesNum = *(int *)((char *)pageData + 1);
                            offset = 5;
                            index = 0;
                        }
                    }
                }
            }
            free(pageData);
        }
            //real situation
        else if (type == TypeReal) {
            if (lowKey != NULL && highKey != NULL && *(float *)(char *)lowKey == *(float *)(char *)highKey && !lowKeyInclusive && !highKeyInclusive) {
                return 0;
            }
            rootPage=ixFileHandle.fileHandle.RootPage;
            void * pageData = malloc(4096);
            ixFileHandle.fileHandle.readPage(rootPage, pageData);
            unsigned page = (unsigned) rootPage;
            char leafIndicator = *(char *)pageData;
            vector<PageNum> trace;
            bool freePages = true;
            int index;
            while (leafIndicator != '\xFF') {
                nonleafSearch(ixFileHandle, pageData, page, leafIndicator, type, lowKey, trace, freePages, index);
            }
            short int moveBeginIndex, moveEndIndex;
            index = 0;
            searchLeaf(pageData, type, lowKey, moveBeginIndex, moveEndIndex, index);

            int nextPageNum = 0;
            int offset = moveBeginIndex;
            RID rid;
            short int NodesNum = *(short int *)((char *)pageData + 1);
            if (!lowKeyInclusive) {
                while (*(float *)((char *)pageData + offset) == *(float *)(char *)lowKey) {
                    index++;
                    offset += 12;
                }
                if (index + 1 > NodesNum) {
                    nextPageNum = *(int *)((char *)pageData + 4096 - 4);
                    if (nextPageNum == -1) {
                        free(pageData);
                        return 0;
                    }
                    ixFileHandle.fileHandle.readPage(nextPageNum, pageData);
                    NodesNum = *(short int *)((char *)pageData + 1);
                    offset = 5;
                    index = 0;
                }
            }
            if (highKey == NULL) {
                while (1) {
                    if (index + 1 <= NodesNum) {
                        rid.pageNum = *(unsigned *)((char *)pageData + offset + 4);
                        rid.slotNum = *(unsigned *)((char *)pageData + offset + 8);
                        ix_ScanIterator.returnedRID.push_back(rid);
                        vector<char> vKey;
                        for (int i = 0; i < 4; i++) {
                            char ch = *((char *)pageData + offset + i);
                            vKey.push_back(ch);
                        }
                        ix_ScanIterator.returnedKey.push_back(vKey);
                        offset += 12;
                        index++;
                    }
                    else {
                        nextPageNum = *(int *)((char *)pageData + 4096 - 4);
                        if (nextPageNum == -1) {
                            free(pageData);
                            return 0;
                        }
                        ixFileHandle.fileHandle.readPage(nextPageNum, pageData);
                        NodesNum = *(short int *)((char *)pageData + 1);
                        offset = 5;
                        index = 0;
                    }
                }
            }
              
            else {
                while (*(float *)((char *)pageData + offset) < *(float *)((char *)highKey)) {
                    if (index + 1 <= NodesNum) {
                        rid.pageNum = *(unsigned *)((char *)pageData + offset + 4);
                        rid.slotNum = *(unsigned *)((char *)pageData + offset + 8);
                        ix_ScanIterator.returnedRID.push_back(rid);
                        vector<char> vKey;
                        for (int i = 0; i < 4; i++) {
                            char ch = *((char *)pageData + offset + i);
                            vKey.push_back(ch);
                        }
                        ix_ScanIterator.returnedKey.push_back(vKey);
                        offset += 12;
                        index++;
                    }
                    else {
                        nextPageNum = *(int *)((char *)pageData + 4096 - 4);
                        if (nextPageNum == -1) {
                            free(pageData);
                            return 0;
                        }
                        ixFileHandle.fileHandle.readPage(nextPageNum, pageData);
                        NodesNum = *(short int *)((char *)pageData + 1);
                        offset = 5;
                        index = 0;
                    }
                }
                if (highKeyInclusive) {
                    while (*(float *)((char *)pageData + offset) == *(float *)((char *)highKey)) {
                        if (index + 1 <= NodesNum) {
                            rid.pageNum = *(unsigned *)((char *)pageData + offset + 4);
                            rid.slotNum = *(unsigned *)((char *)pageData + offset + 8);
                            ix_ScanIterator.returnedRID.push_back(rid);
                            vector<char> vKey;
                            for (int i = 0; i < 4; i++) {
                                char ch = *((char *)pageData + offset + i);
                                vKey.push_back(ch);
                            }
                            ix_ScanIterator.returnedKey.push_back(vKey);
                            offset += 12;
                            index++;
                        }
                        else {
                            nextPageNum = *(int *)((char *)pageData + 4096 - 4);
                            if (nextPageNum == -1) {
                                free(pageData);
                                return 0;
                            }
                            ixFileHandle.fileHandle.readPage(nextPageNum, pageData);
                            NodesNum = *(int *)((char *)pageData + 1);
                            offset = 5;
                            index = 0;
                        }
                    }
                }
            }

            free(pageData);
        }
            //varchar situation
        else {
            int lowLength;
            if (lowKey != NULL) {
                lowLength = *(int *)lowKey;
            }
            int entryVarCharLength;
            bool comparison, equal = false;
            if (lowKey != NULL && highKey != NULL) {
                compareVarchar((void *)lowKey, highKey, 4, lowLength + 4, comparison, equal);
                if (equal == true && !lowKeyInclusive && !highKeyInclusive) {
                    return 0;
                }
            }
            rootPage=ixFileHandle.fileHandle.RootPage;
            void * pageData = malloc(4096);
            ixFileHandle.fileHandle.readPage(rootPage, pageData);
            unsigned page = (unsigned) rootPage;
            char leafIndicator = *(char *)pageData;
            vector<PageNum> trace;
            bool freePages = true;
            int index;
            while (leafIndicator != '\xFF') {
                nonleafSearch(ixFileHandle, pageData, page, leafIndicator, type, lowKey, trace, freePages, index);
            }
            short int moveBeginIndex, moveEndIndex;
            index = 0;
            searchLeaf(pageData, type, lowKey, moveBeginIndex, moveEndIndex, index);

            int nextPageNum = 0;
            int offset = moveBeginIndex;
            RID rid;
            short int NodesNum = *(short int *)((char *)pageData + 1);
            if (!lowKeyInclusive) {
                entryVarCharLength = *(short int *)((char *)pageData + 4096 - 4 - (index + 1) * 2) - offset - 8;
                equal = false;
                compareVarchar(pageData, lowKey, offset, offset + entryVarCharLength, comparison, equal);
                while (equal == true) {
                    offset += entryVarCharLength + 8;
                    index++;
                    entryVarCharLength = *(short int *)((char *)pageData + 4096 - 4 - (index + 1) * 2) - offset - 8;
                    equal = false;
                    compareVarchar(pageData, lowKey, offset, offset + entryVarCharLength, comparison, equal);
                }
                if (index + 1 > NodesNum) {
                    nextPageNum = *(int *)((char *)pageData + 4096 - 4);
                    if (nextPageNum == -1) {
                        free(pageData);
                        return 0;
                    }
                    ixFileHandle.fileHandle.readPage(nextPageNum, pageData);
                    NodesNum = *(short int *)((char *)pageData + 1);
                    offset = 5;
                    index = 0;

                }
            }

            if (highKey == NULL) {
                entryVarCharLength = *(short int *)((char *)pageData + 4096 - 4 - (index + 1) * 2) - offset - 8;
                while (1) {
                    if (index + 1 <= NodesNum) {
                        rid.pageNum = *(unsigned *)((char *)pageData + offset + entryVarCharLength);
                        rid.slotNum = *(unsigned *)((char *)pageData + offset + entryVarCharLength + 4);
                        ix_ScanIterator.returnedRID.push_back(rid);
                        vector<char> vKey;
                        for (int i = 0; i < entryVarCharLength; i++) {
                            char ch = *((char *)pageData + offset + i);
                            vKey.push_back(ch);
                        }
                        ix_ScanIterator.returnedKey.push_back(vKey);
                        offset += entryVarCharLength + 8;
                        index++;
                    }
                    else {
                        nextPageNum = *(int *)((char *)pageData + 4096 - 4);
                        if (nextPageNum == -1) {
                            free(pageData);
                            return 0;
                        }
                        ixFileHandle.fileHandle.readPage(nextPageNum, pageData);
                        NodesNum = *(short int *)((char *)pageData + 1);
                        offset = 5;
                        index = 0;
                    }
                    entryVarCharLength = *(short int *)((char *)pageData + 4096 - 4 - (index + 1) * 2) - offset - 8;
                }
            }
            else {
                
                comparison = true;
                entryVarCharLength = *(short int *)((char *)pageData + 4096 - 4 - (index + 1) * 2) - offset - 8;
                compareVarchar(pageData, highKey, offset, offset + entryVarCharLength, comparison, equal);
                while (comparison == false) {
                    if (index + 1 <= NodesNum) {

                        rid.pageNum = *(unsigned *)((char *)pageData + offset + entryVarCharLength);
                        rid.slotNum = *(unsigned *)((char *)pageData + offset + entryVarCharLength + 4);
                        ix_ScanIterator.returnedRID.push_back(rid);
                        vector<char> vKey;
                        for (int i = 0; i < entryVarCharLength; i++) {
                            char ch = *((char *)pageData + offset + i);
                            vKey.push_back(ch);
                        }
                        ix_ScanIterator.returnedKey.push_back(vKey);

                        offset += entryVarCharLength + 8;
                        index++;
                        if (index + 1 > NodesNum) {
                            nextPageNum = *(int *)((char *)pageData + 4096 - 4);
                            if (nextPageNum == -1) {
                                free(pageData);
                                return 0;
                            }
                            ixFileHandle.fileHandle.readPage(nextPageNum, pageData);
                            NodesNum = *(short int *)((char *)pageData + 1);
                            offset = 5;
                            index = 0;
                        }
                    }
                    else {
                        nextPageNum = *(int *)((char *)pageData + 4096 - 4);
                        if (nextPageNum == -1) {
                            free(pageData);
                            return 0;
                        }
                        ixFileHandle.fileHandle.readPage(nextPageNum, pageData);
                        NodesNum = *(short int *)((char *)pageData + 1);
                        offset = 5;
                        index = 0;
                    }
                    comparison = true;
                    entryVarCharLength = *(short int *)((char *)pageData + 4096 - 4 - (index + 1) * 2) - offset - 8;
                    compareVarchar(pageData, highKey, offset, offset + entryVarCharLength, comparison, equal);
                }

                if (highKeyInclusive) {
                    equal = false;
                    entryVarCharLength = *(short int *)((char *)pageData + 4096 - 4 - (index + 1) * 2) - offset - 8;
                    compareVarchar(pageData, highKey, offset, offset + entryVarCharLength, comparison, equal);
                    while (equal == true) {
                        if (index + 1 <= NodesNum) {
                            rid.pageNum = *(unsigned *)((char *)pageData + offset + entryVarCharLength);
                            rid.slotNum = *(unsigned *)((char *)pageData + offset + entryVarCharLength + 4);
                            ix_ScanIterator.returnedRID.push_back(rid);
                            vector<char> vKey;
                            for (int i = 0; i < entryVarCharLength; i++) {
                                char ch = *((char *)pageData + offset + i);
                                vKey.push_back(ch);
                            }
                            ix_ScanIterator.returnedKey.push_back(vKey);
                            offset += entryVarCharLength + 8;
                            index++;
                        }
                        else {
                            nextPageNum = *(int *)((char *)pageData + 4096 - 4);
                            if (nextPageNum == -1) {
                                free(pageData);
                                return 0;
                            }
                            ixFileHandle.fileHandle.readPage(nextPageNum, pageData);
                            NodesNum = *(int *)((char *)pageData + 1);
                            offset = 5;
                            index = 0;
                        }
                        entryVarCharLength = *(short int *)((char *)pageData + 4096 - 4 - (index + 1) * 2) - offset - 8;
                        equal = false;
                        compareVarchar(pageData, highKey, offset, offset + entryVarCharLength, comparison, equal);
                    }
                }
            }

            free(pageData);
        }

        return 0;
    }
}

void IndexManager::printBtree(IXFileHandle &ixFileHandle, const Attribute &attribute) const {
    int rootPage=ixFileHandle.fileHandle.RootPage;
    //get root page
    void *pageData = malloc(4096);
    ixFileHandle.fileHandle.readPage(rootPage, pageData);

    unsigned page = (unsigned) rootPage;
    string ans = treeSerialize(ixFileHandle, attribute.type, page, pageData);
    std::cout << ans << endl;

    free(pageData);
    return;
}



RC
IndexManager::compareVarchar(void *pageData, const void *key, const short int beginOffset, const short int endOffset,
                                bool &comparison, bool &equal) {
    char* keyStr=(char*)malloc(4096);
    char* leafStr=(char*)malloc(4096);
    int keylen=*(int*)key;
    int leaflen=endOffset-beginOffset;
    memcpy(keyStr,(char*)key+4,keylen);
    memcpy(leafStr,(char*)pageData+beginOffset,leaflen);
    int i=0;
    comparison=false;
    equal=false;
    for(;i<keylen&&i<leaflen;i++){
        if(keyStr[i]>leafStr[i]){
            comparison=true;
            break;
        }
    }
    if(keylen==leaflen&&i==keylen){
        comparison=true;
        equal=true;
    }
    free(keyStr);
    free(leafStr);
    return 0;



}



RC IndexManager::nonleafSearch(IXFileHandle &ixFileHandle, void *pageData, PageNum &page, char &leafIndicator,
                                 const AttrType type, const void *key, vector <PageNum> &pageTrack, bool equalfreePages, int &index) {

    if (key == NULL) {
        page = *(int *)((char *)pageData + 5);
        ixFileHandle.fileHandle.readPage(page, pageData);
        leafIndicator = *(char *)pageData;
        return 0;
    }
    short int NodesNum = *(short int *)((char *)pageData + 1);

    if (type == TypeVarChar) {
        bool comparison = false;
        bool equal = false;
        short int beginOffset = 9;
        short int endOffset = *(short int *)((char *)pageData + 4094);
        short int i = 0;
        while (!comparison && i < NodesNum) {
            compareVarchar(pageData, key, beginOffset, endOffset, comparison, equal);
            if (comparison && !equal) {
                if (equalfreePages) {
                    page = *(int *)((char *)pageData + beginOffset - 4);
                    pageTrack.push_back(page);
                    ixFileHandle.fileHandle.readPage(page, pageData);
                    leafIndicator = *(char *)pageData;
                }
                else {
                    index = i;
                }
                return 0;
            }

            else if (equal) {
                if (equalfreePages) {
                    page = *(int *)((char *)pageData + endOffset);
                    pageTrack.push_back(page);
                    ixFileHandle.fileHandle.readPage(page, pageData);
                    leafIndicator = *(char *)pageData;
                }
                else {
                    return -1;
                }
                return 0;
            }

            i ++;
            beginOffset = endOffset + 4;
            endOffset = *(short int *)((char *)pageData + 4094 - i * 2);

        }

        if (!comparison) {
            if (equalfreePages) {
                page = *(int *)((char *)pageData + beginOffset - 4);
                pageTrack.push_back(page);
                ixFileHandle.fileHandle.readPage(page, pageData);
                leafIndicator = *(char *)pageData;
            }
            else {
                index = i;
            }
        }
    }
    else {
        short int offset = 9;
        for (int i = 0; i < NodesNum; ++ i) {
            if (*(float *)((char *)pageData + offset) > *(float *)key) {
                if (equalfreePages) {
                    page = *(int *)((char *)pageData + offset - 4);
                    pageTrack.push_back(page);
                    ixFileHandle.fileHandle.readPage(page, pageData);
                    leafIndicator = *(char *)pageData;
                    return 0;
                }
                else {
                    index = i;
                }

            }
            offset += 8;
        }
        if (equalfreePages) {
            page = *(int *)((char *)pageData + offset - 4);
            pageTrack.push_back(page);
            ixFileHandle.fileHandle.readPage(page, pageData);
            leafIndicator = *(char *)pageData;
        }
        else {
            index = NodesNum;
        }
    }

    return 0;
}

RC IndexManager::searchLeaf(void *pageData, const AttrType &type, const void *key, short int &moveBeginIndex,
                              short int &moveEndIndex, int &index) {
    short int NodesNum = *(short int *)((char *)pageData + 1);
    if (NodesNum == 0) {
        moveBeginIndex = 5;
        moveEndIndex = 5;
        index = 0;
        return 0;
    }
    if (type == TypeVarChar) {
        if (key == NULL) {
            moveBeginIndex = 5;
            moveEndIndex = *(short int *)((char *)pageData + 4092 - NodesNum * 2);
            index = 0;
            return 0;
        }
        bool comparison = false;
        bool equal = false;
        short int beginOffset = 5;
        short int endOffset = *(short int *)((char *)pageData + 4090) - 8;
        short int i = 0;
        while (!comparison && i < NodesNum) {
            compareVarchar(pageData, key, beginOffset, endOffset, comparison, equal);
            if (comparison) {
                moveBeginIndex = beginOffset;
                moveEndIndex = *(short int *)((char *)pageData + 4092 - NodesNum * 2);
                index = i;
                return 0;
            }
            i ++;
            beginOffset = endOffset + 8;
            endOffset = *(short int *)((char *)pageData + 4090 - i * 2) - 8;
        }
        if (!comparison) {
            moveBeginIndex = beginOffset;
            moveEndIndex = moveBeginIndex;
        }
    }
    else if (type == TypeInt) {
        if (key == NULL) {
            moveBeginIndex = 5;
            moveEndIndex = 4096 - *(short int *)((char *)pageData + 3) - 4;
            index = 0;
            return 0;
        }
        short int offset = 5;
        for (int i = 0; i < NodesNum; ++ i) {
            if (*(int *)((char *)pageData + offset) >= *(int *)key) {
                moveBeginIndex = offset;
                moveEndIndex = 4096 - *(short int *)((char *)pageData + 3) - 4;
                index = i;
                return 0;
            }
            offset += 12;
        }
        moveBeginIndex = offset;
        moveEndIndex = offset;
    }
    else {
        if (key == NULL) {
            moveBeginIndex = 5;
            moveEndIndex = 4096 - *(short int *)((char *)pageData + 3) - 4;
            index = 0;
            return 0;
        }
        short int offset = 5;
        for (int i = 0; i < NodesNum; ++ i) {
            if (*(float *)((char *)pageData + offset) >= *(float *)key) {
                moveBeginIndex = offset;
                moveEndIndex = 4096 - *(short int *)((char *)pageData + 3) - 4;
                index = i;
                return 0;
            }
            offset += 12;
        }
        moveBeginIndex = offset;
        moveEndIndex = offset;
    }
    index = NodesNum;
    return 0;
}

RC
IndexManager::splitLeaf(IXFileHandle &ixFileHandle, void *pageData, void *newPage, AttrType type, vector <PageNum> &trace,
                        void *midKey) {
    trace.pop_back();
    short int NodesNum = *(short int *)((char *)pageData + 1);//当前memory多少node
    short int freePages = *(short int *)((char *)pageData + 3);//当前有多少空位
    int newPageNumber = ixFileHandle.fileHandle.getNumberOfPages();
    short int NodesNumOld = NodesNum / 2;
    short int NodesNumNew = NodesNum - NodesNumOld;
    if (type == TypeVarChar) {
        short int lastEnd;
        short int splitOffset = *(short int *)((char *)pageData + 4092 - 2 * NodesNumOld);
        if (NodesNumOld != 1)
            lastEnd = *(short int *)((char *)pageData + 4094 - 2 * NodesNumOld);
        else
            lastEnd = 5;
        short int nextRightEnd = *(short int *)((char *)pageData + 4090 - 2 * NodesNumOld);
        void *leftLastKey = malloc(4096);
        void *rightFirstKey = malloc(4096);
        memcpy(leftLastKey, (char *)pageData + lastEnd, splitOffset - 8 - lastEnd);
        memcpy(rightFirstKey, (char *)pageData + splitOffset, nextRightEnd - 8 - splitOffset);
        short int originalSplit = splitOffset;
        while (memcmp(leftLastKey, rightFirstKey, splitOffset - 8 - lastEnd) == 0 && NodesNumOld >= 1){
            splitOffset = lastEnd;
            NodesNumOld --;
            NodesNumNew ++;
            if (NodesNumOld != 1)
                lastEnd = *(short int *)((char *)pageData + 4094 - 2 * NodesNumOld);
            else
                lastEnd = 5;
            nextRightEnd = *(short int *)((char *)pageData + 4090 - 2 * NodesNumOld);
            memset(leftLastKey, 0, 4096);
            memset(rightFirstKey, 0, 4096);
            memcpy(leftLastKey, (char *)pageData + lastEnd, splitOffset - 8 - lastEnd);
            memcpy(rightFirstKey, (char *)pageData + splitOffset, nextRightEnd - 8 - splitOffset);
        }
        if (NodesNumOld == 0) {
            splitOffset = originalSplit;
            NodesNumOld = NodesNum / 2;
            NodesNumNew = NodesNum - NodesNumOld;
            if (NodesNumOld != 1)
                lastEnd = *(short int *)((char *)pageData + 4094 - 2 * NodesNumOld);
            else
                lastEnd = 5;
            nextRightEnd = *(short int *)((char *)pageData + 4090 - 2 * NodesNumOld);
            memset(leftLastKey, 0, 4096);
            memset(rightFirstKey, 0, 4096);
            memcpy(leftLastKey, (char *)pageData + lastEnd, splitOffset - 8 - lastEnd);
            memcpy(rightFirstKey, (char *)pageData + splitOffset, nextRightEnd - 8 - splitOffset);
            while (memcmp(leftLastKey, rightFirstKey, splitOffset - 8 - lastEnd) == 0 && NodesNumNew >= 1){
                splitOffset = nextRightEnd;
                NodesNumOld ++;
                NodesNumNew --;
                lastEnd = *(short int *)((char *)pageData + 4094 - 2 * NodesNumOld);
                nextRightEnd = *(short int *)((char *)pageData + 4090 - 2 * NodesNumOld);
                memset(leftLastKey, 0, 4096);
                memset(rightFirstKey, 0, 4096);
                memcpy(leftLastKey, (char *)pageData + lastEnd, splitOffset - 8 - lastEnd);
                memcpy(rightFirstKey, (char *)pageData + splitOffset, nextRightEnd - 8 - splitOffset);
            }
        }
        free(leftLastKey);
        free(rightFirstKey);

        short int moveBeginIndex = *(short int *)((char *)pageData + 4092 - 2 * NodesNumOld);
        short int moveEndIndex = *(short int *)((char *)pageData + 4092 - 2 * NodesNum);
        for (int i = NodesNumOld + 1; i <= NodesNum; ++ i) {
            short int newOffset = *(short int *)((char *)pageData + 4092 - 2 * i);
            newOffset = newOffset - moveBeginIndex + 5;
            memcpy((char *)pageData + 4092 - 2 * i, &newOffset, 2);
        }
        void *directory = malloc(2 * NodesNumNew);
        memcpy(directory, (char *)pageData + 4092 - 2 * NodesNum, 2 * NodesNumNew);
        memset((char *)pageData + 4092 - 2 * NodesNum, 0, 2 * NodesNumNew);
        freePages += moveEndIndex - moveBeginIndex + 2 * NodesNumNew;
        void *data = malloc(moveEndIndex - moveBeginIndex);
        memcpy(data, (char *)pageData + moveBeginIndex, moveEndIndex - moveBeginIndex);
        memset((char *)pageData + moveBeginIndex, 0, moveEndIndex - moveBeginIndex);
        memcpy((char *)pageData + 1, &NodesNumOld, 2);
        memcpy((char *)pageData + 3, &freePages, 2);
        int nextPage = *(int *)((char *)pageData + 4092);
        memcpy((char *)pageData + 4092, &newPageNumber, 4);
        // Set up a new page
        char leafIndicator = '\xFF';
        freePages = 4096 - (9 + moveEndIndex - moveBeginIndex + 2 * NodesNumNew);
        memcpy((char *)newPage + 4092, &nextPage, 4);
        memcpy((char *)newPage + 4092 - 2 * NodesNumNew, directory, 2 * NodesNumNew);
        memcpy(newPage, &leafIndicator, 1);
        memcpy((char *)newPage + 1, &NodesNumNew, 2);
        memcpy((char *)newPage + 3, &freePages, 2);
        memcpy((char *)newPage + 5, data, moveEndIndex - moveBeginIndex);
        int firstKeyEnd = *(short int *)((char *)newPage + 4090);
        firstKeyEnd -= 13;
        memcpy(midKey, &firstKeyEnd, 4);
        memcpy((char *)midKey + 4, (char *)newPage + 5, firstKeyEnd);
        free(data);
        free(directory);
    }
    else {
        short int originalSplitOffset, splitOffset;
        splitOffset = 5 + 12 * NodesNumOld;
        originalSplitOffset = splitOffset;
        void *leftLastKey = malloc(4);
        void *rightFirstKey = malloc(4);
        memcpy(leftLastKey, (char *)pageData + splitOffset - 12, 4);
        memcpy(rightFirstKey, (char *)pageData + splitOffset, 4);
        while (memcmp(leftLastKey, rightFirstKey, 4) == 0 && NodesNumOld >= 1) {
            splitOffset -= 12;
            NodesNumOld --;
            NodesNumNew ++;
            memcpy(leftLastKey, (char *)pageData + splitOffset - 12, 4);
            memcpy(rightFirstKey, (char *)pageData + splitOffset, 4);
        }
        if (NodesNumOld == 0) {
            splitOffset = originalSplitOffset;
            NodesNumOld = NodesNum/ 2;
            NodesNumNew = NodesNum - NodesNumOld;
            memcpy(leftLastKey, (char *)pageData + splitOffset - 12, 4);
            memcpy(rightFirstKey, (char *)pageData + splitOffset, 4);
            while (memcmp(leftLastKey, rightFirstKey, 4) == 0 && NodesNumNew >= 1) {
                splitOffset += 12;
                NodesNumOld ++;
                NodesNumNew --;
                memcpy(leftLastKey, (char *)pageData + splitOffset, 4);
                memcpy(rightFirstKey, (char *)pageData + splitOffset + 12, 4);
            }
        }
        free(leftLastKey);
        free(rightFirstKey);

        short int moveBeginIndex = 5 + 12 * NodesNumOld;
        short int moveEndIndex = 5 + 12 * NodesNum;
        freePages += moveEndIndex - moveBeginIndex;
        void *data = malloc(moveEndIndex - moveBeginIndex);
        memcpy(data, (char *)pageData + moveBeginIndex, moveEndIndex - moveBeginIndex);
        memset((char *)pageData + moveBeginIndex, 0, moveEndIndex - moveBeginIndex);
        memcpy((char *)pageData + 1, &NodesNumOld, 2);
        memcpy((char *)pageData + 3, &freePages, 2);
        int nextPage = *(int *)((char *)pageData + 4092);
        memcpy((char *)pageData + 4092, &newPageNumber, 4);
        // Set up a new page
        char leafIndicator = '\xFF';
        freePages = 4096 - (9 + moveEndIndex - moveBeginIndex);
        memcpy((char *)newPage + 4092, &nextPage, 4);
        memcpy(newPage, &leafIndicator, 1);
        memcpy((char *)newPage + 1, &NodesNumNew, 2);
        memcpy((char *)newPage + 3, &freePages, 2);
        memcpy((char *)newPage + 5, data, moveEndIndex - moveBeginIndex);
        memcpy(midKey, (char *)newPage + 5, 4);
        free(data);
    }
    return 0;
}

RC IndexManager::splitIndex(IXFileHandle &ixFileHandle, void *pageData, void *newPage, AttrType type,
                            vector <PageNum> &trace, void *leftPageLastKey, void *rightPageFirstKey) {
    trace.pop_back();
    short int NodesNum = *(short int *)((char *)pageData + 1);
    short int freePages = *(short int *)((char *)pageData + 3);
    short int NodesNumOld = NodesNum / 2;
    short int NodesNumNew = NodesNum - NodesNumOld;
    if (type == TypeVarChar) {
        short int moveBeginIndex = *(short int *)((char *)pageData + 4096 - 2 * NodesNumOld);
        short int moveEndIndex = *(short int *)((char *)pageData + 4096 - 2 * NodesNum) + 4;
        for (int i = NodesNumOld + 1; i <= NodesNum; ++ i) {
            short int newOffset = *(short int *)((char *)pageData + 4096 - 2 * i);
            newOffset = newOffset - moveBeginIndex + 5;
            memcpy((char *)pageData + 4096 - 2 * i, &newOffset, 2);
        }
        void *directory = malloc(2 * NodesNumNew);
        memcpy(directory, (char *)pageData + 4096 - 2 * NodesNum, 2 * NodesNumNew);
        memset((char *)pageData + 4096 - 2 * NodesNum, 0, 2 * NodesNumNew);
        freePages += moveEndIndex - moveBeginIndex + 2 * NodesNumNew - 4;
        void *data = malloc(moveEndIndex - moveBeginIndex);
        memcpy(data, (char *)pageData + moveBeginIndex, moveEndIndex - moveBeginIndex);
        memset((char *)pageData + moveBeginIndex, 0, moveEndIndex - moveBeginIndex);
        memcpy((char *)pageData + 1, &NodesNumOld, 2);
        memcpy((char *)pageData + 3, &freePages, 2);
        void *lastPointer = malloc(4);
        memcpy(lastPointer, data, 4);
        memcpy((char *)pageData + moveBeginIndex, lastPointer, 4);
        free(lastPointer);
        // Set up a new page
        char leafIndicator = '\x00';
        freePages = 4096 - (5 + moveEndIndex - moveBeginIndex + 2 * NodesNumNew);
        memcpy((char *)newPage + 4096 - 2 * NodesNumNew, directory, 2 * NodesNumNew);
        memcpy(newPage, &leafIndicator, 1);
        memcpy((char *)newPage + 1, &NodesNumNew, 2);
        memcpy((char *)newPage + 3, &freePages, 2);
        memcpy((char *)newPage + 5, data, moveEndIndex - moveBeginIndex);
        int firstKeyEnd = *(short int *)((char *)newPage + 4094);
        firstKeyEnd -= 9;
        memcpy(rightPageFirstKey, &firstKeyEnd, 4);
        memcpy((char *)rightPageFirstKey + 4, (char *)newPage + 9, firstKeyEnd);
        int Length;
        if (NodesNumOld == 1) {
            Length = *(short int *)((char *)pageData + 4094) - 9;
        }
        else {
            Length = *(short int *)((char *)pageData + 4096 - 2 * NodesNumOld) - *(short int *)((char *)pageData + 4096 - 2 * (NodesNumOld - 1)) - 4;
        }
        memcpy(leftPageLastKey, &Length, 4);
        memcpy((char *)leftPageLastKey + 4, (char *)pageData + moveBeginIndex - Length, Length);
        free(data);
        free(directory);
    }
    else {
        short int moveBeginIndex = 5 + 8 * NodesNumOld + 4;
        short int moveEndIndex = 5 + 8 * NodesNum + 4;
        freePages += moveEndIndex - moveBeginIndex;
        void *data = malloc(moveEndIndex - moveBeginIndex + 4);
        memcpy(data, (char *)pageData + moveBeginIndex - 4, moveEndIndex - moveBeginIndex + 4);
        memset((char *)pageData + moveBeginIndex, 0, moveEndIndex - moveBeginIndex);
        memcpy((char *)pageData + 1, &NodesNumOld, 2);
        memcpy((char *)pageData + 3, &freePages, 2);
        // Set up a new page
        char leafIndicator = '\x00';
        freePages = 4096 - (9 + moveEndIndex - moveBeginIndex);
        memcpy(newPage, &leafIndicator, 1);
        memcpy((char *)newPage + 1, &NodesNumNew, 2);
        memcpy((char *)newPage + 3, &freePages, 2);
        memcpy((char *)newPage + 5, data, moveEndIndex - moveBeginIndex + 4);
        memcpy(rightPageFirstKey, (char *)newPage + 9, 4);
        memcpy(leftPageLastKey, (char *)pageData + moveBeginIndex - 8, 4);
        free(data);
    }
    return 0;
}

RC IndexManager::indexPageUpdate(IXFileHandle &ixFileHandle, void *pageData, const void *key, AttrType type, void *midKey,
                                 bool leftOrRight, PageNum newPageNumber) {
    short int NodesNum = *(short int *)((char *)pageData + 1);
    short int freePages = *(short int *)((char *)pageData + 3);
    NodesNum --;
    if (leftOrRight) {
        if (type == TypeVarChar) {
            short int end = *(short int *)((char *)pageData + 4096 - 2 * NodesNum) + 4;
            short int Length = *(short int *)((char *)pageData + 4096 - 2 * (NodesNum + 1)) + 4 - end;
            int keyLength = Length - 4;
            memcpy(midKey, &keyLength, 4);
            memcpy((char *)midKey + 4, (char *)pageData + end, keyLength);
            memset((char *)pageData + end, 0, Length);
            memset((char *)pageData + 4096 - 2 * (NodesNum + 1), 0, 2);
            freePages += Length + 2;
        }
        else {
            memcpy(midKey, (char *)pageData + 5 + 8 * NodesNum + 4, 4);
            memset((char *)pageData + 5 + 8 * NodesNum + 4, 0, 8);
            freePages += 8;
        }
    }
    else {
        if (type == TypeVarChar) {
            int Length = *(short int *)((char *)pageData + 4094) - 9;
            memcpy(midKey, &Length, 4);
            memcpy((char *)midKey + 4, (char *)pageData + 9, Length);
            short int moveBeginIndex = 9 + Length;
            short int moveEndIndex = *(short int *)((char *)pageData + 4096 - 2 * (NodesNum + 1)) + 4;
            void *data = malloc(moveEndIndex - moveBeginIndex);
            memcpy(data, (char *)pageData + moveBeginIndex, moveEndIndex - moveBeginIndex);
            memset((char *)pageData + moveBeginIndex, 0, moveEndIndex - moveBeginIndex);
            memcpy((char *)pageData + 5, data, moveEndIndex - moveBeginIndex);
            free(data);
            for (int i = 2; i <= NodesNum + 1; ++ i) {
                short int newOffset = *(short int *)((char *)pageData + 4096 - i * 2);
                newOffset -= Length + 4;
                memcpy((char *)pageData + 4096 - i * 2, &newOffset, 2);
            }
            void *directory = malloc(2 * NodesNum);
            memcpy(directory, (char *)pageData + 4096 - 2 * (NodesNum + 1), 2 * NodesNum);
            memset((char *)pageData + 4096 - 2 * (NodesNum + 1), 0, 2);
            memcpy((char *)pageData + 4096 - 2 * NodesNum, directory, 2 * NodesNum);
            free(directory);
            freePages += 4 + Length + 2;
        }
        else {
            void *data = malloc(8 * NodesNum);
            memcpy(midKey, (char *)pageData + 9, 4);
            memcpy(data, (char *)pageData + 13, 8 * NodesNum);
            memset((char *)pageData + 13, 0, 8 * NodesNum);
            memcpy((char *)pageData + 5, data, 8 * NodesNum);
            free(data);
            freePages += 8;
        }
    }
    memcpy((char *)pageData + 1, &NodesNum, 2);
    memcpy((char *)pageData + 3, &freePages, 2);
    indexPageInsert(ixFileHandle, pageData, key, type, newPageNumber);
    return 0;
}

RC IndexManager::createRoot(IXFileHandle &ixFileHandle, void *midKey, AttrType type, PageNum left, PageNum right) {
    void *rootPage = malloc(4096);
    memset(rootPage, 0, 4096);
    char leafIndicator = '\x00';
    short int NodesNum = 1;
    short int freePages;
    memcpy(rootPage, &leafIndicator, 1);
    memcpy((char *)rootPage + 1, &NodesNum, 2);
    if (type == TypeVarChar) {
        int length = *(int *)midKey;
        freePages = 4096 - 15 - length;
        memcpy((char *)rootPage + 3, &freePages, 2);
        int pointerLeft = (int) left;
        int pointerRight = (int) right;
        memcpy((char *)rootPage + 5, &pointerLeft, 4);
        memcpy((char *)rootPage + 9, (char *)midKey + 4, length);
        memcpy((char *)rootPage + 9 + length, &pointerRight, 4);
        short int endOffset = 9 + length;
        memcpy((char *)rootPage + 4094, &endOffset, 2);
    }
    else {
        freePages = 4096 - 17;
        memcpy((char *)rootPage + 3, &freePages, 2);
        int pointerLeft = (int) left;
        int pointerRight = (int) right;
        memcpy((char *)rootPage + 5, &pointerLeft, 4);
        memcpy((char *)rootPage + 9, midKey, 4);
        memcpy((char *)rootPage + 13, &pointerRight, 4);
    }
    ixFileHandle.fileHandle.appendPage(rootPage);
    int root = ixFileHandle.fileHandle.getNumberOfPages() - 1;
    ixFileHandle.fileHandle.RootPage=root;
    free(rootPage);
    return 0;
}

bool IndexManager::compare(const void *m1, void *m2, AttrType type) {
    if (type == TypeVarChar) {
        int l1 = *(int *)m1;
        int l2 = *(int *)m2;
        int l = l1 < l2 ? l1 : l2;
        for (int i = 0; i < l; ++ i) {
            if (*((char *)m1 + 4 + i) < *((char *)m2 + 4 + i))
                return false;
            else if (*((char *)m1 + 4 + i) > *((char *)m2 + 4 + i))
                return true;
        }
        if (l1 < l2)
            return false;
        else
            return true;
    }
    else if (type == TypeInt) {
        int n1 = *(int *)m1;
        int n2 = *(int *)m2;
        return n1 >= n2;
    }
    else {
        float n1 = *(float *)m1;
        float n2 = *(float *)m2;
        return n1 >= n2;
    }
}



RC IndexManager::indexPageInsert(IXFileHandle &ixFileHandle, void *pageData, const void *key, AttrType type,
                                   PageNum newPageNumber) {
    short int freePages = *(short int *)((char *)pageData + 3);
    short int NodesNum = *(short int *)((char *)pageData + 1);
    short int moveBeginIndex, moveEndIndex;
    int index = 0;
    vector<PageNum> trace;
    PageNum page = 0;
    char leafIndicator = '\xFF';
    if (type == TypeVarChar) {
        int length = *(int *)key;
        freePages -= length + 6;
        nonleafSearch(ixFileHandle, pageData, page, leafIndicator, type, key, trace, false, index);
        if (index == 0) {
            moveBeginIndex = 9;
        }
        else {
            moveBeginIndex = *(short int *)((char *)pageData + 4096 - 2 * index) + 4;
        }
        moveEndIndex = *(short int *)((char *)pageData + 4096 - 2 * NodesNum) + 4;
        // Move the data
        if (moveBeginIndex != moveEndIndex) {
            void *data = malloc(moveEndIndex - moveBeginIndex);
            memcpy(data, (char *)pageData + moveBeginIndex, moveEndIndex - moveBeginIndex);
            memset((char *)pageData + moveBeginIndex, 0, moveEndIndex- moveBeginIndex);
            memcpy((char *)pageData + moveBeginIndex + length + 4, data, moveEndIndex - moveBeginIndex);
            free(data);
        }
        memcpy((char *)pageData + moveBeginIndex, (char *)key + 4, length);
        memcpy((char *)pageData + moveBeginIndex + length, &newPageNumber, 4);
        // Move the directory
        moveBeginIndex = 4096 - 2 * index;
        moveEndIndex = 4096 - 2 * NodesNum;
        void *directory = malloc(moveBeginIndex - moveEndIndex);
        memcpy(directory, (char *)pageData + 4096 - 2 * NodesNum, moveBeginIndex - moveEndIndex);
        memset((char *)pageData + 4096 - 2 * NodesNum, 0, moveBeginIndex - moveEndIndex);
        short int lastPointer;
        if (index == 0) {
            lastPointer = 9 + length;
        }
        else {
            lastPointer = *(short int *)((char *)pageData + moveBeginIndex);
            lastPointer += 4 + length;
        }
        memcpy((char *)pageData + moveBeginIndex - 2, &lastPointer, 2);
        for (int i = 0; i < NodesNum - index; ++ i) {
            lastPointer = *(short int *)((char *)directory + 2 * i);
            lastPointer += 4 + length;
            memcpy((char *)directory + 2 * i, &lastPointer, 2);
        }
        memcpy((char *)pageData + 4096 - 2 * (NodesNum + 1), directory, moveBeginIndex - moveEndIndex);
        free(directory);
    }
    else {
        freePages -= 8;
        nonleafSearch(ixFileHandle, pageData, page, leafIndicator, type, key, trace, false, index);
        moveBeginIndex = 5 + 8 * index + 4;
        moveEndIndex = 5 + 8 * NodesNum + 4;
        // Move the data
        if (moveBeginIndex != moveEndIndex) {
            void *data = malloc(moveEndIndex - moveBeginIndex);
            memcpy(data, (char *)pageData + moveBeginIndex, moveEndIndex - moveBeginIndex);
            memset((char *)pageData + moveBeginIndex, 0, moveEndIndex- moveBeginIndex);
            memcpy((char *)pageData + moveBeginIndex + 8, data, moveEndIndex - moveBeginIndex);
            free(data);
        }
        memcpy((char *)pageData + moveBeginIndex, key, 4);
        memcpy((char *)pageData + moveBeginIndex + 4, &newPageNumber, 4);
    }
    // Update the freePages and NodesNum
    NodesNum ++;
    memcpy((char *)pageData + 1, &NodesNum, 2);
    memcpy((char *)pageData + 3, &freePages, 2);
    return 0;
}

RC IndexManager::insertLeafPage(void *pageData, const void *key, AttrType type, const RID &rid) {
    short int freePages = *(short int *)((char *)pageData + 3);
    short int NodesNum = *(short int *)((char *)pageData + 1);
    if (type == TypeVarChar) {
        int length = *(int *)key;
        freePages -= length + 10;
        NodesNum ++;
        short int moveBeginIndex, moveEndIndex;
        int index = 0;
        searchLeaf(pageData, type, key, moveBeginIndex, moveEndIndex, index);//理论上来说这个index应该是nextpage
        // Move the data
        if (moveBeginIndex != moveEndIndex) {//找出新的元素插入点  把元素插到movebegin 前
            void *data = malloc(moveEndIndex - moveBeginIndex);
            memcpy(data, (char *)pageData + moveBeginIndex, moveEndIndex - moveBeginIndex);
            memcpy((char *)pageData + moveBeginIndex + 8 + length, data, moveEndIndex - moveBeginIndex);
            free(data);
            void *directory = malloc((NodesNum - 1 - index) * 2);
            for (int i = index; i < NodesNum - 1; ++ i) {
                short int newOffset = *(short int *)((char *)pageData + 4090 - i * 2) + 8 + length;
                memcpy((char *)pageData + 4090 - i * 2, &newOffset, 2);
            }
            memcpy(directory, (char *)pageData + 4092 - (NodesNum - 1) * 2, (NodesNum - 1 - index) * 2);
            memcpy((char *)pageData + 4092 - 2 * NodesNum, directory, (NodesNum - 1 - index) * 2);
            free(directory);
        }
        // Insert the new node in the leaf
        short int end = moveBeginIndex + 8 + length;
        memcpy((char *)pageData + 4090 - index * 2, &end, 2);//整个page后边那个end pointer
        memcpy((char *)pageData + moveBeginIndex, (char *)key + 4, length);
        memcpy((char *)pageData + moveBeginIndex + length, &rid.pageNum, sizeof(unsigned int));
        memcpy((char *)pageData + moveBeginIndex + 4 + length, &rid.slotNum, sizeof(unsigned int));
        // Update the freePages and NodesNum
        memcpy((char *)pageData + 1, &NodesNum, 2);
        memcpy((char *)pageData + 3, &freePages, 2);
    }
    else {
        freePages -= 12;
        NodesNum ++;
        short int moveBeginIndex, moveEndIndex;
        int index;
        searchLeaf(pageData, type, key, moveBeginIndex, moveEndIndex, index);
        // Move the data
        if (moveBeginIndex != moveEndIndex) {
            void *data = malloc(moveEndIndex - moveBeginIndex);
            memcpy(data, (char *)pageData + moveBeginIndex, moveEndIndex - moveBeginIndex);
            memcpy((char *)pageData + moveBeginIndex + 12, data, moveEndIndex - moveBeginIndex);
            free(data);
        }
        // Insert the new node in the leaf
        memcpy((char *)pageData + moveBeginIndex, key, 4);
        memcpy((char *)pageData + moveBeginIndex + 4, &rid.pageNum, sizeof(unsigned int));
        memcpy((char *)pageData + moveBeginIndex + 8, &rid.slotNum, sizeof(unsigned int));
        // Update the freePages and NodesNum
        memcpy((char *)pageData + 1, &NodesNum, 2);
        memcpy((char *)pageData + 3, &freePages, 2);
    }
    return 0;
}

string IndexManager::treeSerialize(IXFileHandle &ixFileHandle, const AttrType type, PageNum root, void *pageData) const
{
    int NodesNum = *(short int *)((char *)pageData + 1);
    char leafIndicator = *(char *)pageData;
    int index;
    string s = "";
    if (NodesNum == 0) {
        return "";
    }

    if (leafIndicator == '\x00') {
        s += "{\"keys\":[";
        int offset = 5;
        if (type == TypeInt) {
            int tempKey = *(int *)((char *)pageData + offset + 4);
            s += "\"" + to_string(tempKey) + "\"";
            for (index = 1; index < NodesNum; index++) {
                tempKey = *(int *)((char *)pageData + offset + 4 + 8 * index);////
                s += ",\"" + to_string(tempKey) + "\"";
            }
            s += "],\"children\":[";
            void *tempPage = malloc(4096);
            PageNum tempPageNum = *(unsigned *)((char *)pageData + offset);
            ixFileHandle.fileHandle.readPage(tempPageNum, tempPage);
            s += treeSerialize(ixFileHandle, type, tempPageNum, tempPage);

            for (index = 0; index < NodesNum; index++) {
                tempPageNum = *(unsigned *)((char *)pageData + offset + 8 * (1 + index));////
                ixFileHandle.fileHandle.readPage(tempPageNum, tempPage);
                s += ",";
                s += treeSerialize(ixFileHandle, type, tempPageNum, tempPage);
            }
            s += "]}";
            free(tempPage);
        }
        else if (type == TypeReal) {
            int tempKey = *(float *)((char *)pageData + offset + 4);////
            s += "\"" + to_string(tempKey) + "\"";
            for (index = 1; index < NodesNum; index++) {
                tempKey = *(float *)((char *)pageData + offset + 4 + 8 * index);////
                s += ",\"" + to_string(tempKey) + "\"";
            }
            s += "],\"children\":[";
            void *tempPage = malloc(4096);
            PageNum tempPageNum = *(unsigned *)((char *)pageData + offset);
            ixFileHandle.fileHandle.readPage(tempPageNum, tempPage);
            s += treeSerialize(ixFileHandle, type, tempPageNum, tempPage);

            for (index = 0; index < NodesNum; index++) {
                tempPageNum = *(unsigned *)((char *)pageData + offset + 8 * (1 + index));////
                //memset();
                ixFileHandle.fileHandle.readPage(tempPageNum, tempPage);
                s += ",";
                s += treeSerialize(ixFileHandle, type, tempPageNum, tempPage);
            }
            s += "]}";
            free(tempPage);
        }
        else {
            string tempKey = "";
            short int tailOffset = *(short int *)((char *)pageData + 4096 - 2);
            int varcharLength = tailOffset - offset - 4;
            for (int i = 0; i < varcharLength; i++) {
                char ch = *((char *)pageData + offset + 4 + i);
                tempKey += ch;
            }
            s += "\"" + tempKey + "\"";
            for (index = 1; index < NodesNum; index++) {
                tempKey = "";
                tailOffset = *(short int *)((char *)pageData + 4096 - 2 * (index + 1));
                varcharLength = tailOffset - *(short int *)((char *)pageData + 4096 - 2 * index) - 4;
                for (int i = 0; i < varcharLength; i++) {
                    char ch = *((char *)pageData + *(short int *)((char *)pageData + 4096 - 2 * index) + 4 + i);
                    tempKey += ch;
                }
                s += ",\"" + tempKey + "\"";
            }
            s += "],\"children\":[";
            void *tempPage = malloc(4096);
            PageNum tempPageNum = *(unsigned *)((char *)pageData + offset);
            ixFileHandle.fileHandle.readPage(tempPageNum, tempPage);
            s += treeSerialize(ixFileHandle, type, tempPageNum, tempPage);

            for (index = 0; index < NodesNum; index++) {
                tailOffset = *(short int *)((char *)pageData + 4096 - 2 * (index + 1));
                tempPageNum = *(unsigned *)((char *)pageData + tailOffset);////
                ixFileHandle.fileHandle.readPage(tempPageNum, tempPage);
                s += ",";
                s += treeSerialize(ixFileHandle, type, tempPageNum, tempPage);
            }
            s += "]}";
            free(tempPage);
        }
    }
    else {
        s += "{\"keys\":[";
        int offset = 5;
        index = 0;
        if (type == TypeInt) {
            int tempKey = *(int *)((char *)pageData + offset);
            RID tempRID;
            tempRID.pageNum = *(unsigned *)((char *)pageData + offset + 4);
            tempRID.slotNum = *(unsigned *)((char *)pageData + offset + 8);
            index = 1;
            s += "\"" + to_string(tempKey) + ":[(" + to_string(tempRID.pageNum) + "," + to_string(tempRID.slotNum) + ")";
            int before = tempKey;
            for (; index < NodesNum;) {
                tempKey = *(int *)((char *)pageData + offset + 12 * index);
                if (tempKey == before) {
                    tempRID.pageNum = *(unsigned *)((char *)pageData + offset + 12 * index + 4);
                    tempRID.slotNum = *(unsigned *)((char *)pageData + offset + 12 * index + 8);
                    index++;
                    s += ",(" + to_string(tempRID.pageNum) + "," + to_string(tempRID.slotNum) + ")";
                } else {
                    break;
                }
            }

            while (index < NodesNum) {
                int tempKey = *(int *)((char *)pageData + offset + 12 * index);
                tempRID.pageNum = *(unsigned *)((char *)pageData + offset + 12 * index + 4);
                tempRID.slotNum = *(unsigned *)((char *)pageData + offset + 12 * index + 8);
                if (tempKey != before) {
                    s += "]\",\"" + to_string(tempKey) + ":[(" + to_string(tempRID.pageNum) + "," + to_string(tempRID.slotNum) + ")";
                    before = tempKey;
                }
                else {
                    s += ",(" + to_string(tempRID.pageNum) + "," + to_string(tempRID.slotNum) + ")";
                }
                index++;
            }
            s += "]\"]}";
        }
        else if (type == TypeReal) {
            int tempKey = *(float *)((char *)pageData + offset);
            RID tempRID;
            tempRID.pageNum = *(unsigned *)((char *)pageData + offset + 4);
            tempRID.slotNum = *(unsigned *)((char *)pageData + offset + 8);
            index = 1;
            s += "\"" + to_string(tempKey) + ":[(" + to_string(tempRID.pageNum) + "," + to_string(tempRID.slotNum) + ")";
            int before = tempKey;
            for (; index < NodesNum;) {
                tempKey = *(float *)((char *)pageData + offset + 12 * index);
                if (tempKey == before) {
                    tempRID.pageNum = *(unsigned *)((char *)pageData + offset + 12 * index + 4);
                    tempRID.slotNum = *(unsigned *)((char *)pageData + offset + 12 * index + 8);
                    index++;
                    s += ",(" + to_string(tempRID.pageNum) + "," + to_string(tempRID.slotNum) + ")";
                } else {
                    break;
                }
            }

            while (index < NodesNum) {
                int tempKey = *(float *)((char *)pageData + offset + 12 * index);
                tempRID.pageNum = *(unsigned *)((char *)pageData + offset + 8 * index + 4);
                tempRID.slotNum = *(unsigned *)((char *)pageData + offset + 8 * index + 8);
                if (tempKey != before) {
                    s += "]\",\"" + to_string(tempKey) + ":[(" + to_string(tempRID.pageNum) + "," + to_string(tempRID.slotNum) + ")";
                    before = tempKey;
                }
                else {
                    s += ",(" + to_string(tempRID.pageNum) + "," + to_string(tempRID.slotNum) + ")";
                }
                index++;
            }
            s += "]\"]}";

        }
            //varchar situation
        else {
            string tempKey = "";
            short int tailOffset = *(short int *)((char *)pageData + 4096 - 6);
            int varcharLength = tailOffset - offset - 8;
            for (int i = 0; i < varcharLength; i++) {
                char ch = *((char *)pageData + offset + i);
                tempKey += ch;
            }
            RID tempRID;
            tempRID.pageNum = *(unsigned *)((char *)pageData + tailOffset - 8);
            tempRID.slotNum = *(unsigned *)((char *)pageData + tailOffset - 4);
            index = 1;
            s += "\"" + tempKey + ":[(" + to_string(tempRID.pageNum) + "," + to_string(tempRID.slotNum) + ")";
            string before = tempKey;
            for (; index < NodesNum;) {
                tailOffset = *(short int *)((char *)pageData + 4096 - 4 - 2 * (index + 1));
                varcharLength = tailOffset - *(short int *)((char *)pageData + 4096 - 4 - 2 * index) - 8;
                for (int i = 0; i < varcharLength; i++) {
                    char ch = *((char *)pageData + tailOffset - varcharLength - 8 + i);
                    tempKey += ch;
                }
                if (tempKey == before) {
                    tempRID.pageNum = *(unsigned *)((char *)pageData + tailOffset - 8);
                    tempRID.slotNum = *(unsigned *)((char *)pageData + tailOffset - 4);
                    index++;
                    s += ",(" + to_string(tempRID.pageNum) + "," + to_string(tempRID.slotNum) + ")";
                } else {
                    break;
                }
            }

            while (index < NodesNum) {
                tempKey = "";
                tailOffset = *(short int *)((char *)pageData + 4096 - 4 - 2 * (index + 1));
                varcharLength = tailOffset - *(short int *)((char *)pageData + 4096 - 4 - 2 * index) - 8;
                for (int i = 0; i < varcharLength; i++) {
                    char ch = *((char *)pageData + tailOffset - varcharLength - 8 + i);
                    tempKey += ch;
                }
                tempRID.pageNum = *(unsigned *)((char *)pageData + tailOffset - 8);
                tempRID.slotNum = *(unsigned *)((char *)pageData + tailOffset - 4);
                if (tempKey != before) {
                    s += "]\",\"" + tempKey + ":[(" + to_string(tempRID.pageNum) + "," + to_string(tempRID.slotNum) + ")";
                    before = tempKey;
                }
                else {
                    s += ",(" + to_string(tempRID.pageNum) + "," + to_string(tempRID.slotNum) + ")";
                }
                index++;
            }
            s += "]\"]}";
        }
    }

    return s;
}

IX_ScanIterator::IX_ScanIterator() {
    index = 0;
}

IX_ScanIterator::~IX_ScanIterator() {
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {

    if (index >= (int)returnedRID.size()) {
        return IX_EOF;
    }
    rid = returnedRID[index];
    vector<char> vKey = returnedKey[index];
    int offset = 0;
    if (this->type == TypeVarChar) {
        offset = 4;
        int vKeyLength = vKey.size();
        memcpy(key, &vKeyLength, sizeof(int));
    }
    for (unsigned int i = 0; i < vKey.size(); i++) {
        memcpy((char *)key + offset + i, &vKey[i], sizeof(char));
    }
    index ++;
    return 0;
}

RC IX_ScanIterator::close() {
    returnedRID.clear();
    returnedKey.clear();
    index = 0;
    return 0;

}

IXFileHandle::IXFileHandle() {

}

IXFileHandle::~IXFileHandle() {
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    return fileHandle.collectCounterValues(readPageCount,writePageCount,appendPageCount);
}

