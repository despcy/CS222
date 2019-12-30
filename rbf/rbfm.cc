#include "rbfm.h"
#include <iostream>
#include <cstring>
#include <cmath>
RecordBasedFileManager *RecordBasedFileManager::_rbf_manager = nullptr;

RecordBasedFileManager &RecordBasedFileManager::instance() {
    static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() = default;

RecordBasedFileManager::~RecordBasedFileManager() { delete _rbf_manager; }

RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;
//TODO: uncomment the line below would cause an error
//RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

RC RecordBasedFileManager::createFile(const std::string &fileName) {

    return pfm.createFile(fileName);

}

RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
   return pfm.destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
    return pfm.openFile(fileName,fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return pfm.closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, RID &rid) {
    //parse data into record format
    char* record=(char*)malloc(PAGE_SIZE);
    unsigned short recordSize=0;

  //  RecordBasedFileManager::instance().printRecord(recordDescriptor,data);
    if(data2Record(recordDescriptor,data,record,recordSize)!=0)return -1;

    //if page number =0 create a new page
    if(fileHandle.appendPageCounter==0){
        if(creatEmptyPage(fileHandle)!=0)return -1;
    }
    int insertPageNum=fileHandle.appendPageCounter-1;
    //check last page space
    unsigned freeSpace=getPageFreeSpace(fileHandle,fileHandle.appendPageCounter-1);
    //if last page space not sufficient, from first page check space

    if(freeSpace< recordSize){
        unsigned i=0;
        int flag=0;
        for(;i<fileHandle.appendPageCounter-1;i++){
           freeSpace=getPageFreeSpace(fileHandle,i);

           if(freeSpace>= recordSize){
               insertPageNum=i;
               flag=1;
               break;
           }
        }
        //if not space avaliable, create new page
        if(flag==0){
            if(creatEmptyPage(fileHandle)!=0)return -1;
            insertPageNum=fileHandle.appendPageCounter-1;

        }
    }




    //insert record into the page
    rid.pageNum=insertPageNum;
    //assign the value to rid
    if(insertRecordToPage(fileHandle,insertPageNum,record,recordSize,rid.slotNum)!=0)return -1;
    free(record);
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                      const RID &rid, void *data) {
    //find record according to rid

    //if record don't exit, return false



    void* pageData;

    pageData=malloc(PAGE_SIZE);
    if(fileHandle.readPage(rid.pageNum,pageData)!=0)return -1;
    unsigned short recLen=0;
    unsigned short recOff=0;
    getSlotInfo(pageData,rid.slotNum,recLen,recOff);
    if(recLen==0&&recOff==0)return -1;

    char* record=(char*)malloc(PAGE_SIZE);
    if(getRecordFromPage((char*)pageData,rid.slotNum,record)!=0)return -1;



    int recordtype=-1;
    RID linkrid;
    handleRecordHeader(record,recordtype,linkrid);
    if(recordtype==0||recordtype==2){
       // recurse call readRecord
        free(pageData);
        free(record);
       return readRecord(fileHandle,recordDescriptor,linkrid,data);

    }



    //if found, convert record to data format
    // assign value to data

    //打补丁，读头
//    int recordtype=-1;
//    RID linkrid;
//    handleRecordHeader(record,recordtype,linkrid);
//    if(recordtype==0||recordtype==2){
//       // recurse call readRecord
//       readRecord(fileHandle,recordDescriptor,linkrid,data);
//       return 0;
//    }
//    //else is linked from or inserted directly, do aso normal


    //
    if(record2Data(recordDescriptor,record,data)!=0)return -1;
    free(pageData);
    return 0;
}

void RecordBasedFileManager::handleRecordHeader(char *record, int &recordtype, RID &linkrid) {
    void *head = malloc(8);
    memcpy(head, record, 8);
    unsigned short sign;
    memcpy(&sign,head,2);
    memcpy(&linkrid.pageNum,(char*)head+2,4);
    memcpy(&linkrid.slotNum,(char*)head+6,2);
    if(sign==0){//0x0000
        recordtype=0;
    }else if(sign==65535){//0xffff
        recordtype=1;
    }else if(sign==255){//0x00ff
        recordtype=2;
    }else{//'1'->0x4900
        recordtype=-1;
    }

//!!!

    free(head);


}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,const RID &rid) {
    //TODO: judge if the rid.slot num is valied?
    int recordType=0;
    RID realrid=getRealRID(fileHandle,recordDescriptor,rid,recordType);
    void* pageData;

    pageData=malloc(PAGE_SIZE);
    if(fileHandle.readPage(realrid.pageNum,pageData)!=0)return -1;
    unsigned short del_recordLen=0;
    unsigned short del_recordOff=0;
    if(getSlotInfo(pageData,realrid.slotNum,del_recordLen,del_recordOff)!=0)return -1;
    unsigned short freeSpaceOffset=0;
    unsigned short numOfSlots=0;
    if(getPageInfo(pageData,freeSpaceOffset,numOfSlots)!=0)return -1;

    //move the record trunc left
    unsigned short start=del_recordOff+del_recordLen;
    unsigned short end=freeSpaceOffset;
    unsigned short moveBytes=del_recordLen;
    if(lmoveRecordTrunc(pageData,start,end,moveBytes)!=0)return -1;
    //set deleted slot to 0x00
    if(setSlotInfo(pageData,realrid.slotNum,0,0)!=0)return -1;
    //update the header slot offset
    if(setPageInfo(pageData,freeSpaceOffset-del_recordLen,numOfSlots)!=0)return -1;
    //update the offset in other slot
    if(updateSlotsOffsetAfterDelete(pageData,numOfSlots,del_recordOff,del_recordLen)!=0)return -1;
    //write back
    if(fileHandle.writePage(realrid.pageNum,pageData)!=0)return -1;
    free(pageData);
    return 0;
}

RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data) {
    //convert data format into field and print
    unsigned short fieldLength=recordDescriptor.size();
    int nullIndicatorBytesNum=(int)ceil(fieldLength/8.0);
    unsigned char nullIndicators[nullIndicatorBytesNum];
    int dataptrOffset=0;
    for(int i=0;i<nullIndicatorBytesNum;i++){
        std::memcpy(&nullIndicators[i],(char*)data+dataptrOffset,1);
        dataptrOffset+=1;
    }
    int i=0;
    for(int j=0;j<nullIndicatorBytesNum;j++) {
        int nullIndicator = nullIndicators[j];
        int mask = 128;

        while ( i < fieldLength && mask!=0) {
            //check if not null field
            std::cout << recordDescriptor[i].name << ": ";
            if ((nullIndicator & mask) == 0) {
                //non null field
                if (recordDescriptor[i].type == TypeInt || recordDescriptor[i].type == TypeReal) {
                    //read 4 bytes directly

                    if (recordDescriptor[i].type == TypeInt) {
                        int tmp = 0;
                        std::memcpy(&tmp, (char *) data + dataptrOffset, 4);
                        std::cout << tmp;
                    } else {
                        float tmp = 0.0;
                        std::memcpy(&tmp, (char *) data + dataptrOffset, 4);
                        std::cout << tmp;
                    }

                    //dataoffset + 4
                    dataptrOffset += 4;
                } else {
                    //read 4 bytes as length, then read data
                    unsigned length;

                    std::memcpy(&length, ((char *) data) + dataptrOffset, 4);
                    dataptrOffset += 4;
                    char str[length + 1];
                    std::memcpy(&str, ((char *) data) + dataptrOffset, length);
                    str[length] = '\0';
                    std::cout << str;
                    dataptrOffset += length;

                }
            } else {
                //null
                std::cout << "NULL";
            }
            //null field the record len don't change
            std::cout << "    ";


            mask /= 2;
            i++;
        }

    }

    std::cout<<std::endl;

    return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, const RID &rid) {
    int recordtype=-1;
    RID realrid=getRealRID(fileHandle,recordDescriptor,rid,recordtype);
    //get the record length
    unsigned short recordlength=0;
    unsigned short recordOffset=0;

    char* pageData=(char*) malloc(PAGE_SIZE);
    if(fileHandle.readPage(realrid.pageNum,pageData)!=0);
    if(getSlotInfo(pageData,realrid.slotNum,recordlength,recordOffset)!=0)return -1;

    char* newRecord=(char*)malloc(PAGE_SIZE);
    unsigned short newrecordlength=0;
    if(data2Record(recordDescriptor,data,newRecord,newrecordlength)!=0)return -1;

    //all operations are based on the realrid
    //delete then insert
    deleteRecord(fileHandle,recordDescriptor,realrid);
    //------把realrid 的slot占用起来
    //if the page is not same page
    int insertPageNum=realrid.pageNum;
    //check last page space
    unsigned freeSpace=getPageFreeSpace(fileHandle,realrid.pageNum);
    //if last page space not sufficient, from first page check space



    if(freeSpace< newrecordlength){
        RID childRID;
        childRID.slotNum=0;
        childRID.pageNum=0;
        setRecordType(newRecord,1,childRID);

        //for 循环，插入到别的page里
        unsigned i=0;
        int flag=0;
        for(;i<fileHandle.appendPageCounter;i++){
            freeSpace=getPageFreeSpace(fileHandle,i);

            if(freeSpace>= newrecordlength){
                insertPageNum=i;
                flag=1;
                break;
            }//怀疑这里逻辑冲突，新的占用了link slot
        }
        //if not space avaliable, create new page
        if(flag==0){
            if(creatEmptyPage(fileHandle)!=0)return -1;
            insertPageNum=fileHandle.appendPageCounter-1;

        }
        childRID.pageNum=insertPageNum;
        if (insertRecordToPage(fileHandle, insertPageNum, newRecord, newrecordlength,childRID.slotNum ) != 0)return -1;
        //-------------
        char* linkRecord = (char*)malloc(PAGE_SIZE);
        if(recordtype==1||recordtype==2){
            recordtype=2;
        }else if(recordtype==-1){
            recordtype=0;
        }

        setRecordType(linkRecord,recordtype,childRID);

        if (insertRecordToPageGivenRID(fileHandle, realrid.pageNum, linkRecord, 10,realrid) != 0)return -1;



    }else {

        if(recordtype==-1){
            recordtype=-1;
        }else{
            recordtype=1;
        }
        setRecordType(newRecord, recordtype, realrid);
        if (insertRecordToPageGivenRID(fileHandle, realrid.pageNum, newRecord, newrecordlength, realrid) != 0)return -1;
    }








    free(pageData);
    free(newRecord);
    return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                         const RID &rid, const std::string &attributeName, void *data) {

    char* tupleData=(char*)malloc(PAGE_SIZE);
    if(readRecord(fileHandle,recordDescriptor,rid,tupleData)!=0)return -1;
    ((char*)data)[0]='\x00';
//RecordBasedFileManager::instance().printRecord(recordDescriptor,tupleData);
    unsigned short fieldLength=recordDescriptor.size();
    int nullIndicatorBytesNum=(int)ceil(fieldLength/8.0);
    unsigned char nullIndicators[nullIndicatorBytesNum];
    int dataptrOffset=0;
    for(int i=0;i<nullIndicatorBytesNum;i++){
        std::memcpy(&nullIndicators[i],(char*)tupleData+dataptrOffset,1);
        dataptrOffset+=1;
    }
    int i=0;
    for(int j=0;j<nullIndicatorBytesNum;j++) {
        int nullIndicator = nullIndicators[j];
        int mask = 128;

        while ( i < fieldLength && mask!=0) {
            //check if not null field

            if ((nullIndicator & mask) == 0) {
                //non null field
                if (recordDescriptor[i].type == TypeInt || recordDescriptor[i].type == TypeReal) {
                    //read 4 bytes directly

                    if (recordDescriptor[i].type == TypeInt) {

                        if(recordDescriptor[i].name.compare(attributeName)==0){
                            memcpy((char*)data+1,(char *) tupleData + dataptrOffset, 4);
                            free(tupleData);
                            return 0;
                        }
                    } else {

                        if(recordDescriptor[i].name.compare(attributeName)==0){
                            memcpy((char*)data+1,(char *) tupleData + dataptrOffset, 4);
                            free(tupleData);
                            return 0;
                        }
                    }

                    //dataoffset + 4
                    dataptrOffset += 4;
                } else {
                    //read 4 bytes as length, then read data
                    unsigned length;

                    std::memcpy(&length, (char *) tupleData + dataptrOffset, 4);

                    if(recordDescriptor[i].name.compare(attributeName)==0){
                        std::memcpy((char*)data+1, &length, 4);
                        std::memcpy((char*)data+1+4, (char *) tupleData + dataptrOffset+4, length);
                        free(tupleData);
                        return 0;
                    }
                    dataptrOffset += 4;
                    dataptrOffset += length;

                }
            } else {
                if(recordDescriptor[i].name.compare(attributeName)==0){

                    free(tupleData);
                    return 0;
                }
            }
            //null field the record len don't change



            mask /= 2;
            i++;
        }

    }

    free(tupleData);
    return -1;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                const std::vector<std::string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator) {

    std::string filename=fileHandle.filename;
    RecordBasedFileManager::instance().closeFile(fileHandle);

    RecordBasedFileManager::instance().openFile(filename,rbfm_ScanIterator.fileHandle);
    rbfm_ScanIterator.recordDescriptor=recordDescriptor;
    rbfm_ScanIterator.conditionAttribute=conditionAttribute;
    rbfm_ScanIterator.compOp=compOp;

    rbfm_ScanIterator.value=value;

    rbfm_ScanIterator.attributeNames=attributeNames;
    rbfm_ScanIterator.curRID.pageNum=0;
    rbfm_ScanIterator.curRID.slotNum=0;


    return 0;
}


RC RecordBasedFileManager::creatEmptyPage(FileHandle &fileHandle) {
    void* pageData=malloc(PAGE_SIZE);

    setPageInfo(pageData,0,0);
    //write to file
    if(fileHandle.appendPage(pageData)!=0)return  -1;
    free(pageData);
    return 0;
}

unsigned RecordBasedFileManager::getPageFreeSpace(FileHandle &fileHandle, unsigned pageNum) {
    void* pageData;
    pageData=malloc(PAGE_SIZE);//the reason why segmentation fault on test p1
    if(fileHandle.readPage(pageNum,pageData)!=0)return -1;
    unsigned short freeSpaceOffset=0;
    unsigned short numOfSlots=0;
    getPageInfo(pageData,freeSpaceOffset,numOfSlots);
    int freeSpace=PAGE_SIZE-freeSpaceOffset-numOfSlots*2*INFO_BLOCK_SIZE-2*INFO_BLOCK_SIZE;
    freeSpace-=2*INFO_BLOCK_SIZE;
    if(freeSpace<=0)return 0;//segmentation fault 的罪魁祸首
    free(pageData);
    return freeSpace;
}

RC RecordBasedFileManager::insertRecordToPage(FileHandle &fileHandle, unsigned pageNum, char *record,const unsigned recordSize,unsigned &slotNum) {
    void* pageData;
    pageData=malloc(PAGE_SIZE);
    if(fileHandle.readPage(pageNum,pageData)!=0)return -1;

    unsigned short freeSpaceOffset=0;
    unsigned short numOfSlots=0;
    getPageInfo(pageData,freeSpaceOffset,numOfSlots);
    unsigned short recordLength= recordSize;
    unsigned short recordOffset=freeSpaceOffset;
    std::memcpy((char*)pageData+freeSpaceOffset,record,recordLength);
    //char* test=(char*) pageData;
    //search to see if there are empty slot to reuse
    int i=1;
    int flag=0;
    int insertIndex=0;
    for(;i<=numOfSlots;i++){
        unsigned short recordlen=0;
        unsigned short recordoff=0;
        if(getSlotInfo(pageData,i,recordlen,recordoff)!=0)return -1;
        if(recordlen==0&&recordoff==0){
            flag=1;
            insertIndex=i;
            break;
        }
    }
    if(flag==0)//no avaliable reused slot
    {
        createNewSlot(pageData, recordLength, recordOffset, numOfSlots + 1);

        freeSpaceOffset += recordLength;
        setPageInfo(pageData, freeSpaceOffset, numOfSlots + 1);
        slotNum = numOfSlots + 1;
    }else{

        setSlotInfo(pageData,insertIndex,recordLength,recordOffset);
        freeSpaceOffset += recordLength;
        setPageInfo(pageData, freeSpaceOffset, numOfSlots );
        slotNum = insertIndex;
    }



    fileHandle.writePage(pageNum,pageData);

    free(pageData);
    return 0;
}

RC RecordBasedFileManager::getRecordFromPage(const char *pageData, unsigned short slotNum, char *record) {
    unsigned short recordLength=0;
    unsigned short recordOffset=0;

    if(getSlotInfo(pageData,slotNum,recordLength,recordOffset)!=0)return -1;

   // char* p=(char*)(pageData+recordOffset);
    if(recordLength==0&&recordOffset==0)return -1;
    std::memcpy(record,pageData+recordOffset,recordLength);

    return 0;
}

RC RecordBasedFileManager::data2Record(const std::vector<Attribute> &recordDescriptor,const void *data,char *record,
                                       unsigned short &recordSize) {

    unsigned short fieldLength=recordDescriptor.size();
    int nullIndicatorBytesNum=(int)ceil(fieldLength/8.0);
    unsigned char nullIndicators[nullIndicatorBytesNum];
    int dataptrOffset=0;
    for(int i=0;i<nullIndicatorBytesNum;i++){
        std::memcpy(&nullIndicators[i],(char*)data+dataptrOffset,1);
        dataptrOffset+=1;
    }
    int i=0;
    //init the record space
    unsigned short recordLen=0;
   // record=(char*)malloc(INFO_BLOCK_SIZE+INFO_BLOCK_SIZE*(fieldLength+1));
   // std::memcpy(record,fieldLength,)
    recordLen+=INFO_BLOCK_SIZE+INFO_BLOCK_SIZE*(fieldLength+1);//the total length of record

    //init the record filed number header
    std::memcpy(record,&fieldLength,INFO_BLOCK_SIZE);
    unsigned short recordptrOffset=INFO_BLOCK_SIZE;
    //init the first record pointer block
    std::memcpy(record+recordptrOffset,&recordLen,INFO_BLOCK_SIZE);
    recordptrOffset+=INFO_BLOCK_SIZE;

    for(int j=0;j<nullIndicatorBytesNum;j++) {
        int nullIndicator = nullIndicators[j];
        int mask = 128;
        while (i < fieldLength && mask != 0) {
            //check if not null field
            if ((nullIndicator & mask) == 0) {
                //non null field
                if (recordDescriptor[i].type == TypeInt || recordDescriptor[i].type == TypeReal) {
                    //read 4 bytes directly

                    // record=(char*)realloc(record,recordLen+4);

                    std::memcpy(record + recordLen, (char *) data + dataptrOffset, 4);
                    recordLen += 4;

                    //dataoffset + 4
                    dataptrOffset += 4;
                } else {
                    //read 4 bytes as length, then read data
                    unsigned length;

                    std::memcpy(&length, (char *) data + dataptrOffset, 4);

                    dataptrOffset += 4;
                    // record=(char*)realloc(record,recordLen+length);
                    if (length == 0) {//handle the empty string ""
                        length = 1;
                        char emptyStr = '\0';
                        std::memcpy(record + recordLen, &emptyStr, length);
                    } else {
                        std::memcpy(record + recordLen, (char *) data + dataptrOffset, length);
                        dataptrOffset += length;
                    }

                    recordLen += length;
                }
            }
            //null field the record len don't change


            std::memcpy(record + recordptrOffset, &recordLen, INFO_BLOCK_SIZE);
            recordptrOffset += INFO_BLOCK_SIZE;


            mask /= 2;
            i++;
        }

    }

    recordSize=recordLen;

    //打补丁，加头
    void* tmp=malloc(recordSize);
    memcpy(tmp,record,recordSize);
    char* headerfield=(char*)malloc(8);
    headerfield[0]='1';
    memcpy(record,headerfield,8);
    free(headerfield);
    memcpy((char*)record+8,tmp,recordSize);
    recordSize+=8;
    free(tmp);
    return 0;
}

RC RecordBasedFileManager::record2Data(const std::vector<Attribute> &recordDescriptor,const char *record, void *data) {

    //read total field number

    unsigned short totalfields=0;
    unsigned recordPtrOffset=INFO_BLOCK_SIZE+8;//+8打补丁
    std::memcpy(&totalfields,record+8,INFO_BLOCK_SIZE);
    unsigned short nullIndicatorBytes=(unsigned short)ceil(totalfields/8.0);
   // data=malloc(nullIndicatorBytes);
    unsigned indicatorVal=0;
    unsigned dataOffset=nullIndicatorBytes;
    unsigned char nullsIndicator[nullIndicatorBytes];
    int j=0;//for char array nullsIndicator index
    for(int i=0;i<totalfields;i++){
        unsigned short offsetleft=0;
        unsigned short offsetright=0;
        std::memcpy(&offsetleft,record+recordPtrOffset,INFO_BLOCK_SIZE);
        std::memcpy(&offsetright,record+recordPtrOffset+INFO_BLOCK_SIZE,INFO_BLOCK_SIZE);
        offsetleft+=8;
        offsetright+=8;//补丁
        unsigned short fieldLength=offsetright-offsetleft;
        if(fieldLength==0){
            //null value
            indicatorVal+=1;
        }else{

            //field data of length fieldlength
            if(recordDescriptor[i].type==TypeInt||recordDescriptor[i].type==TypeReal){
                //4 byte for data
               // data=realloc(data,dataOffset+4);
                std::memcpy((char*)data+dataOffset,record+offsetleft,4);
                dataOffset+=4;
            }else{
                //4 byte length, then data
                unsigned length=fieldLength;

              //  data=realloc(data,dataOffset+4);
                //if field length==1 and read data equals to '\0' set length =0
                if(length==1){
                    char tmp;
                    std::memcpy(&tmp, record + offsetleft, fieldLength);
                    if(tmp=='\0'){
                        length=0;
                    }
                }


                std::memcpy((char*)data+dataOffset,&length,4);

                dataOffset+=4;

                if(length!=0) {//since length is modified above
                    // data=realloc(data,dataOffset+fieldLength);
                    std::memcpy((char *) data + dataOffset, record + offsetleft, fieldLength);
                    dataOffset += fieldLength;
                }
            }
        }
        indicatorVal<<=1;
        if((i+1)%8==0){

            indicatorVal>>=1;
            nullsIndicator[j]=indicatorVal;
            j++;
            indicatorVal=0;
        }
        recordPtrOffset+=INFO_BLOCK_SIZE;
    }

    if(j<nullIndicatorBytes) {
        indicatorVal>>=1;
        indicatorVal<<=(8-(totalfields%8));
        nullsIndicator[j] = indicatorVal;
    }

    //set indicator bit into data
  //  indicatorVal<<=(nullIndicatorBytes*8-totalfields);//不全byte整数，惊了这里这个std::memcpy整数居然是从低位到高位copy
    std::memcpy(data,&nullsIndicator,nullIndicatorBytes);
   // unsigned test=0;
   // std::memcpy(&test,data,nullIndicatorBytes);

    return 0;
}

RC RecordBasedFileManager::getPageInfo(const void *pageData, unsigned short &freeSpaceOffset, unsigned short &numOfSlots) {

    std::memcpy(&freeSpaceOffset,(char*)pageData+PAGE_SIZE-INFO_BLOCK_SIZE,INFO_BLOCK_SIZE);
    std::memcpy(&numOfSlots,(char*)pageData+PAGE_SIZE-INFO_BLOCK_SIZE*2,INFO_BLOCK_SIZE);
    return 0;
}

RC RecordBasedFileManager::getSlotInfo(const void *pageData, unsigned short slotNum, unsigned short &recordLength,
                                       unsigned short &recordOffset) {
    std::memcpy(&recordLength,(char*)pageData+PAGE_SIZE-2*INFO_BLOCK_SIZE-2*INFO_BLOCK_SIZE*slotNum,INFO_BLOCK_SIZE);
    std::memcpy(&recordOffset,(char*)pageData+PAGE_SIZE-2*INFO_BLOCK_SIZE-2*INFO_BLOCK_SIZE*slotNum+INFO_BLOCK_SIZE,INFO_BLOCK_SIZE);
    return 0;
}

RC RecordBasedFileManager::setSlotInfo(void *pageData, unsigned short slotNum, unsigned short recordLength, unsigned short recordOffset) {

    std::memcpy((char*)pageData+PAGE_SIZE-2*INFO_BLOCK_SIZE-2*INFO_BLOCK_SIZE*slotNum,&recordLength,INFO_BLOCK_SIZE);
    std::memcpy((char*)pageData+PAGE_SIZE-2*INFO_BLOCK_SIZE-2*INFO_BLOCK_SIZE*slotNum+INFO_BLOCK_SIZE,&recordOffset,INFO_BLOCK_SIZE);
    return 0;
}

RC RecordBasedFileManager::createNewSlot(void *pageData, unsigned short recordLength, unsigned short recordOffset,unsigned numOfSlots) {

    return setSlotInfo(pageData,numOfSlots,recordLength,recordOffset);

}

RC RecordBasedFileManager::setPageInfo(void *pageData, unsigned short freeSpaceOffset, unsigned short numOfSlots) {
    //assign free space offset

    std::memcpy((char*)pageData+PAGE_SIZE-INFO_BLOCK_SIZE,&freeSpaceOffset,INFO_BLOCK_SIZE);
    //assign number of record offset

    std::memcpy((char*)pageData+PAGE_SIZE-INFO_BLOCK_SIZE*2,&numOfSlots,INFO_BLOCK_SIZE);
    return 0;
}

RC RecordBasedFileManager::lmoveRecordTrunc(void *pageData, unsigned short start, unsigned short end,
                                            unsigned short bytesToMove) {
    void* tmp=malloc(end-start);
    memcpy(tmp,(char*)pageData+start,end-start);
    memcpy((char*)pageData+start-bytesToMove,tmp,end-start);
    free(tmp);
    return 0;
}

RC RecordBasedFileManager::updateSlotsOffsetAfterDelete(void *pageData,unsigned short slotCount, unsigned short minOffset, unsigned short len) {
    for(int i=1;i<=slotCount;i++){
        unsigned short recordlen=0;
        unsigned short recordoff=0;
        if(getSlotInfo(pageData,i,recordlen,recordoff)!=0)return -1;
        if(recordoff>minOffset) {
            if (setSlotInfo(pageData, i, recordlen, recordoff - len) != 0)return -1;
        }
    }
    return 0;
}
//这个recordtype是底层的recordtype
RID RecordBasedFileManager::getRealRID(FileHandle &fileHandle,const std::vector<Attribute> &recordDescriptor, const RID &rid, int &reocrdType) {
    RID newID;

    char* pageData=(char*)malloc(PAGE_SIZE);
    int rc=fileHandle.readPage(rid.pageNum,pageData);
    char* recordData=(char*)malloc(PAGE_SIZE);
    getRecordFromPage(pageData,rid.slotNum,recordData);

    reocrdType=-1;

    handleRecordHeader(recordData,reocrdType,newID);

    if(reocrdType==0||reocrdType==2){
        free(pageData);
        free(recordData);
        return getRealRID(fileHandle,recordDescriptor,newID,reocrdType);


    }


    free(pageData);
    free(recordData);
    return rid;
}


void RecordBasedFileManager::setRecordType(char *record, int recordType,RID linkrid) {


    if(recordType==0) {
        record[0]='\x00';
        record[1]='\x00';
        memcpy(record+2,&linkrid.pageNum,4);
        //这里应该不用转换成unsigned short
        memcpy(record+6,&linkrid.slotNum,2);
    }else if(recordType==1){
        record[0]='\xff';
        record[1]='\xff';
        memcpy(record+2,&linkrid.pageNum,4);
        //这里应该不用转换成unsigned short
        memcpy(record+6,&linkrid.slotNum,2);
    }else if(recordType==2){

        record[0]='\x00';
        record[1]='\xff';
        memcpy(record+2,&linkrid.pageNum,4);
        //这里应该不用转换成unsignsced short
        memcpy(record+6,&linkrid.slotNum,2);
    }else{
        record[0] = '1';
    }




}

RC RecordBasedFileManager::insertRecordToPageGivenRID(FileHandle &fileHandle, unsigned pageNum, char *record,
                                                      const unsigned recordSize, RID rid) {
    void* pageData;
    pageData=malloc(PAGE_SIZE);
    if(fileHandle.readPage(pageNum,pageData)!=0)return -1;

    unsigned short freeSpaceOffset=0;
    unsigned short numOfSlots=0;
    getPageInfo(pageData,freeSpaceOffset,numOfSlots);
    unsigned short recordLength= recordSize;
    unsigned short recordOffset=freeSpaceOffset;
    std::memcpy((char*)pageData+freeSpaceOffset,record,recordLength);
    //char* test=(char*) pageData;
    //search to see if there are empty slot to reuse


    setSlotInfo(pageData,rid.slotNum,recordLength,recordOffset);
    freeSpaceOffset += recordLength;
    setPageInfo(pageData, freeSpaceOffset, numOfSlots );





    fileHandle.writePage(pageNum,pageData);

    free(pageData);
    return 0;
}


RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
    //check if the next page is needed
    unsigned short freeSpaceOffset =0;
    unsigned short numOfSlots =0;
    char* pageData=(char*)malloc(PAGE_SIZE);
    rid=curRID;
    if(fileHandle.readPage(rid.pageNum,pageData)!=0)return -1;
    if(RecordBasedFileManager::instance().getPageInfo(pageData,freeSpaceOffset,numOfSlots)!=0)return -1;
    if(numOfSlots>=rid.slotNum+1){
        rid.slotNum++;
    }else{

        if(fileHandle.getNumberOfPages()-1==rid.pageNum){
            free(pageData);
            return RBFM_EOF;
        }
        rid.pageNum++;
        rid.slotNum=1;

    }
    //if this is the last tuple in last page,return EOF
    //if need, reset currid
    //read data
    fileHandle.readPage(rid.pageNum,pageData);
    //if slt is 0x0000, get next record in next slot, recur


    unsigned short recordlength=0;
    unsigned short recordoffset=0;
    if(RecordBasedFileManager::instance().getSlotInfo(pageData,rid.slotNum,recordlength,recordoffset)!=0)return -1;
    if(recordlength==0&&recordoffset==0){
        //empty slot
        free(pageData);
        curRID=rid;
        return getNextRecord(rid,data);
    }

    //if header is a median link or has parent, iterate next
    char* record=(char*)malloc(PAGE_SIZE);
    if(RecordBasedFileManager::instance().getRecordFromPage(pageData,rid.slotNum,record)!=0)return -1;

    int recordType=0;
    RID realrid;
    RecordBasedFileManager::instance().handleRecordHeader(record,recordType,realrid);
    if(recordType==1||recordType==2){
        free(record);
        free(pageData);
        curRID=rid;
        return getNextRecord(rid,data);
    }
    if(recordType==0){
        realrid=RecordBasedFileManager::instance().getRealRID(fileHandle,recordDescriptor,rid,recordType);
    }else{
        realrid=rid;
    }
    //get real rid

    fileHandle.readPage(realrid.pageNum,pageData);
    //if real rid slot is 0x0000 same as above
    if(RecordBasedFileManager::instance().getSlotInfo(pageData,realrid.slotNum,recordlength,recordoffset)!=0)return -1;
    if(recordlength==0&&recordoffset==0){
        //empty slot
        free(record);
        free(pageData);
        curRID=rid;
        return getNextRecord(rid,data);
    }


    //read record 会自动找到real id
    if(RecordBasedFileManager::instance().readRecord(fileHandle,recordDescriptor,realrid,record)!=0)return -1;
    //according to comp to check drop or not

    char* data1=(char*)malloc(PAGE_SIZE);
    RecordBasedFileManager::instance().readAttribute(fileHandle,recordDescriptor,realrid,conditionAttribute,data1);
    AttrType type;
    for(int i=0;i<recordDescriptor.size();i++){
        if(recordDescriptor[i].name==conditionAttribute){
            type=recordDescriptor[i].type;
            break;
        }
    }

    if(compare(data1,value,compOp,type)==false){
        free(data1);
        free(record);
        free(pageData);
        curRID=rid;
        return getNextRecord(rid,data);
    }

    free(data1);
    //if this record is ok, return the field in thoses attributes

    free(pageData);


    unsigned short recsize=0;
    char* rec=(char*)malloc(PAGE_SIZE);
    RecordBasedFileManager::instance().data2Record(recordDescriptor,record,rec,recsize);

    curRID=rid;
    //  RecordBasedFileManager::instance().printRecord(recordDescriptor,record);

    formatData(rec,data);
    // RecordBasedFileManager::instance().printRecord(recordDescriptor,data);

    //--------
    free(record);
    free(rec);
    return 0;
}

bool s1BigThans2(std::string s1,std::string s2){
    int minsize=0;
    if(s1.size()>s2.size()){
        minsize=s2.size();
    }else{
        minsize=s1.size();
    }
    for(int i=0;i<minsize;i++){
        if(s1[i]>s2[i]){
            return true;
        }
        if(s1[i]<s2[i]){
            return false;
        }

    }

    if(s1.size()>s2.size())return true;

    return false;
}

bool RBFM_ScanIterator::compare(void *data1,const void *data2, CompOp compOp,AttrType type) {
    if(compOp==NO_OP)return true;
    //NULL
    char* d1=(char*)data1;
    char* d2=(char*)data2;
    if(d1[0]!='\x00'){//NULL
        if(compOp==EQ_OP) {
            if (data2 == NULL)return true;
        }
        return false;
    }

    if(type==TypeVarChar){
        unsigned len=0;
        memcpy(&len,d1+1,4);
        int lend2=0;
        memcpy(&lend2,d2,4);

        std::string data1str(d1+5,len);
        std::string data2str(d2+4,lend2);
        if(compOp==EQ_OP){

            return data1str.compare(data2str)==0;



        }else{
            if(compOp==LT_OP){
                return !s1BigThans2(data1str,data2str);
            }else if(compOp==LE_OP){
                return (data1str.compare(data2str)==0)||!s1BigThans2(data1str,data2str);
            }else if(compOp==GT_OP){
                return s1BigThans2(data1str,data2str);
            }else if (compOp==GE_OP){
                return (data1str.compare(data2str)==0)||s1BigThans2(data1str,data2str);
            }else if(compOp==NE_OP){
                return data1str.compare(data2str)!=0;
            }
        }
    }

    if(type==TypeInt){
        int d1f=0;
        int d2f=0;
        memcpy(&d1f,(char*)data1+1,4);
        memcpy(&d2f,(char*)data2,4);
        if(compOp==EQ_OP){
            return d1f==d2f;
        }else if(compOp==LT_OP){
            return d1f<d2f;
        }else if(compOp==LE_OP){
            return d1f<=d2f;
        }else if(compOp==GT_OP){
            return d1f>d2f;
        }else if (compOp==GE_OP){
            return d1f>=d2f;
        }else if(compOp==NE_OP){
            return d1f!=d2f;
        }
    }

    if(type==TypeReal){
        float d1f=0.0;
        float d2f=0.0;
        memcpy(&d1f,(char*)data1+1,4);
        memcpy(&d2f,(char*)data2,4);
        if(compOp==EQ_OP){
            return d1f==d2f;
        }else if(compOp==LT_OP){
            return d1f<d2f;
        }else if(compOp==LE_OP){
            return d1f<=d2f;
        }else if(compOp==GT_OP){
            return d1f>d2f;
        }else if (compOp==GE_OP){
            return d1f>=d2f;
        }else if(compOp==NE_OP){
            return d1f!=d2f;
        }
    }

    return false;
}
void RBFM_ScanIterator::formatData(char *record,void* data) {
    //recordDescriptor
    //attributeNames
    //record->真的是我自己定义的那个record
    //read total field number
 //改自：record2data
    unsigned short totalfields=attributeNames.size();

    unsigned short nullIndicatorBytes=(unsigned short)ceil(totalfields/8.0);
    // data=malloc(nullIndicatorBytes);
    unsigned indicatorVal=0;
    unsigned dataOffset=nullIndicatorBytes;
    unsigned char nullsIndicator[nullIndicatorBytes];
    int j=0;//for char array nullsIndicator index
    for(int i=0;i<totalfields;i++){
        unsigned recordPtrOffset=INFO_BLOCK_SIZE+8;//+8打补丁
        for(int k=0;k<recordDescriptor.size();k++) {
            if(recordDescriptor[k].name.compare(attributeNames[i])==0) {
                unsigned short offsetleft = 0;
                unsigned short offsetright = 0;
                std::memcpy(&offsetleft, record + recordPtrOffset, INFO_BLOCK_SIZE);
                std::memcpy(&offsetright, record + recordPtrOffset + INFO_BLOCK_SIZE, INFO_BLOCK_SIZE);
                offsetleft += 8;
                offsetright += 8;//补丁
                unsigned short fieldLength = offsetright - offsetleft;
                if (fieldLength == 0) {
                    //null value
                    indicatorVal += 1;
                } else {

                    //field data of length fieldlength
                    if (recordDescriptor[k].type == TypeInt || recordDescriptor[k].type == TypeReal) {
                        //4 byte for data
                        // data=realloc(data,dataOffset+4);
                        std::memcpy((char *) data + dataOffset, record + offsetleft, 4);
                        dataOffset += 4;
                    } else {
                        //4 byte length, then data
                        unsigned length = fieldLength;

                        //  data=realloc(data,dataOffset+4);
                        //if field length==1 and read data equals to '\0' set length =0
                        if (length == 1) {
                            char tmp;
                            std::memcpy(&tmp, record + offsetleft, fieldLength);
                            if (tmp == '\0') {
                                length = 0;
                            }
                        }


                        std::memcpy((char *) data + dataOffset, &length, 4);

                        dataOffset += 4;

                        if (length != 0) {//since length is modified above
                            // data=realloc(data,dataOffset+fieldLength);
                            std::memcpy((char *) data + dataOffset, record + offsetleft, fieldLength);
                            dataOffset += fieldLength;
                        }
                    }
                }
                indicatorVal <<= 1;
                if ((i + 1) % 8 == 0) {

                    indicatorVal >>= 1;
                    nullsIndicator[j] = indicatorVal;
                    j++;
                    indicatorVal = 0;
                }
                break;

            }
            recordPtrOffset += INFO_BLOCK_SIZE;
        }

    }

    if(j<nullIndicatorBytes) {
        indicatorVal>>=1;
        indicatorVal<<=(8-(totalfields%8));
        nullsIndicator[j] = indicatorVal;
    }

    //set indicator bit into data
    //  indicatorVal<<=(nullIndicatorBytes*8-totalfields);//不全byte整数，惊了这里这个std::memcpy整数居然是从低位到高位copy
    std::memcpy(data,&nullsIndicator,nullIndicatorBytes);
    // unsigned test=0;
    // std::memcpy(&test,data,nullIndicatorBytes);



}





