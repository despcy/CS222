#include <cmath>
#include "rm.h"
#include <vector>
#include <cstring>
#include "../ix/ix.h"

using namespace std;
RelationManager *RelationManager::_relation_manager = nullptr;

RelationManager &RelationManager::instance() {
    static RelationManager _relation_manager = RelationManager();
    return _relation_manager;
}

RelationManager::RelationManager() = default;

RelationManager::~RelationManager() { delete _relation_manager; }

RelationManager::RelationManager(const RelationManager &) = default;

RelationManager &RelationManager::operator=(const RelationManager &) = default;

RC RelationManager::createCatalog() {
    RecordBasedFileManager &rbfm=RecordBasedFileManager::instance();
    if(rbfm.createFile("Tables.tbl")!=0)return -1;
    if(rbfm.createFile("Columns.tbl")!=0)return -1;
    std::vector<Attribute> tableattrs;
    Attribute tableid{"table-id",TypeInt,4};
    Attribute tablename{"table-name",TypeVarChar,50};
    Attribute filename{"file-name",TypeVarChar,50};
   // Attribute access{"access-rights",TypeVarChar,50};
    tableattrs.push_back(tableid);
    tableattrs.push_back(tablename);
    tableattrs.push_back(filename);
    //tableattrs.push_back(access);
    createTable("Tables",tableattrs);
    Attribute columnname{"column-name",TypeVarChar,50};
    Attribute columntype{"column-type",TypeInt,4};
    Attribute columnlength{"column-length",TypeInt,4};
    Attribute columnposition{"column-position",TypeInt,4};
    std::vector<Attribute> columnattrs;
    columnattrs.push_back(tableid);
    columnattrs.push_back(columnname);
    columnattrs.push_back(columntype);
    columnattrs.push_back(columnlength);
    columnattrs.push_back(columnposition);
    createTable("Columns",columnattrs);
    char* data=(char*)malloc(PAGE_SIZE);
    RID tmp;
    //(1, "Tables", "Tables.tbl")
    data[0]='\x00';
    typeInt2data(data+1,1);
    typeVarchar2data(data+5,6,"Tables");
    typeVarchar2data(data+15,10,"Tables.tbl");
    insertTuple("Tables",data,tmp);
    //(2, "Columns", "Columns.tbl")
    data[0]='\x00';
    typeInt2data(data+1,2);
    typeVarchar2data(data+5,7,"Columns");
    typeVarchar2data(data+16,11,"Columns.tbl");
    insertTuple("Tables",data,tmp);
   //(1, "table-id", TypeInt, 4 , 1)
    data[0]='\x00';
    typeInt2data(data+1,1);
    int len=8;
    typeVarchar2data(data+5,len,"table-id");
    typeInt2data(data+9+len,TypeInt);
    typeInt2data(data+13+len,4);
    typeInt2data(data+17+len,1);
    insertTuple("Columns",data,tmp);
   //(1, "table-name", TypeVarChar, 50, 2)
    data[0]='\x00';
    typeInt2data(data+1,1);
    len=10;
    typeVarchar2data(data+5,len,"table-name");
    typeInt2data(data+9+len,TypeVarChar);
    typeInt2data(data+13+len,50);
    typeInt2data(data+17+len,2);
    insertTuple("Columns",data,tmp);
   //(1, "file-name", TypeVarChar, 50, 3)
    data[0]='\x00';
    typeInt2data(data+1,1);
    len=9;
    typeVarchar2data(data+5,len,"file-name");
    typeInt2data(data+9+len,TypeVarChar);
    typeInt2data(data+13+len,50);
    typeInt2data(data+17+len,3);
    insertTuple("Columns",data,tmp);
   //(2, "table-id", TypeInt, 4, 1)
    data[0]='\x00';
    typeInt2data(data+1,2);
    len=8;
    typeVarchar2data(data+5,len,"table-id");
    typeInt2data(data+9+len,TypeInt);
    typeInt2data(data+13+len,4);
    typeInt2data(data+17+len,1);
    insertTuple("Columns",data,tmp);
   //(2, "column-name",  TypeVarChar, 50, 2)
    data[0]='\x00';
    typeInt2data(data+1,2);
    len=11;
    typeVarchar2data(data+5,len,"column-name");
    typeInt2data(data+9+len,TypeVarChar);
    typeInt2data(data+13+len,50);
    typeInt2data(data+17+len,2);
    insertTuple("Columns",data,tmp);
   //(2, "column-type", TypeInt, 4, 3)
    data[0]='\x00';
    typeInt2data(data+1,2);
    len=11;
    typeVarchar2data(data+5,len,"column-type");
    typeInt2data(data+9+len,TypeInt);
    typeInt2data(data+13+len,4);
    typeInt2data(data+17+len,3);
    insertTuple("Columns",data,tmp);
   //(2, "column-length", TypeInt, 4, 4)
    data[0]='\x00';
    typeInt2data(data+1,2);
    len=13;
    typeVarchar2data(data+5,len,"column-length");
    typeInt2data(data+9+len,TypeInt);
    typeInt2data(data+13+len,4);
    typeInt2data(data+17+len,4);
    insertTuple("Columns",data,tmp);
   //(2, "column-position", TypeInt, 4, 5)
    data[0]='\x00';
    typeInt2data(data+1,2);
    len=15;
    typeVarchar2data(data+5,len,"column-position");
    typeInt2data(data+9+len,TypeInt);
    typeInt2data(data+13+len,4);
    typeInt2data(data+17+len,5);
    insertTuple("Columns",data,tmp);
    free(data);
    return 0;
}

RC RelationManager::deleteCatalog() {
    RecordBasedFileManager &rbfm=RecordBasedFileManager::instance();
    if(rbfm.destroyFile("Tables.tbl")!=0)return -1;
    if(rbfm.destroyFile("Columns.tbl")!=0)return -1;
    return 0;
}

RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
    RecordBasedFileManager &rbfm=RecordBasedFileManager::instance();
    if(rbfm.createFile(tableName+".tbl")!=0)return -1;
    //update table.tbl
    char* data=(char*)malloc(PAGE_SIZE);
    RID tmp;
    //(1, "Tables", "Tables.tbl")
    //TODO:怎么去gettable id？
    int tableid=assignTableID();
    data[0]='\x00';
    typeInt2data(data+1,tableid);
    typeVarchar2data(data+5,tableName.length(),tableName.c_str());
    std::string filename=tableName+".tbl";
    typeVarchar2data(data+9+tableName.length(),tableName.length()+4,filename.c_str());
    insertTuple("Tables",data,tmp);
    //update column.tbl
    for(int i=0;i<attrs.size();i++){
        data[0]='\x00';
        typeInt2data(data+1,tableid);
        typeVarchar2data(data+5,attrs[i].name.length(),attrs[i].name);
        typeInt2data(data+9+attrs[i].name.length(),attrs[i].type);
        typeInt2data(data+13+attrs[i].name.length(),(int)attrs[i].length);
        typeInt2data(data+17+attrs[i].name.length(),i+1);

        insertTuple("Columns",data,tmp);

    }

    return 0;
}

RC RelationManager::deleteTable(const std::string &tableName) {
    if(tableName=="Tables"||tableName=="Columns")return -1;
    RecordBasedFileManager &rbfm=RecordBasedFileManager::instance();

    if(rbfm.destroyFile(tableName+".tbl")!=0)return -1;
    //TODO:delete tuple in table.tbl

    //TODO:delete tuple in column.tbl

    string indexFilename=tableName+".idx";
    IndexManager::instance().destroyFile(indexFilename);

    return 0;
}

RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
//防止recursion call， get attributes of table and column is fixed
if(tableName=="Tables"){
    Attribute tableid{"table-id",TypeInt,4};
    Attribute tablename{"table-name",TypeVarChar,50};
    Attribute filename{"file-name",TypeVarChar,50};
    // Attribute access{"access-rights",TypeVarChar,50};
    attrs.push_back(tableid);
    attrs.push_back(tablename);
    attrs.push_back(filename);

    return 0;
}else if(tableName=="Columns"){
    Attribute tableid{"table-id",TypeInt,4};
    Attribute columnname{"column-name",TypeVarChar,50};
    Attribute columntype{"column-type",TypeInt,4};
    Attribute columnlength{"column-length",TypeInt,4};
    Attribute columnposition{"column-position",TypeInt,4};
    attrs.push_back(tableid);
    attrs.push_back(columnname);
    attrs.push_back(columntype);
    attrs.push_back(columnlength);
    attrs.push_back(columnposition);

    return 0;
}
unsigned tableid=getTableID(tableName);
//search from Columns.tbl
CompOp op=EQ_OP;
std::vector<Attribute> attrs1;
getAttributes("Columns",attrs1);
RM_ScanIterator rmsi;
std::vector<std::string> attrs2;
for(int i=0;i<attrs1.size();i++){
    attrs2.push_back(attrs1[i].name);
}

scan("Columns", "table-id", op, &tableid, attrs2, rmsi);

//read attrs
char* data=(char*)malloc(PAGE_SIZE);
RID tupleRid;
while(rmsi.getNextTuple(tupleRid,data)!=RM_EOF){
    //Same code as printRecord
    Attribute attrtmp;

    int nullIndicatorBytesNum=(int)ceil(attrs2.size()/8.0);
    //RecordBasedFileManager::instance().printRecord(attrs1,data);
    //calculate null indicator bit

    unsigned nameLen=0;
    //remember to skip id
    std::memcpy(&nameLen,(char*)data+4+nullIndicatorBytesNum,4);

    char* name=(char*)malloc(PAGE_SIZE);
    AttrType atttype;
    AttrLength  attrlen;
    std::memcpy(name,(char*)data+8+nullIndicatorBytesNum,nameLen);
    std::memcpy(&atttype,(char*)data+8+nameLen+nullIndicatorBytesNum,4);
    std::memcpy(&attrlen,(char*)data+12+nameLen+nullIndicatorBytesNum,4);
    name[nameLen]='\0';
    std::string namestr(name);

    attrtmp.name=name;
    attrtmp.type=atttype;
    attrtmp.length=attrlen;

    attrs.push_back(attrtmp);
    free(name);
}

rmsi.close();
free(data);


    return 0;
}

RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
    RecordBasedFileManager &rbfm=RecordBasedFileManager::instance();
    FileHandle handler;
    if(rbfm.openFile(tableName+".tbl",handler)!=0)return -1;
    std::vector<Attribute> attrs;
    getAttributes(tableName,attrs);
    if(rbfm.insertRecord(handler,attrs,data,rid)!=0)return -1;
    rbfm.closeFile(handler);
    //QE index file





    //same as print record
    vector<Attribute> recordDescriptor=attrs;
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
            Attribute attribute=attrs[i];
            string indexFileName=tableName+"_"+attribute.name+".idx";
            IXFileHandle ixFileHandle;
            int fileExist=IndexManager::instance().openFile(indexFileName,ixFileHandle);
            //check if not null field
           // std::cout << recordDescriptor[i].name << ": ";
            if ((nullIndicator & mask) == 0) {
                //non null field
                if(fileExist==-1) {

                dataptrOffset+=4;
                }
                else if (recordDescriptor[i].type == TypeInt || recordDescriptor[i].type == TypeReal) {
                    //read 4 bytes directly



                        IndexManager::instance().insertEntry(ixFileHandle,recordDescriptor[i],(char*)data+dataptrOffset,rid);


                    //dataoffset + 4
                    dataptrOffset += 4;
                } else {
                    //read 4 bytes as length, then read data
                    unsigned length;
                    IndexManager::instance().insertEntry(ixFileHandle,recordDescriptor[i],(char*)data+dataptrOffset,rid);
                    std::memcpy(&length, ((char *) data) + dataptrOffset, 4);
                    dataptrOffset += 4;
                    char str[length + 1];
                    std::memcpy(&str, ((char *) data) + dataptrOffset, length);
                    str[length] = '\0';
                   // std::cout << str;

                    dataptrOffset += length;

                }
            } else {
                //null
                //std::cout << "NULL";
            }
            //null field the record len don't change
          //  std::cout << "    ";

            IndexManager::instance().closeFile(ixFileHandle);
            mask /= 2;
            i++;
        }

    }

   // std::cout<<std::endl;

    return 0;
}

RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
    RecordBasedFileManager &rbfm=RecordBasedFileManager::instance();
    FileHandle handler;
    if(rbfm.openFile(tableName+".tbl",handler)!=0)return -1;
    std::vector<Attribute> attrs;
    getAttributes(tableName,attrs);
    void* data=malloc(PAGE_SIZE);
    if(rbfm.readRecord(handler,attrs,rid,data)!=0)return -1;




    //------qe
    //same as print record
    vector<Attribute> recordDescriptor=attrs;
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
            Attribute attribute=attrs[i];
            string indexFileName=tableName+"_"+attribute.name+".idx";
            IXFileHandle ixFileHandle;
            int fileExist=IndexManager::instance().openFile(indexFileName,ixFileHandle);
            //check if not null field
            // std::cout << recordDescriptor[i].name << ": ";
            if ((nullIndicator & mask) == 0) {
                //non null field

                if(fileExist==-1) {

                    dataptrOffset+=4;
                }
                else if (recordDescriptor[i].type == TypeInt || recordDescriptor[i].type == TypeReal) {
                    //read 4 bytes directly



                    IndexManager::instance().deleteEntry(ixFileHandle,recordDescriptor[i],(char*)data+dataptrOffset,rid);


                    //dataoffset + 4
                    dataptrOffset += 4;
                } else {
                    //read 4 bytes as length, then read data
                    unsigned length;
                    IndexManager::instance().deleteEntry(ixFileHandle,recordDescriptor[i],(char*)data+dataptrOffset,rid);
                    std::memcpy(&length, ((char *) data) + dataptrOffset, 4);
                    dataptrOffset += 4;
                    char str[length + 1];
                    std::memcpy(&str, ((char *) data) + dataptrOffset, length);
                    str[length] = '\0';
                    // std::cout << str;

                    dataptrOffset += length;

                }
            } else {
                //null
                //std::cout << "NULL";
            }
            //null field the record len don't change
            //  std::cout << "    ";

            IndexManager::instance().closeFile(ixFileHandle);
            mask /= 2;
            i++;
        }

    }
    //---------
    if(rbfm.deleteRecord(handler,attrs,rid)!=0) return -1;
    free(data);
    return 0;
}

RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
    RecordBasedFileManager &rbfm=RecordBasedFileManager::instance();
    FileHandle handler;
    if(rbfm.openFile(tableName+".tbl",handler)!=0)return -1;
    std::vector<Attribute> attrs;
    getAttributes(tableName,attrs);
    //delete
    void* data1=malloc(PAGE_SIZE);
    if(rbfm.readRecord(handler,attrs,rid,data1)!=0)return -1;




    //------qe
    //same as print record
    vector<Attribute> recordDescriptor=attrs;
    unsigned short fieldLength=recordDescriptor.size();
    int nullIndicatorBytesNum=(int)ceil(fieldLength/8.0);
    unsigned char nullIndicators[nullIndicatorBytesNum];
    int dataptrOffset=0;
    for(int i=0;i<nullIndicatorBytesNum;i++){
        std::memcpy(&nullIndicators[i],(char*)data1+dataptrOffset,1);
        dataptrOffset+=1;
    }
    int i=0;
    for(int j=0;j<nullIndicatorBytesNum;j++) {
        int nullIndicator = nullIndicators[j];
        int mask = 128;

        while ( i < fieldLength && mask!=0) {
            Attribute attribute=attrs[i];
            string indexFileName=tableName+"_"+attribute.name+".idx";
            IXFileHandle ixFileHandle;
            int fileExist=IndexManager::instance().openFile(indexFileName,ixFileHandle);
            //check if not null field
            // std::cout << recordDescriptor[i].name << ": ";
            if ((nullIndicator & mask) == 0) {
                //non null field
                if(fileExist==-1) {

                    dataptrOffset+=4;
                }
                else if (recordDescriptor[i].type == TypeInt || recordDescriptor[i].type == TypeReal) {
                    //read 4 bytes directly



                    IndexManager::instance().deleteEntry(ixFileHandle,recordDescriptor[i],(char*)data1+dataptrOffset,rid);


                    //dataoffset + 4
                    dataptrOffset += 4;
                } else {
                    //read 4 bytes as length, then read data
                    unsigned length;
                    IndexManager::instance().deleteEntry(ixFileHandle,recordDescriptor[i],(char*)data1+dataptrOffset,rid);
                    std::memcpy(&length, ((char *) data1) + dataptrOffset, 4);
                    dataptrOffset += 4;
                    char str[length + 1];
                    std::memcpy(&str, ((char *) data1) + dataptrOffset, length);
                    str[length] = '\0';
                    // std::cout << str;

                    dataptrOffset += length;

                }
            } else {
                //null
                //std::cout << "NULL";
            }
            //null field the record len don't change
            //  std::cout << "    ";

            IndexManager::instance().closeFile(ixFileHandle);
            mask /= 2;
            i++;
        }

    }

    //=====insert====
    //same as print record

     fieldLength=recordDescriptor.size();
     nullIndicatorBytesNum=(int)ceil(fieldLength/8.0);
  //  nullIndicators[nullIndicatorBytesNum];
     dataptrOffset=0;
    for(int i=0;i<nullIndicatorBytesNum;i++){
        std::memcpy(&nullIndicators[i],(char*)data+dataptrOffset,1);
        dataptrOffset+=1;
    }
     i=0;
    for(int j=0;j<nullIndicatorBytesNum;j++) {
        int nullIndicator = nullIndicators[j];
        int mask = 128;

        while ( i < fieldLength && mask!=0) {
            Attribute attribute=attrs[i];
            string indexFileName=tableName+"_"+attribute.name+".idx";
            IXFileHandle ixFileHandle;
            int fileExist=IndexManager::instance().openFile(indexFileName,ixFileHandle);
            //check if not null field
            // std::cout << recordDescriptor[i].name << ": ";
            if ((nullIndicator & mask) == 0) {
                //non null field
                if(fileExist==-1) {

                    dataptrOffset+=4;
                }
                else
                if (recordDescriptor[i].type == TypeInt || recordDescriptor[i].type == TypeReal) {
                    //read 4 bytes directly



                    IndexManager::instance().insertEntry(ixFileHandle,recordDescriptor[i],(char*)data+dataptrOffset,rid);


                    //dataoffset + 4
                    dataptrOffset += 4;
                } else {
                    //read 4 bytes as length, then read data
                    unsigned length;
                    IndexManager::instance().insertEntry(ixFileHandle,recordDescriptor[i],(char*)data+dataptrOffset,rid);
                    std::memcpy(&length, ((char *) data) + dataptrOffset, 4);
                    dataptrOffset += 4;
                    char str[length + 1];
                    std::memcpy(&str, ((char *) data) + dataptrOffset, length);
                    str[length] = '\0';
                    // std::cout << str;

                    dataptrOffset += length;

                }
            } else {
                //null
                //std::cout << "NULL";
            }
            //null field the record len don't change
            //  std::cout << "    ";

            IndexManager::instance().closeFile(ixFileHandle);
            mask /= 2;
            i++;
        }

    }

    //============
    if(rbfm.updateRecord(handler,attrs,data,rid)!=0) return -1;
    rbfm.closeFile(handler);
    return 0;

}

RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
    RecordBasedFileManager &rbfm=RecordBasedFileManager::instance();
    FileHandle handler;
    if(rbfm.openFile(tableName+".tbl",handler)!=0)return -1;
    std::vector<Attribute> attrs;
    getAttributes(tableName,attrs);
    if(rbfm.readRecord(handler,attrs,rid,data)!=0) return -1;
 //   RecordBasedFileManager::instance().printRecord(attrs,data);
    return 0;
}

RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data) {
    RecordBasedFileManager &rbfm=RecordBasedFileManager::instance();

    if(rbfm.printRecord(attrs,data)!=0) return -1;
    return 0;
}

RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                  void *data) {
    RecordBasedFileManager &rbfm=RecordBasedFileManager::instance();
    FileHandle handler;
    if(rbfm.openFile(tableName+".tbl",handler)!=0)return -1;
    std::vector<Attribute> attrs;
    getAttributes(tableName,attrs);
    if(rbfm.readAttribute(handler,attrs,rid,attributeName,data)!=0) return -1;
    return 0;

}

RC RelationManager::scan(const std::string &tableName,
                         const std::string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const std::vector<std::string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator) {
    RecordBasedFileManager &rbfm=RecordBasedFileManager::instance();
    FileHandle handler;
    if(rbfm.openFile(tableName+".tbl",handler)!=0)return -1;
    std::vector<Attribute>attrs;
    getAttributes(tableName,attrs);
    if(RecordBasedFileManager::instance().scan(handler,attrs,conditionAttribute,compOp,value,attributeNames,rm_ScanIterator.iterator)!=0)return -1;

    return 0;
}




// Extra credit work
RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
    return 0;
}

// Extra credit work
RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
    return 0;
}

void RelationManager::typeInt2data(void *data, unsigned value) {
    std::memcpy(data,&value,4);
}

void RelationManager::typeReal2data(void *data, float value) {
    std::memcpy(data,&value,4);
}

void RelationManager::typeVarchar2data(void *data, unsigned length, std::string s) {

    const char* cstr=s.c_str();
    std::memcpy(data,&length,4);
    std::memcpy((char*)data+4,cstr,length);

}

int RelationManager::getTableID(std::string tablename) {
    CompOp op=EQ_OP;
    //  std::vector<Attribute> attrs1;#
    //getAttributes("Tables",attrs1);
    RM_ScanIterator rmsi;
    std::vector<std::string> attrs2;
    // for(int i=0;i<attrs1.size();i++){
    attrs2.push_back("table-id");
    //}
    const char* tablenamec=tablename.c_str();
    char* value = (char*) malloc(tablename.size()+4);
    int length=tablename.size();
    std::memcpy((char *)value,&length, 4);
    std::memcpy((char *)value + 4, tablenamec, length);

    scan("Tables", "table-name", op, value, attrs2, rmsi);

//read attrs
    char* data=(char*)malloc(PAGE_SIZE);
    RID tupleRid;

    while(rmsi.getNextTuple(tupleRid,data)!=RM_EOF){
        //Same code as printRecord


        int tableID=0;
        std::memcpy(&tableID,(char*)data+1,4);

        return tableID;
    }




    free(value);
    rmsi.close();

    return 0;
}


int RelationManager::assignTableID() {
    CompOp op = NO_OP;
    //  std::vector<Attribute> attrs1;
    //getAttributes("Tables",attrs1);
    RM_ScanIterator rmsi;
    std::vector<std::string> attrs2;
    // for(int i=0;i<attrs1.size();i++){
    attrs2.push_back("table-id");
    //}

    scan("Tables", "table-id", op, NULL, attrs2, rmsi);

//read attrs
    char *data = (char *) malloc(PAGE_SIZE);
    RID tupleRid;
    int max = INT_MIN;
    while (rmsi.getNextTuple(tupleRid, data) != RM_EOF) {
        //Same code as printRecord


        int tableID = 0;
        std::memcpy(&tableID, (char *) data + 1, 4);

        if (tableID > max) {
            max = tableID;
        }
    }


    return max + 1;


}
// QE IX related
RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName) {
    //check if table and attr exists
    vector<Attribute> attributes;
    if(getAttributes(tableName,attributes)!=0)return -1;
    int attrNum=0;
    for(;attrNum<attributes.size();attrNum++){
        if(attributes[attrNum].name==attributeName){
            break;
        }
    }
    if(attrNum==attributes.size())return -1;


    //create index file
    string indexfilename=tableName+"_"+attributeName+".idx";
   if( IndexManager::instance().createFile(indexfilename)!=0)return -1;
   IXFileHandle ixFileHandle;    //get the ix file handle
   if( IndexManager::instance().openFile(indexfilename,ixFileHandle)!=0)return -1;

    RM_ScanIterator rmsi;
    Attribute attr=attributes[attrNum];
    vector<string> attrNames;
    attrNames.push_back(attributeName);
    if(scan(tableName,attributeName,NO_OP,NULL,attrNames,rmsi)!=0)return -1;

    //insert the data into the index file, key is the value of the data

    //iterate through rm scan iterator
    char * recordData=(char*)malloc(PAGE_SIZE);
    RID rid;
    while(rmsi.getNextTuple(rid,recordData)!=RM_EOF){
        void* key = malloc(PAGE_SIZE);
        if(attr.type==TypeVarChar){
            memcpy(key,recordData+5,*(int *)(recordData+1));
           if(IndexManager::instance().insertEntry(ixFileHandle,attr,key,rid)!=0)return -1;
        }else{
            //int or real
            memcpy(key,recordData+1,4);
            if(IndexManager::instance().insertEntry(ixFileHandle,attr,key,rid)!=0)return -1;
        }
        free(key);
    }
    free(recordData);
    rmsi.close();



    //close file
    if(IndexManager::instance().closeFile(ixFileHandle)!=0)return -1;
    return 0;
}

RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName) {

    string indexfileName=tableName+"_"+attributeName+".idx";
    if(IndexManager::instance().destroyFile(indexfileName)!=0)return -1;

    return 0;
}

RC RelationManager::indexScan(const std::string &tableName,
                              const std::string &attributeName,
                              const void *lowKey,
                              const void *highKey,
                              bool lowKeyInclusive,
                              bool highKeyInclusive,
                              RM_IndexScanIterator &rm_IndexScanIterator) {
    //check if exist
    //create index file
    string indexfilename=tableName+"_"+attributeName+".idx";
    vector<Attribute> attributes;
    if(getAttributes(tableName,attributes)!=0)return -1;
    int attrNum=0;
    for(;attrNum<attributes.size();attrNum++){
        if(attributes[attrNum].name==attributeName){
            break;
        }
    }
    if(attrNum==attributes.size())return -1;
    Attribute attribute=attributes[attrNum];
    IXFileHandle ixFileHandle;
    if( IndexManager::instance().openFile(indexfilename,ixFileHandle)!=0)return -1;
    rm_IndexScanIterator.ixScanIterator;
    IndexManager::instance().scan(ixFileHandle,attribute,lowKey,highKey,lowKeyInclusive,highKeyInclusive,rm_IndexScanIterator.ixScanIterator);


    return 0;
}
