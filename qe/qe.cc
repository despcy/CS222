
#include "qe.h"
#include <cmath>
#include <iostream>
#include <string>
#include "../rbf/rbfm.cc"
using namespace std;
//Iterator


//============Filter

Filter::Filter(Iterator *input, const Condition &condition) {
this->iterator=input;
this->filterCondition=condition;
//set attributes
input->getAttributes(this->attributes);//这里是复制上一个iterator里边的这个属性，因为这里应该是多层filter input
//cout<<attributes.size()<<"sizedebug"<<endl;
}

void Filter::getAttributes(std::vector<Attribute> &attrs) const{
    for(int i=0;i<this->attributes.size();i++){
        attrs.push_back(this->attributes[i]);
    }

}

RC Filter::getNextTuple(void *data){
    //
    while(iterator->getNextTuple(data)!=QE_EOF){
        if(isValiedDataFiltedByFilter(data)){
            return 0;//OK. break
        }
    }
    return QE_EOF;
}

bool Filter::isValiedDataFiltedByFilter(void* data) {

    if(attributes.empty())return false;
    void * lvalue;
    AttrType ltype;
    bool islNull;
    void * rvalue;
    AttrType rtype;
    bool isrNull;

    //get the left value and the right value
    bool lfound= false;
    bool rfound= false;
    //复制那一段print的function就行了
    vector<Attribute> recordDescriptor=attributes;

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
            // std::cout << recordDescriptor[i].name << ": ";
            if ((nullIndicator & mask) == 0) {
                //non null field
                if(filterCondition.lhsAttr==recordDescriptor[i].name){
                        islNull=false;
                        lvalue=(char*)data+dataptrOffset;
                        ltype=recordDescriptor[i].type;
                        lfound=true;
                }else if(filterCondition.bRhsIsAttr && filterCondition.rhsAttr==recordDescriptor[i].name){
                    isrNull=false;
                    rvalue=(char*)data+dataptrOffset;
                    rtype=recordDescriptor[i].type;
                    rfound=true;
                }
                if (recordDescriptor[i].type == TypeInt || recordDescriptor[i].type == TypeReal) {
                    //read 4 bytes directly






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
                    // std::cout << str;

                    dataptrOffset += length;

                }
            } else {
                //null
                //std::cout << "NULL";
                if(filterCondition.lhsAttr==recordDescriptor[i].name){
                    islNull=true;

                }else if(filterCondition.bRhsIsAttr && filterCondition.rhsAttr==recordDescriptor[i].name){
                    isrNull=true;

                }
            }
            //null field the record len don't change
            //  std::cout << "    ";


            mask /= 2;
            i++;
        }

    }

    //compare two values

    if(!lfound||(filterCondition.bRhsIsAttr && !rfound))return -1;//can not found left or can not found right attr

    if(!filterCondition.bRhsIsAttr){//assign the value, in this case the rhs data is value
        rvalue=filterCondition.rhsValue.data;
        rtype=filterCondition.rhsValue.type;
            if(filterCondition.rhsValue.data==NULL){
                isrNull=true;
            }else{
                isrNull=false;

            }


    }

    if(ltype!=rtype)return false;

    if(islNull&&isrNull){
        if(filterCondition.op==NO_OP)return true;
        return false;
    }


    return compare(lvalue,rvalue,filterCondition.op,ltype);
}

bool Filter::compare(void *lval, void *rval, CompOp op,AttrType type) {
    if(type==TypeVarChar){
        char* d1=(char*)lval;
        char* d2=(char*)rval;

        unsigned len=0;
        memcpy(&len,d1,4);
        int lend2=0;
        memcpy(&lend2,d2,4);
        CompOp compOp=op;
        std::string data1str(d1+4,len);
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



    }else{
        if (type == TypeInt) {

            switch (op) {
                case EQ_OP://=
                    return (*(int*)lval == *(int*)rval);
                case LT_OP://<
                    return (*(int*)lval < *(int*)rval);
                case GT_OP://>
                    return (*(int*)lval > *(int*)rval);
                case LE_OP://<=
                    return (*(int*)lval <= *(int*)rval);
                case GE_OP://>=
                    return (*(int*)lval >= *(int*)rval);
                case NE_OP://!=
                    return (*(int*)lval != *(int*)rval);
                case NO_OP://no condition
                    return true;
            }
        }else{

            switch (op) {
                case EQ_OP://=
                    return (*(float*)lval == *(float*)rval);
                case LT_OP://<
                    return (*(float*)lval < *(float*)rval);
                case GT_OP://>
                    return (*(float*)lval > *(float*)rval);
                case LE_OP://<=
                    return (*(float*)lval <= *(float*)rval);
                case GE_OP://>=
                    return (*(float*)lval >= *(float*)rval);
                case NE_OP://!=
                    return (*(float*)lval != *(float*)rval);
                case NO_OP://no condition
                    return true;
            }
        }
    }
    return false;
}
Filter::~Filter() {
    attributes.clear();
}
//================Project




Project::Project(Iterator *input, const std::vector<std::string> &attrNames) {
iterator=input;
vector<Attribute> attr;
input->getAttributes(attr);
for(int i=0;i<attr.size();i++){//init the projected attribute
    for(int j=0;j<attrNames.size();j++){
        if(attr[i].name==attrNames[j]){
            attributesToProject.push_back(attr[i]);
            break;
        }
    }
}
}

Project::~Project() {
attributesToProject.clear();
}
RC Project::getNextTuple(void *data) {

    void* tmpData=malloc(PAGE_SIZE);
    if(iterator->getNextTuple(tmpData)==QE_EOF)return QE_EOF;
    vector<Attribute> attr;
    iterator->getAttributes(attr);
    project(tmpData,attr,data);


    free(tmpData);
    return 0;

}

void Project::getAttributes(std::vector<Attribute> &attrs) const{
for(int i=0;i<attributesToProject.size();i++){
    attrs.push_back(attributesToProject[i]);
}
}

void Project::project(void* input,std::vector<Attribute> inputAttr,void* output){
    //set the null indicator bit of the output to 0-> not support null field yet TODO:null
    unsigned int outputFieldLen=attributesToProject.size();
    int outnullIndicatorBytesNum=(int)ceil(outputFieldLen/8.0);
    memset(output,0,outnullIndicatorBytesNum);//set output null indicator
    int outputOffset=outnullIndicatorBytesNum;
    //iterate,
    //复制那一段print的function就行了
    vector<Attribute> recordDescriptor=inputAttr;

    unsigned short fieldLength=recordDescriptor.size();
    int nullIndicatorBytesNum=(int)ceil(fieldLength/8.0);
    unsigned char nullIndicators[nullIndicatorBytesNum];
    int dataptrOffset=0;
    for(int i=0;i<nullIndicatorBytesNum;i++){
        std::memcpy(&nullIndicators[i],(char*)input+dataptrOffset,1);
        dataptrOffset+=1;
    }
    int i=0;
    for(int j=0;j<nullIndicatorBytesNum;j++) {
        int nullIndicator = nullIndicators[j];
        int mask = 128;

        while ( i < fieldLength && mask!=0) {


            //check if not null field
            // std::cout << recordDescriptor[i].name << ": ";
            if ((nullIndicator & mask) == 0) {
                //non null field

                if (recordDescriptor[i].type == TypeInt || recordDescriptor[i].type == TypeReal) {
                    //read 4 bytes directly

                    for(int j=0;j<attributesToProject.size();j++){
                        if(recordDescriptor[i].name==attributesToProject[j].name){
                            memcpy((void*)((char*)output+outputOffset),(void*)((char*)input+dataptrOffset),4);
                            outputOffset+=4;
                        }
                    }




                    //dataoffset + 4
                    dataptrOffset += 4;
                } else {
                    //read 4 bytes as length, then read data
                    unsigned length;

                    std::memcpy(&length, ((char *) input) + dataptrOffset, 4);
                    dataptrOffset += 4;
                    char str[length + 1];
                    std::memcpy(&str, ((char *) input) + dataptrOffset, length);
                    str[length] = '\0';
                    // std::cout << str;

                    for(int j=0;j<attributesToProject.size();j++){
                        if(recordDescriptor[i].name==attributesToProject[j].name){
                            memcpy((void*)((char*)output+outputOffset),(void*)((char*)input+dataptrOffset-4),4+length);
                            outputOffset+=(4+length);
                        }
                    }

                    dataptrOffset += length;

                }
            } else {
                //null
                //std::cout << "NULL";
                cout << "do not support null projection yet";
            }
            //null field the record len don't change



            mask /= 2;
            i++;
        }

    }

}
//==============
//==============BNL=======
BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned numPages) {
leftIter=leftIn;
rightTableIter=rightIn;
joinCondition=condition;
this->numPages=numPages;
leftIn->getAttributes(attrLeft);
rightIn->getAttributes(attrRight);
curBufferSize=0;
for(int i=0;i<attrLeft.size();i++){

    if(attrLeft[i].name==condition.lhsAttr){

        joinType=attrLeft[i].type;
        break;
    }
}
rightIn->setIterator();
}

BNLJoin::~BNLJoin() {
this->intHash.clear();
this->realHash.clear();
this->varCharHash.clear();
this->bufferQueue.clear();
}

RC BNLJoin::getNextTuple(void *data) {
    if(bufferQueue.empty()){
        void* rightTuple=malloc(PAGE_SIZE);
        //add to outputBuffer
        do {

            if(rightTableIter->getNextTuple(rightTuple)!=0){
                if (buildNextHash() != 0) {

                    break;//no more tuples
                }

                rightTableIter->setIterator();
            }
            searchAndAdd(rightTuple);
           // cout<<curBufferSize<<"Curbuf byte size Debug"<<endl;
            //cout<<bufferQueue.size()<<"Debug"<<endl;
        }while(curBufferSize<PAGE_SIZE);//One page for output buffer
        free(rightTuple);

    }

    if(bufferQueue.size()==0)return -1;
    //pop from the queue and add ele to data
    string tuplestr=bufferQueue[0];
    bufferQueue.erase(bufferQueue.begin());
    curBufferSize-=tuplestr.length();
    memcpy(data,(void*)tuplestr.c_str(),tuplestr.length());


    return 0;
}


void BNLJoin::getAttributes(std::vector<Attribute> &attrs) const{
    //leftAttr+right attr
    for(int i=0;i<attrLeft.size();i++){
        attrs.push_back(attrLeft[i]);
    }

    for(int j=0;j<attrRight.size();j++){
        attrs.push_back(attrRight[j]);
    }

}

RC BNLJoin::searchAndAdd(void *const tuple) {
    //tuple is the right tuple, add right tuple to the buffer
    if(joinType==TypeInt) {
        int attrVal;
        int attrSize;
        int righttupleSize;
        readAttributeVal(&attrVal, &attrSize, &righttupleSize, tuple, joinCondition.rhsAttr, attrRight);
        if(intHash.find(attrVal)!=intHash.end()){
            vector<string> leftTuples=intHash.find(attrVal)->second;
            for(int i=0;i<leftTuples.size();i++){
                void* newTuple=malloc(PAGE_SIZE);
                int tupleSize=0;
                void* leftTuple=malloc(PAGE_SIZE);
                memcpy(leftTuple,leftTuples[i].c_str(),leftTuples[i].length());
                joinLeftAndRight(leftTuple,leftTuples[i].length(),tuple,righttupleSize,newTuple,&tupleSize);
                curBufferSize+=tupleSize;
                string newTupleStr="";
                for(int i=0;i<tupleSize;i++){
                    newTupleStr+=*((char*)newTuple+i);
                }
                bufferQueue.push_back(newTupleStr);
                free(newTuple);
                free(leftTuple);
            }
        }

    }else if(joinType==TypeReal){
        float attrVal;
        int attrSize;
        int righttupleSize;
        readAttributeVal(&attrVal, &attrSize, &righttupleSize, tuple, joinCondition.rhsAttr, attrRight);
        if(realHash.find(attrVal)!=realHash.end()){
            vector<string> leftTuples=realHash.find(attrVal)->second;
            for(int i=0;i<leftTuples.size();i++){
                void* newTuple=malloc(PAGE_SIZE);
                int tupleSize=0;
                void* leftTuple=malloc(PAGE_SIZE);
                memcpy(leftTuple,leftTuples[i].c_str(),leftTuples[i].length());
                joinLeftAndRight(leftTuple,leftTuples[i].length(),tuple,righttupleSize,newTuple,&tupleSize);
                curBufferSize+=tupleSize;
                string newTupleStr="";
                for(int i=0;i<tupleSize;i++){
                    newTupleStr+=*((char*)newTuple+i);
                }
                bufferQueue.push_back(newTupleStr);
                free(newTuple);
                free(leftTuple);
            }
        }
    }else{
        //type varchar
        string attrVal;
        int attrSize;
        int righttupleSize;
        readAttributeVal(&attrVal, &attrSize, &righttupleSize, tuple, joinCondition.rhsAttr, attrRight);
        if(varCharHash.find(attrVal)!=varCharHash.end()){
            vector<string> leftTuples=varCharHash.find(attrVal)->second;
            for(int i=0;i<leftTuples.size();i++){
                void* newTuple=malloc(PAGE_SIZE);
                int tupleSize=0;
                void* leftTuple=malloc(PAGE_SIZE);
                memcpy(leftTuple,leftTuples[i].c_str(),leftTuples[i].length());
                joinLeftAndRight(leftTuple,leftTuples[i].length(),tuple,righttupleSize,newTuple,&tupleSize);
                curBufferSize+=tupleSize;
                string newTupleStr="";
                for(int i=0;i<tupleSize;i++){
                    newTupleStr+=*((char*)newTuple+i);
                }
                bufferQueue.push_back(newTupleStr);
                free(newTuple);
                free(leftTuple);
            }
        }
    }
    return 0;
}

RC BNLJoin::joinLeftAndRight(void *const leftTuple,int leftSize, void *const rightTuple,int rightSize, void *newTuple, int *tuplesize) {
    int leftNullIndecatorBytes=ceil((double)attrLeft.size() / 8);
    int rightNullIndecatorBytes=ceil((double)attrRight.size() / 8);
    int newNullIndicatorBytes=ceil(((double)attrLeft.size()+(double)attrRight.size()) / 8);

    memset(newTuple,0,newNullIndicatorBytes);
    memcpy((char*)newTuple+newNullIndicatorBytes,(char*)leftTuple+leftNullIndecatorBytes,leftSize-leftNullIndecatorBytes);
    memcpy((char*)newTuple+newNullIndicatorBytes+leftSize-leftNullIndecatorBytes,(char*)rightTuple+rightNullIndecatorBytes,rightSize-rightNullIndecatorBytes);
    int totalsize=newNullIndicatorBytes+leftSize+rightSize-leftNullIndecatorBytes-rightNullIndecatorBytes;
    memcpy(tuplesize,&totalsize,4);

    return 0;
};

RC INLJoin::joinLeftAndRight(void *const leftTuple, int leftSize, void *const rightTuple, int rightSize, void *newTuple,
                             int *tuplesize) {
    int leftNullIndecatorBytes=ceil((double)attrLeft.size() / 8);
    int rightNullIndecatorBytes=ceil((double)attrRight.size() / 8);
    int newNullIndicatorBytes=ceil(((double)attrLeft.size()+(double)attrRight.size()) / 8);

    memset(newTuple,0,newNullIndicatorBytes);
    memcpy((char*)newTuple+newNullIndicatorBytes,(char*)leftTuple+leftNullIndecatorBytes,leftSize-leftNullIndecatorBytes);
    memcpy((char*)newTuple+newNullIndicatorBytes+leftSize-leftNullIndecatorBytes,(char*)rightTuple+rightNullIndecatorBytes,rightSize-rightNullIndecatorBytes);
    int totalsize=newNullIndicatorBytes+leftSize+rightSize-leftNullIndecatorBytes-rightNullIndecatorBytes;
    memcpy(tuplesize,&totalsize,4);

    return 0;
}

RC BNLJoin::buildNextHash() {
   // return -1 when no more to build
   if(joinType==TypeInt){
       intHash.clear();
       void *leftTuple=malloc(PAGE_SIZE);
       void *attrVal=malloc(4);
       int hashSize=numPages*PAGE_SIZE;
       while(hashSize>0){

           if(leftIter->getNextTuple(leftTuple)!=0){
               free(leftTuple);//no more left to read
               free(attrVal);
               if(intHash.empty()){
                   return -1;
               }
               return 0;
           }
           //add the left tuple to hashmap
            int attrVal;
           int attrSize;
           int tupleSize;
           readAttributeVal(&attrVal,&attrSize,&tupleSize,leftTuple,joinCondition.lhsAttr,attrLeft);
           hashSize-=tupleSize;
           //get key
           string tuple="";
           for(int i=0;i<tupleSize;i++){
               tuple+=*((char*)leftTuple+i);
           }
           intHash[attrVal].push_back(tuple);



       }


       free(leftTuple);
       free(attrVal);
   }else if(joinType==TypeReal){
       realHash.clear();
       void *leftTuple=malloc(PAGE_SIZE);
       void *attrVal=malloc(4);
       int hashSize=numPages*PAGE_SIZE;
       while(hashSize>0){

           if(leftIter->getNextTuple(leftTuple)!=0){
               free(leftTuple);//no more left to read
               free(attrVal);
               if(realHash.empty()){
                   return -1;
               }
               return 0;
           }
           //add the left tuple to hashmap
           float attrVal;
           int attrSize;
           int tupleSize;
           readAttributeVal(&attrVal,&attrSize,&tupleSize,leftTuple,joinCondition.lhsAttr,attrLeft);
           hashSize-=tupleSize;
           //get key
           string tuple="";
           for(int i=0;i<tupleSize;i++){
               tuple+=*((char*)leftTuple+i);
           }
           realHash[attrVal].push_back(tuple);



       }


       free(leftTuple);
       free(attrVal);
   }else{
       //varchar
       varCharHash.clear();
       void *leftTuple=malloc(PAGE_SIZE);
       void *attrVal=malloc(4);
       int hashSize=numPages*PAGE_SIZE;
       while(hashSize>0){

           if(leftIter->getNextTuple(leftTuple)!=0){
               free(leftTuple);//no more left to read
               free(attrVal);
               if(varCharHash.empty()){
                   return -1;
               }
               return 0;
           }
           //add the left tuple to hashmap
           string attrVal;
           int attrSize;
           int tupleSize;
           readAttributeVal(&attrVal,&attrSize,&tupleSize,leftTuple,joinCondition.lhsAttr,attrLeft);
           hashSize-=tupleSize;
           //get key
           string tuple="";
           for(int i=0;i<tupleSize;i++){
               tuple+=*((char*)leftTuple+i);
           }
           varCharHash[attrVal].push_back(tuple);



       }


       free(leftTuple);
       free(attrVal);
   }


    return 0;
}

RC BNLJoin::readAttributeVal(void* attrVal, void* attrSize, int* tupleSize ,void* const tuple, string attribute,vector<Attribute> attrs) {
//Print 改版
    //复制那一段print的function就行了
    vector<Attribute> recordDescriptor=attrs;

    unsigned short fieldLength=recordDescriptor.size();
    int nullIndicatorBytesNum=(int)ceil(fieldLength/8.0);
    unsigned char nullIndicators[nullIndicatorBytesNum];
    int dataptrOffset=0;
    for(int i=0;i<nullIndicatorBytesNum;i++){
        std::memcpy(&nullIndicators[i],(char*)tuple+dataptrOffset,1);
        dataptrOffset+=1;
    }
    int i=0;
    for(int j=0;j<nullIndicatorBytesNum;j++) {
        int nullIndicator = nullIndicators[j];
        int mask = 128;

        while ( i < fieldLength && mask!=0) {


            //check if not null field
            // std::cout << recordDescriptor[i].name << ": ";
            if ((nullIndicator & mask) == 0) {
                //non null field

                if (recordDescriptor[i].type == TypeInt || recordDescriptor[i].type == TypeReal) {
                    //read 4 bytes directly

                    if(recordDescriptor[i].name==attribute){
                        memcpy(attrVal,(char*)tuple+dataptrOffset,4);

                        int IntRealsize=4;
                        memcpy(attrSize,&IntRealsize,4);
                    }




                    //dataoffset + 4
                    dataptrOffset += 4;
                } else {
                    //read 4 bytes as length, then read data
                    unsigned length;

                    std::memcpy(&length, ((char *) tuple) + dataptrOffset, 4);
                    dataptrOffset += 4;
                    char str[length + 1];
                    std::memcpy(&str, ((char *) tuple) + dataptrOffset, length);
                    str[length] = '\0';
                    // std::cout << str;

                    if(recordDescriptor[i].name==attribute){
                        memcpy(attrVal,str,length+1);


                        memcpy(attrSize,&length,4);
                    }

                    dataptrOffset += length;

                }
            } else {
                //null
                //std::cout << "NULL";

            }
            //null field the record len don't change



            mask /= 2;
            i++;
        }

    }

    memcpy(tupleSize,&dataptrOffset,4);
    return 0;
}

RC INLJoin::readAttributeVal(void* attrVal, void* attrSize, int* tupleSize ,void* const tuple, string attribute,vector<Attribute> attrs) {
//Print 改版
    //复制那一段print的function就行了
    vector<Attribute> recordDescriptor=attrs;

    unsigned short fieldLength=recordDescriptor.size();
    int nullIndicatorBytesNum=(int)ceil(fieldLength/8.0);
    unsigned char nullIndicators[nullIndicatorBytesNum];
    int dataptrOffset=0;
    for(int i=0;i<nullIndicatorBytesNum;i++){
        std::memcpy(&nullIndicators[i],(char*)tuple+dataptrOffset,1);
        dataptrOffset+=1;
    }
    int i=0;
    for(int j=0;j<nullIndicatorBytesNum;j++) {
        int nullIndicator = nullIndicators[j];
        int mask = 128;

        while ( i < fieldLength && mask!=0) {


            //check if not null field
            // std::cout << recordDescriptor[i].name << ": ";
            if ((nullIndicator & mask) == 0) {
                //non null field

                if (recordDescriptor[i].type == TypeInt || recordDescriptor[i].type == TypeReal) {
                    //read 4 bytes directly

                    if(recordDescriptor[i].name==attribute){
                        memcpy(attrVal,(char*)tuple+dataptrOffset,4);

                        int IntRealsize=4;
                        memcpy(attrSize,&IntRealsize,4);
                    }




                    //dataoffset + 4
                    dataptrOffset += 4;
                } else {
                    //read 4 bytes as length, then read data
                    unsigned length;

                    std::memcpy(&length, ((char *) tuple) + dataptrOffset, 4);
                    dataptrOffset += 4;
                    char str[length + 1];
                    std::memcpy(&str, ((char *) tuple) + dataptrOffset, length);
                    str[length] = '\0';
                    // std::cout << str;

                    if(recordDescriptor[i].name==attribute){
                        memcpy(attrVal,str,length+1);


                        memcpy(attrSize,&length,4);
                    }

                    dataptrOffset += length;

                }
            } else {
                //null
                //std::cout << "NULL";

            }
            //null field the record len don't change



            mask /= 2;
            i++;
        }

    }

    memcpy(tupleSize,&dataptrOffset,4);
    return 0;
}

RC Aggregate::readAttributeVal(void* attrVal, void* attrSize, int* tupleSize ,void* const tuple, string attribute,vector<Attribute> attrs) {
//Print 改版
    //复制那一段print的function就行了
    vector<Attribute> recordDescriptor=attrs;

    unsigned short fieldLength=recordDescriptor.size();
    int nullIndicatorBytesNum=(int)ceil(fieldLength/8.0);
    unsigned char nullIndicators[nullIndicatorBytesNum];
    int dataptrOffset=0;
    for(int i=0;i<nullIndicatorBytesNum;i++){
        std::memcpy(&nullIndicators[i],(char*)tuple+dataptrOffset,1);
        dataptrOffset+=1;
    }
    int i=0;
    for(int j=0;j<nullIndicatorBytesNum;j++) {
        int nullIndicator = nullIndicators[j];
        int mask = 128;

        while ( i < fieldLength && mask!=0) {


            //check if not null field
            // std::cout << recordDescriptor[i].name << ": ";
            if ((nullIndicator & mask) == 0) {
                //non null field

                if (recordDescriptor[i].type == TypeInt || recordDescriptor[i].type == TypeReal) {
                    //read 4 bytes directly

                    if(recordDescriptor[i].name==attribute){
                        memcpy(attrVal,(char*)tuple+dataptrOffset,4);

                        int IntRealsize=4;
                        memcpy(attrSize,&IntRealsize,4);
                    }




                    //dataoffset + 4
                    dataptrOffset += 4;
                } else {
                    //read 4 bytes as length, then read data
                    unsigned length;

                    std::memcpy(&length, ((char *) tuple) + dataptrOffset, 4);
                    dataptrOffset += 4;
                    char str[length + 1];
                    std::memcpy(&str, ((char *) tuple) + dataptrOffset, length);
                    str[length] = '\0';
                    // std::cout << str;

                    if(recordDescriptor[i].name==attribute){
                        memcpy(attrVal,str,length+1);


                        memcpy(attrSize,&length,4);
                    }

                    dataptrOffset += length;

                }
            } else {
                //null
                //std::cout << "NULL";

            }
            //null field the record len don't change



            mask /= 2;
            i++;
        }

    }

    memcpy(tupleSize,&dataptrOffset,4);
    return 0;
}

//=================INL=========

INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
this->leftIter=leftIn;
this->rightIter=rightIn;
this->joinCondition=condition;
leftIn->getAttributes(attrLeft);
rightIn->getAttributes(attrRight);
for(int i=0;i<attrLeft.size();i++){
    if(attrLeft[i].name==condition.lhsAttr){
        joinType=attrLeft[i].type;
        break;
    }
};
}
RC INLJoin::getNextTuple(void *data) {
while(outputBuffer.empty()){
    //go to next left tuple
    void* leftTuple=malloc(PAGE_SIZE);
    if(leftIter->getNextTuple(leftTuple)!=0){
        free(leftTuple);
        return -1;
    }
    void* leftattVal=malloc(PAGE_SIZE);
    int leftAttrSize;
    int leftTupleSize;
    readAttributeVal(leftattVal,&leftAttrSize,&leftTupleSize,leftTuple,joinCondition.lhsAttr,attrLeft);
    rightIter->setIterator(leftattVal,leftattVal,true,true);

    //add some join records into the output buffer
    void * rightTuple=malloc(PAGE_SIZE);
    while(rightIter->getNextTuple(rightTuple)==0){
        void* rightattrval=malloc(PAGE_SIZE);
        int rightattrSize;
        int righttupleSize;
        readAttributeVal(rightattrval,&rightattrSize,&righttupleSize,rightTuple,joinCondition.rhsAttr,attrRight);
        void* newTuple=malloc(PAGE_SIZE);
        int newTupleSize;
        joinLeftAndRight(leftTuple,leftTupleSize,rightTuple,righttupleSize,newTuple,&newTupleSize);
        string out="";
        for(int i=0;i<newTupleSize;i++){
            out+=*((char*)newTuple+i);
        }
        outputBuffer.push_back(out);
        free(rightattrval);
        free(newTuple);

    }




    free(leftTuple);
    free(rightTuple);
    free(leftattVal);

}

string jointRec=outputBuffer[0];
memcpy(data,jointRec.c_str(),jointRec.length());
outputBuffer.erase(outputBuffer.begin());
return 0;


}

void INLJoin::getAttributes(std::vector<Attribute> &attrs) const  {
    for(int i=0;i<attrLeft.size();i++){
        attrs.push_back(attrLeft[i]);
    }

    for(int j=0;j<attrRight.size();j++){
        attrs.push_back(attrRight[j]);
    }
}

//=============Aggregate======Only Numeric, return single value=
Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op) {
this->input=input;
this->aggregateAttr=aggAttr;
this->operation=op;
input->getAttributes(inputAttrs);
};

RC Aggregate::getNextTuple(void *data) {
    void* tuple=malloc(PAGE_SIZE);
    if(input->getNextTuple(tuple)==-1){
        //set data to 0
        memset(data,0,5);
        return -1;
    }
    void* attrVal=malloc(PAGE_SIZE);
    int attrsize=0;
    int tuplesize=0;
    readAttributeVal(attrVal,&attrsize,&tuplesize,tuple,aggregateAttr.name,inputAttrs);
    memcpy((char*)data+1,attrVal,4);
    free(tuple);
    free(attrVal);
switch (operation){
    case MIN:
        min(data,input);
        break;
    case MAX:
        max(data,input);
        break;

    case COUNT:
        count(data,input);
        break;

    case SUM:
        sum(data,input);
        break;

    case AVG:
        average(data,input);
        break;
    default:
        break;
}


return 0;

};

Aggregate::~Aggregate() {

};

void Aggregate::getAttributes(std::vector<Attribute> &attrs) const{
Attribute attrtmp;
attrtmp.type=TypeReal;
attrtmp.length=aggregateAttr.length;
    switch (operation){
        case MIN:
            attrtmp.name="MIN("+aggregateAttr.name+")";
            break;
        case MAX:
            attrtmp.name="MAX("+aggregateAttr.name+")";
            break;

        case COUNT:
            attrtmp.name="COUNT("+aggregateAttr.name+")";
            break;

        case SUM:
            attrtmp.name="SUM("+aggregateAttr.name+")";
            break;

        case AVG:
            attrtmp.name="AVG("+aggregateAttr.name+")";
            break;
        default:
            break;
    }

//    if(operation==AVG){
//        attrtmp.type=TypeReal;
//    }

    attrs.push_back(attrtmp);
};

RC Aggregate::min(void *data, Iterator *input) {
if(aggregateAttr.type==TypeInt){
    int min=*(int*)((char*)data+1);
    void* intData=malloc(PAGE_SIZE);
    while(input->getNextTuple(intData)==0){
        int attrVal;
        int attrsize;
        int tuplesize;
        readAttributeVal(&attrVal,&attrsize,&tuplesize,intData,aggregateAttr.name,inputAttrs);
        min=std::min(min,attrVal);
    }
    memset(data,0,1);
    float result=(float)min;
    memcpy((char*)data+1,&result,4);
    free(intData);
}else{
    float min=*(float*)((char*)data+1);
    void* intData=malloc(PAGE_SIZE);
    while(input->getNextTuple(intData)==0){
        float attrVal=0;
        int attrsize=0;
        int tuplesize=0;
        readAttributeVal(&attrVal,&attrsize,&tuplesize,intData,aggregateAttr.name,inputAttrs);
        min=std::min(min,attrVal);
    }
    memset(data,0,1);
    float result=(float)min;
    memcpy((char*)data+1,&result,4);
    free(intData);
}
return 0;
};

RC Aggregate::max(void *data, Iterator *input) {
    if(aggregateAttr.type==TypeInt){
        int max=*(int*)((char*)data+1);
        void* intData=malloc(PAGE_SIZE);
        while(input->getNextTuple(intData)==0){
            int attrVal=0;
            int attrsize=0;
            int tuplesize=0;
            readAttributeVal(&attrVal,&attrsize,&tuplesize,intData,aggregateAttr.name,inputAttrs);
           // cout<<attrVal<<endl;
            max=std::max(max,attrVal);
        }
        memset(data,0,1);
        float result=(float)max;
        memcpy((char*)data+1,&result,4);
        free(intData);
    }else{
        float max=*(float*)((char*)data+1);
        void* intData=malloc(PAGE_SIZE);
        while(input->getNextTuple(intData)==0){
            float attrVal=0;
            int attrsize=0;
            int tuplesize=0;
            readAttributeVal(&attrVal,&attrsize,&tuplesize,intData,aggregateAttr.name,inputAttrs);
            max=std::max(max,attrVal);
        }
        memset(data,0,1);
        float result=(float)max;
        memcpy((char*)data+1,&result,4);
        free(intData);
    }

    return 0;
};

RC Aggregate::count(void *data, Iterator *input) {
int cnt=1;
while(input->getNextTuple(data)==0){
    cnt++;
}
    memset(data,0,1);
    float result=(float)cnt;
    cout<<cnt<<endl;
    memcpy((char*)data+1,&result,4);

return 0;
};

RC Aggregate::sum(void *data, Iterator *input) {
    if(aggregateAttr.type==TypeInt){
        int cnt=*(int*)((char*)data+1);
        void* intData=malloc(PAGE_SIZE);
        while(input->getNextTuple(intData)==0){
            int attrVal=0;
            int attrsize=0;
            int tuplesize=0;
            readAttributeVal(&attrVal,&attrsize,&tuplesize,intData,aggregateAttr.name,inputAttrs);
            cnt+=attrVal;
        }
        memset(data,0,1);
        float result=(float)cnt;
        memcpy((char*)data+1,&result,4);
        free(intData);
    }else{
        float cnt=*(int*)((char*)data+1);
        void* intData=malloc(PAGE_SIZE);
        while(input->getNextTuple(intData)==0){
            float attrVal=0;
            int attrsize=0;
            int tuplesize=0;
            readAttributeVal(&attrVal,&attrsize,&tuplesize,intData,aggregateAttr.name,inputAttrs);
            cnt+=attrVal;
        }
        memset(data,0,1);
        float result=(float)cnt;
        memcpy((char*)data+1,&result,4);
        free(intData);
    }

    return 0;
};

RC Aggregate::average(void *data, Iterator *input) {

    if(aggregateAttr.type==TypeInt){
        vector<int> nums;
        int cnt=*(int*)((char*)data+1);
        nums.push_back(cnt);

        void* intData=malloc(PAGE_SIZE);
        while(input->getNextTuple(intData)==0){
            int attrVal=0;
            int attrsize=0;
            int tuplesize=0;
            readAttributeVal(&attrVal,&attrsize,&tuplesize,intData,aggregateAttr.name,inputAttrs);
            nums.push_back(attrVal);
        }



        float result=0;
        for(int i=0;i<nums.size();i++){
            result+=nums[i]*1.0;
        }
        result/=nums.size();
        memset(data,0,1);
        memcpy((char*)data+1,&result,4);
        free(intData);
    }else{
        vector<float> nums;
         float cnt=*(float*)((char*)data+1);
        nums.push_back(cnt);

        void* intData=malloc(PAGE_SIZE);
        while(input->getNextTuple(intData)==0){
            float attrVal=0;
            int attrsize=0;
            int tuplesize=0;
            readAttributeVal(&attrVal,&attrsize,&tuplesize,intData,aggregateAttr.name,inputAttrs);
            nums.push_back(attrVal);
        }


        float result=0.0;
        for(int i=0;i<nums.size();i++){
            result+=nums[i];
        }
        result/=nums.size();
        memset(data,0,1);
        memcpy((char*)data+1,&result,4);
        free(intData);
    }

    return 0;
};