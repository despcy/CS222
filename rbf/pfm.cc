#include <fstream>
#include <iostream>
#include <cstring>
#include "pfm.h"

PagedFileManager *PagedFileManager::_pf_manager = nullptr;

PagedFileManager &PagedFileManager::instance() {
    static PagedFileManager _pf_manager = PagedFileManager();
    return _pf_manager;
}

PagedFileManager::PagedFileManager() = default;

PagedFileManager::~PagedFileManager() { delete _pf_manager; }

PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

RC PagedFileManager::createFile(const std::string &fileName) {


    //check if the file exists
    std::ifstream fileToOpen (fileName, std::ios::in | std::ios::binary);
    if(fileToOpen){
        //file exists, error
        //std::cout<<"Create File Error: File:"<<fileName<<"Already Exists!"<<std::endl;
        fileToOpen.close();
        return -1;

    }
    //create the file
    byte readPageCount[4];
    byte writePageCount[4];
    byte appendPageCount[4];
    byte totalPages[4];
    unsigned rc=0;
    unsigned wc=0;
    unsigned ac=0;
    unsigned tp=0;
    std::memcpy(readPageCount,&rc, sizeof(rc));
    std::memcpy(writePageCount,&wc, sizeof(wc));
    std::memcpy(appendPageCount,&ac, sizeof(ac));
    std::memcpy(totalPages,&tp, sizeof(tp));
    //set the file header metadata
    std::ofstream myFile (fileName, std::ios::out | std::ios::binary);
    myFile.write ((char*)readPageCount, 4);
    myFile.write ((char*)writePageCount, 4);
    myFile.write ((char*)appendPageCount, 4);
    myFile.write ((char*)totalPages, 4);
    return 0;
}

RC PagedFileManager::destroyFile(const std::string &fileName) {
    //check if the file exists
    std::ifstream fileToOpen (fileName, std::ios::in | std::ios::binary);
    if(!fileToOpen){
        //file exists, error
        //std::cout<<"Delete File Error: File:"<<fileName<<"Not Exists!"<<std::endl;

        return -1;

    }
    //delete file
    if( remove(fileName.c_str()) != 0 ) {
        //std::cout<<"Error remove file"<<fileName<<std::endl;
        return -1;
    }

    return 0;

}

RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
    //check if file exists
    std::ifstream fileToOpen (fileName, std::ios::in | std::ios::binary);
    if(!fileToOpen){
        //file exists, error
        //std::cout<<"Open File Error: File:"<<fileName<<"Not Exists!"<<std::endl;

        return -1;

    }
    //check if fileHandle is null
    if(fileHandle.myFile.is_open()){
        //std::cout<<"NonNull file in handler!"<<std::endl;
        return -1;
    }
    //set fileHandle

    fileHandle.myFile=std::fstream(fileName, std::ios::out|std::ios::in | std::ios::binary);
    //TODO: check if the file opens successfully
    byte buffer[4];
    fileHandle.myFile.read((char*)buffer,4);
    std::memcpy(&fileHandle.readPageCounter,buffer, sizeof(buffer));

    fileHandle.myFile.read((char*)buffer,4);
    std::memcpy(&fileHandle.writePageCounter,buffer, sizeof(buffer));

    fileHandle.myFile.read((char*)buffer,4);
    std::memcpy(&fileHandle.appendPageCounter,buffer, sizeof(buffer));

    fileHandle.myFile.read((char*)buffer,4);
    std::memcpy(&fileHandle.RootPage,buffer, sizeof(buffer));

    fileHandle.filename=fileName;

    return 0;
}

RC PagedFileManager::closeFile(FileHandle &fileHandle) {
    //check if fileHandle contains file
    if(!fileHandle.myFile.is_open()){

        return 0;
    }
    //update the file header
    byte readPageCount[4];
    byte writePageCount[4];
    byte appendPageCount[4];
    byte totalPages[4];
    std::memcpy(readPageCount,&fileHandle.readPageCounter, sizeof(fileHandle.readPageCounter));
    std::memcpy(writePageCount,&fileHandle.writePageCounter, sizeof(fileHandle.writePageCounter));
    std::memcpy(appendPageCount,&fileHandle.appendPageCounter, sizeof(fileHandle.appendPageCounter));
    std::memcpy(totalPages,&fileHandle.RootPage, sizeof(fileHandle.RootPage));
    //set the file header metadata
    fileHandle.myFile.seekp(0);
    fileHandle.myFile.write ((char*)readPageCount, 4);
    fileHandle.myFile.write ((char*)writePageCount, 4);
    fileHandle.myFile.write ((char*)appendPageCount, 4);
    fileHandle.myFile.write ((char*)totalPages, 4);


    //close the file
    fileHandle.myFile.close();
    return 0;
}

FileHandle::FileHandle() {
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    RootPage=0;
}

FileHandle::~FileHandle() = default;

RC FileHandle::readPage(PageNum pageNum, void *data) {//page number start with 0

//never do malloc data here

    if(pageNum>=this->appendPageCounter)return -1;
    myFile.seekg(4*4+pageNum*PAGE_SIZE);
    myFile.read((char*)data,PAGE_SIZE);

    readPageCounter++;

    return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data) {

    if(pageNum>=this->appendPageCounter)return -1;
    myFile.seekp(4*4+pageNum*PAGE_SIZE);
    myFile.write((char*)data,PAGE_SIZE);
    writePageCounter++;

    return 0;

}

RC FileHandle::appendPage(const void *data) {

    myFile.seekp(4*4+appendPageCounter*PAGE_SIZE);

    myFile.write((char*)data,PAGE_SIZE);

    appendPageCounter++;

    return 0;
}

unsigned FileHandle::getNumberOfPages() {
    return appendPageCounter;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    readPageCount=readPageCounter;
    writePageCount=writePageCounter;
    appendPageCount=appendPageCounter;
    return 0;
}