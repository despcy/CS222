cmake_minimum_required(VERSION 3.15)
project(rbf)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)

add_executable(rbf
    pfm.cc
    pfm.h
    rbfm.cc
    rbfm.h
  rbftest_01.cc
   rbftest_02.cc
   rbftest_03.cc
    rbftest_04.cc
   rbftest_05.cc
    rbftest_06.cc
   rbftest_07.cc
   rbftest_08.cc
    rbftest_08b.cc
    rbftest_09.cc
    rbftest_10.cc
    rbftest_11.cc
    rbftest_12.cc
    rbftest_p1.cc
       rbftest_p2.cc
        rbftest_p2b.cc
        rbftest_p2c.cc
        rbftest_p3.cc
        rbftest_p3b.cc
       rbftest_p4.cc
       rbftest_p5.cc
        rbftest_p6.cc
        rbftest_delete.cc
        rbftest_update.cc
    test_util.h)

#
#add_executable(MyExe1 main1.cpp)
#add_executable(MyExe2 main2.cpp)
#add_library(MyLib <all the other files>)
#
#target_link_libraries(MyExe1 MyLib)
#target_link_libraries(MyExe2 MyLib)