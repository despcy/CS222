# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.15

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /Applications/CLion.app/Contents/bin/cmake/mac/bin/cmake

# The command to remove a file.
RM = /Applications/CLion.app/Contents/bin/cmake/mac/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /Users/ycx/Documents/GitHub/cs222-fall19-team-38

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /Users/ycx/Documents/GitHub/cs222-fall19-team-38/cmake-build-debug

# Include any dependencies generated for this target.
include CMakeFiles/rbftest_p1.dir/depend.make

# Include the progress variables for this target.
include CMakeFiles/rbftest_p1.dir/progress.make

# Include the compile flags for this target's objects.
include CMakeFiles/rbftest_p1.dir/flags.make

CMakeFiles/rbftest_p1.dir/rbf/rbftest_p1.cc.o: CMakeFiles/rbftest_p1.dir/flags.make
CMakeFiles/rbftest_p1.dir/rbf/rbftest_p1.cc.o: ../rbf/rbftest_p1.cc
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/Users/ycx/Documents/GitHub/cs222-fall19-team-38/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object CMakeFiles/rbftest_p1.dir/rbf/rbftest_p1.cc.o"
	/Library/Developer/CommandLineTools/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/rbftest_p1.dir/rbf/rbftest_p1.cc.o -c /Users/ycx/Documents/GitHub/cs222-fall19-team-38/rbf/rbftest_p1.cc

CMakeFiles/rbftest_p1.dir/rbf/rbftest_p1.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/rbftest_p1.dir/rbf/rbftest_p1.cc.i"
	/Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /Users/ycx/Documents/GitHub/cs222-fall19-team-38/rbf/rbftest_p1.cc > CMakeFiles/rbftest_p1.dir/rbf/rbftest_p1.cc.i

CMakeFiles/rbftest_p1.dir/rbf/rbftest_p1.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/rbftest_p1.dir/rbf/rbftest_p1.cc.s"
	/Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /Users/ycx/Documents/GitHub/cs222-fall19-team-38/rbf/rbftest_p1.cc -o CMakeFiles/rbftest_p1.dir/rbf/rbftest_p1.cc.s

# Object files for target rbftest_p1
rbftest_p1_OBJECTS = \
"CMakeFiles/rbftest_p1.dir/rbf/rbftest_p1.cc.o"

# External object files for target rbftest_p1
rbftest_p1_EXTERNAL_OBJECTS =

rbftest_p1: CMakeFiles/rbftest_p1.dir/rbf/rbftest_p1.cc.o
rbftest_p1: CMakeFiles/rbftest_p1.dir/build.make
rbftest_p1: libRBFM.a
rbftest_p1: libPFM.a
rbftest_p1: CMakeFiles/rbftest_p1.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/Users/ycx/Documents/GitHub/cs222-fall19-team-38/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable rbftest_p1"
	$(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/rbftest_p1.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
CMakeFiles/rbftest_p1.dir/build: rbftest_p1

.PHONY : CMakeFiles/rbftest_p1.dir/build

CMakeFiles/rbftest_p1.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/rbftest_p1.dir/cmake_clean.cmake
.PHONY : CMakeFiles/rbftest_p1.dir/clean

CMakeFiles/rbftest_p1.dir/depend:
	cd /Users/ycx/Documents/GitHub/cs222-fall19-team-38/cmake-build-debug && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /Users/ycx/Documents/GitHub/cs222-fall19-team-38 /Users/ycx/Documents/GitHub/cs222-fall19-team-38 /Users/ycx/Documents/GitHub/cs222-fall19-team-38/cmake-build-debug /Users/ycx/Documents/GitHub/cs222-fall19-team-38/cmake-build-debug /Users/ycx/Documents/GitHub/cs222-fall19-team-38/cmake-build-debug/CMakeFiles/rbftest_p1.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles/rbftest_p1.dir/depend

