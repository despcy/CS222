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
include CMakeFiles/qetest_p04.dir/depend.make

# Include the progress variables for this target.
include CMakeFiles/qetest_p04.dir/progress.make

# Include the compile flags for this target's objects.
include CMakeFiles/qetest_p04.dir/flags.make

CMakeFiles/qetest_p04.dir/qe/qetest_p04.cc.o: CMakeFiles/qetest_p04.dir/flags.make
CMakeFiles/qetest_p04.dir/qe/qetest_p04.cc.o: ../qe/qetest_p04.cc
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/Users/ycx/Documents/GitHub/cs222-fall19-team-38/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object CMakeFiles/qetest_p04.dir/qe/qetest_p04.cc.o"
	/Library/Developer/CommandLineTools/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/qetest_p04.dir/qe/qetest_p04.cc.o -c /Users/ycx/Documents/GitHub/cs222-fall19-team-38/qe/qetest_p04.cc

CMakeFiles/qetest_p04.dir/qe/qetest_p04.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/qetest_p04.dir/qe/qetest_p04.cc.i"
	/Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /Users/ycx/Documents/GitHub/cs222-fall19-team-38/qe/qetest_p04.cc > CMakeFiles/qetest_p04.dir/qe/qetest_p04.cc.i

CMakeFiles/qetest_p04.dir/qe/qetest_p04.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/qetest_p04.dir/qe/qetest_p04.cc.s"
	/Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /Users/ycx/Documents/GitHub/cs222-fall19-team-38/qe/qetest_p04.cc -o CMakeFiles/qetest_p04.dir/qe/qetest_p04.cc.s

# Object files for target qetest_p04
qetest_p04_OBJECTS = \
"CMakeFiles/qetest_p04.dir/qe/qetest_p04.cc.o"

# External object files for target qetest_p04
qetest_p04_EXTERNAL_OBJECTS =

qetest_p04: CMakeFiles/qetest_p04.dir/qe/qetest_p04.cc.o
qetest_p04: CMakeFiles/qetest_p04.dir/build.make
qetest_p04: libQE.a
qetest_p04: libIX.a
qetest_p04: libRM.a
qetest_p04: libRBFM.a
qetest_p04: libPFM.a
qetest_p04: CMakeFiles/qetest_p04.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/Users/ycx/Documents/GitHub/cs222-fall19-team-38/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable qetest_p04"
	$(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/qetest_p04.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
CMakeFiles/qetest_p04.dir/build: qetest_p04

.PHONY : CMakeFiles/qetest_p04.dir/build

CMakeFiles/qetest_p04.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/qetest_p04.dir/cmake_clean.cmake
.PHONY : CMakeFiles/qetest_p04.dir/clean

CMakeFiles/qetest_p04.dir/depend:
	cd /Users/ycx/Documents/GitHub/cs222-fall19-team-38/cmake-build-debug && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /Users/ycx/Documents/GitHub/cs222-fall19-team-38 /Users/ycx/Documents/GitHub/cs222-fall19-team-38 /Users/ycx/Documents/GitHub/cs222-fall19-team-38/cmake-build-debug /Users/ycx/Documents/GitHub/cs222-fall19-team-38/cmake-build-debug /Users/ycx/Documents/GitHub/cs222-fall19-team-38/cmake-build-debug/CMakeFiles/qetest_p04.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles/qetest_p04.dir/depend

