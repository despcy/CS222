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
include CMakeFiles/cli_example_02.dir/depend.make

# Include the progress variables for this target.
include CMakeFiles/cli_example_02.dir/progress.make

# Include the compile flags for this target's objects.
include CMakeFiles/cli_example_02.dir/flags.make

CMakeFiles/cli_example_02.dir/cli/cli_example_02.cc.o: CMakeFiles/cli_example_02.dir/flags.make
CMakeFiles/cli_example_02.dir/cli/cli_example_02.cc.o: ../cli/cli_example_02.cc
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/Users/ycx/Documents/GitHub/cs222-fall19-team-38/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object CMakeFiles/cli_example_02.dir/cli/cli_example_02.cc.o"
	/Library/Developer/CommandLineTools/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/cli_example_02.dir/cli/cli_example_02.cc.o -c /Users/ycx/Documents/GitHub/cs222-fall19-team-38/cli/cli_example_02.cc

CMakeFiles/cli_example_02.dir/cli/cli_example_02.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/cli_example_02.dir/cli/cli_example_02.cc.i"
	/Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /Users/ycx/Documents/GitHub/cs222-fall19-team-38/cli/cli_example_02.cc > CMakeFiles/cli_example_02.dir/cli/cli_example_02.cc.i

CMakeFiles/cli_example_02.dir/cli/cli_example_02.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/cli_example_02.dir/cli/cli_example_02.cc.s"
	/Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /Users/ycx/Documents/GitHub/cs222-fall19-team-38/cli/cli_example_02.cc -o CMakeFiles/cli_example_02.dir/cli/cli_example_02.cc.s

# Object files for target cli_example_02
cli_example_02_OBJECTS = \
"CMakeFiles/cli_example_02.dir/cli/cli_example_02.cc.o"

# External object files for target cli_example_02
cli_example_02_EXTERNAL_OBJECTS =

cli_example_02: CMakeFiles/cli_example_02.dir/cli/cli_example_02.cc.o
cli_example_02: CMakeFiles/cli_example_02.dir/build.make
cli_example_02: libCLI.a
cli_example_02: libQE.a
cli_example_02: libIX.a
cli_example_02: libRM.a
cli_example_02: libRBFM.a
cli_example_02: libPFM.a
cli_example_02: CMakeFiles/cli_example_02.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/Users/ycx/Documents/GitHub/cs222-fall19-team-38/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable cli_example_02"
	$(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/cli_example_02.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
CMakeFiles/cli_example_02.dir/build: cli_example_02

.PHONY : CMakeFiles/cli_example_02.dir/build

CMakeFiles/cli_example_02.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/cli_example_02.dir/cmake_clean.cmake
.PHONY : CMakeFiles/cli_example_02.dir/clean

CMakeFiles/cli_example_02.dir/depend:
	cd /Users/ycx/Documents/GitHub/cs222-fall19-team-38/cmake-build-debug && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /Users/ycx/Documents/GitHub/cs222-fall19-team-38 /Users/ycx/Documents/GitHub/cs222-fall19-team-38 /Users/ycx/Documents/GitHub/cs222-fall19-team-38/cmake-build-debug /Users/ycx/Documents/GitHub/cs222-fall19-team-38/cmake-build-debug /Users/ycx/Documents/GitHub/cs222-fall19-team-38/cmake-build-debug/CMakeFiles/cli_example_02.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles/cli_example_02.dir/depend

