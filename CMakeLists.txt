cmake_minimum_required(VERSION 3.16)
project(hh_communicator)
set(CMAKE_CXX_STANDARD 20)

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS 1)

option(BUILD_COMM_MAIN "Compiles main function for testing changes to API" OFF)

include(FetchContent)
FetchContent_Declare(
    hedgehog
    GIT_REPOSITORY https://github.com/usnistgov/hedgehog.git
    GIT_TAG        master
    FIND_PACKAGE_ARGS NAMES hedgehog
)
FetchContent_MakeAvailable(hedgehog)

find_package(MPI REQUIRED)

if (NOT MSVC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wextra -Werror -Wuninitialized -Wconversion -g")
	# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wextra -Werror -Wuninitialized -Wno-sign-compare -pedantic -O3")
endif (NOT MSVC)

# define hedgehog::comm library
add_library(hh_communicator INTERFACE)
target_include_directories(hh_communicator INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/src/communicator)
target_link_libraries(hh_communicator INTERFACE hedgehog::hedgehog MPI::MPI_CXX)
add_library(hedgehog::comm ALIAS hh_communicator)

# executable
if(BUILD_COMM_MAIN)
    add_executable(hh_communicator_main src/main.cpp)
    target_link_libraries(hh_communicator_main PUBLIC hedgehog::comm)
endif(BUILD_COMM_MAIN)
