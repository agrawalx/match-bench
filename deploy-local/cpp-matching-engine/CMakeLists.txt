cmake_minimum_required(VERSION 3.14)

project(Orderbook LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Build the unit tests (and fetch googletest) only when explicitly requested.
# OFF by default so the platform build-worker `cmake -B out` configure step does
# NOT trigger a network FetchContent during an offline/sandboxed build.
option(ORDERBOOK_BUILD_TESTS "Build googletest unit tests" OFF)

add_library(orderbook_lib
    src/orderbook.cpp
)
target_include_directories(orderbook_lib PUBLIC include)

# --- IICPC contestant target: FIX 4.2 front-end over the matching engine ---
# Output binary lands at out/matching_engine, which the platform cpp Dockerfile
# copies (COPY --from=builder /build/out/{{.Target}}). Name must equal
# benchmark.yaml build.target.
find_package(Threads REQUIRED)
add_executable(matching_engine src/fix_server.cpp)
target_link_libraries(matching_engine PRIVATE orderbook_lib Threads::Threads)
target_include_directories(matching_engine PRIVATE include)
target_compile_options(matching_engine PRIVATE -O3)

# --- Original demo executable (kept; not built by the platform target) ---
add_executable(orderbook_main src/main.cpp)
target_link_libraries(orderbook_main PRIVATE orderbook_lib)

# --- Tests (opt-in: -DORDERBOOK_BUILD_TESTS=ON) ---
if(ORDERBOOK_BUILD_TESTS)
    include(FetchContent)
    FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG        v1.16.0
    )
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)

    enable_testing()

    add_executable(test_order tests/test_order.cpp)
    target_link_libraries(test_order PRIVATE orderbook_lib GTest::gtest_main)
    target_include_directories(test_order PRIVATE include)

    add_executable(test_orderbook_basic tests/test_orderbook_basic.cpp)
    target_link_libraries(test_orderbook_basic PRIVATE orderbook_lib GTest::gtest_main)
    target_include_directories(test_orderbook_basic PRIVATE include)

    add_executable(test_orderbook_types tests/test_orderbook_types.cpp)
    target_link_libraries(test_orderbook_types PRIVATE orderbook_lib GTest::gtest_main)
    target_include_directories(test_orderbook_types PRIVATE include)

    add_executable(test_invariants tests/test_invariants.cpp)
    target_link_libraries(test_invariants PRIVATE orderbook_lib GTest::gtest_main)
    target_include_directories(test_invariants PRIVATE include)

    include(GoogleTest)
    gtest_discover_tests(test_order)
    gtest_discover_tests(test_orderbook_basic)
    gtest_discover_tests(test_orderbook_types)
    gtest_discover_tests(test_invariants)
endif()
