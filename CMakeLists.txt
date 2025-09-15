
### 编译说明
cmake_minimum_required(VERSION 3.10)
project(b_plus_tree)

# 在project()之后添加
set(CMAKE_BUILD_TYPE Debug)  # 默认为Debug模式
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")  # 强制包含调试符号

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 主可执行文件
add_executable(main test/main.cpp src/b_plus_tree.cpp src/base_node.cpp src/leaf_node.cpp src/internal_node.cpp)

# 测试可执行文件
add_executable(base_function_test test/base_function_test.cpp src/b_plus_tree.cpp src/base_node.cpp src/leaf_node.cpp src/internal_node.cpp)


target_link_libraries(base_function_test gtest gtest_main pthread)
target_link_libraries(main pthread)

# 包含目录
target_include_directories(main PUBLIC include)
target_include_directories(base_function_test PUBLIC include)