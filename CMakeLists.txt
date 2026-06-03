cmake_minimum_required(VERSION 3.22)
project(grpc_server C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_LINKER_TYPE LLD)
set(CMAKE_EXE_LINKER_FLAGS "-fuse-ld=lld")

# ---- 可执行文件输出到 bin/ ----
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/bin")

find_package(absl REQUIRED)
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)
find_package(OpenSSL REQUIRED)

set(_REFLECTION gRPC::grpc++_reflection)
set(_GRPC_GRPCPP gRPC::grpc++)

if(TARGET protobuf::libprotobuf)
    set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
elseif(TARGET protobuf::protobuf)
    set(_PROTOBUF_LIBPROTOBUF protobuf::protobuf)
else()
    set(_PROTOBUF_LIBPROTOBUF protobuf)
endif()

if(TARGET protobuf::protoc)
    set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
else()
    set(_PROTOBUF_PROTOC ${Protobuf_PROTOC_EXECUTABLE})
endif()

if(DEFINED gRPC_CPP_PLUGIN_EXECUTABLE AND gRPC_CPP_PLUGIN_EXECUTABLE)
    get_filename_component(_GRPC_CPP_PLUGIN_EXECUTABLE "${gRPC_CPP_PLUGIN_EXECUTABLE}" ABSOLUTE)
    message(STATUS "Using gRPC_CPP_PLUGIN_EXECUTABLE: ${_GRPC_CPP_PLUGIN_EXECUTABLE}")
elseif(DEFINED gRPC_GRPC_CPP_PLUGIN_EXECUTABLE AND gRPC_GRPC_CPP_PLUGIN_EXECUTABLE)
    get_filename_component(_GRPC_CPP_PLUGIN_EXECUTABLE "${gRPC_GRPC_CPP_PLUGIN_EXECUTABLE}" ABSOLUTE)
    message(STATUS "Using gRPC_GRPC_CPP_PLUGIN_EXECUTABLE: ${_GRPC_GRPC_CPP_PLUGIN_EXECUTABLE}")
else()
    find_program(_GRPC_CPP_PLUGIN_EXECUTABLE
        NAMES grpc_cpp_plugin
        PATHS /usr/local/bin /usr/bin /opt/homebrew/bin ${CMAKE_PREFIX_PATH}/bin
        DOC "Path to grpc_cpp_plugin executable"
    )
    if(_GRPC_CPP_PLUGIN_EXECUTABLE)
        get_filename_component(_GRPC_CPP_PLUGIN_EXECUTABLE "${_GRPC_CPP_PLUGIN_EXECUTABLE}" ABSOLUTE)
        message(STATUS "Found grpc_cpp_plugin: ${_GRPC_CPP_PLUGIN_EXECUTABLE}")
    else()
        message(FATAL_ERROR "grpc_cpp_plugin not found.")
    endif()
endif()

# ---- Hiredis 路径 ----
set(HIREDIS_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/hiredis/include")
set(HIREDIS_LIBRARY "${CMAKE_CURRENT_SOURCE_DIR}/third_party/hiredis/lib/libhiredis.a")

# ---- Proto 源文件目录 ----
set(PROTO_DIR "${CMAKE_CURRENT_SOURCE_DIR}/proto")

# ---- Proto 生成代码输出目录 ----
set(PROTO_GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY "${PROTO_GEN_DIR}")

# ---- 通用函数：从 proto 生成 C++ 代码 ----
function(add_proto_library NAME PROTO_FILE)
    get_filename_component(proto_abs "${PROTO_FILE}" ABSOLUTE)
    get_filename_component(proto_dir "${proto_abs}" PATH)
    get_filename_component(proto_name "${proto_abs}" NAME_WE)

    set(pb_src  "${PROTO_GEN_DIR}/${proto_name}.pb.cc")
    set(pb_hdr  "${PROTO_GEN_DIR}/${proto_name}.pb.h")
    set(grpc_src "${PROTO_GEN_DIR}/${proto_name}.grpc.pb.cc")
    set(grpc_hdr "${PROTO_GEN_DIR}/${proto_name}.grpc.pb.h")

    add_custom_command(
        OUTPUT "${pb_src}" "${pb_hdr}" "${grpc_src}" "${grpc_hdr}"
        COMMAND ${_PROTOBUF_PROTOC}
        ARGS --cpp_out "${PROTO_GEN_DIR}"
             --grpc_out "${PROTO_GEN_DIR}"
             -I "${proto_dir}"
             --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
             "${proto_abs}"
        DEPENDS "${proto_abs}"
        COMMENT "Generating C++ code from ${proto_name}.proto"
    )

    add_library(${NAME} ${pb_src} ${pb_hdr} ${grpc_src} ${grpc_hdr})
    target_include_directories(${NAME} PUBLIC "${PROTO_GEN_DIR}")
    target_link_libraries(${NAME}
        absl::flags absl::flags_parse absl::log absl::check
        ${_PROTOBUF_LIBPROTOBUF} ${_GRPC_GRPCPP} ${_REFLECTION})
endfunction()

include_directories("${PROTO_GEN_DIR}")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/server")

# ---- 四个 proto 库 ----
add_proto_library(grpc_proto_lib  "${PROTO_DIR}/login.proto")
add_proto_library(db_proto_lib    "${PROTO_DIR}/db.proto")
add_proto_library(auth_proto_lib  "${PROTO_DIR}/auth.proto")

# ---- Login 服务 ----
add_executable(grpc_server server/server.cpp)
target_link_libraries(grpc_server grpc_proto_lib db_proto_lib auth_proto_lib
    absl::flags absl::flags_parse absl::log absl::check
    ${_REFLECTION} ${_GRPC_GRPCPP} ${_PROTOBUF_LIBPROTOBUF})

add_executable(grpc_client server/client.cpp)
target_link_libraries(grpc_client grpc_proto_lib
    absl::flags absl::flags_parse absl::log absl::check
    ${_REFLECTION} ${_GRPC_GRPCPP} ${_PROTOBUF_LIBPROTOBUF})

# ---- Auth 服务（链接 hiredis + openssl）----
add_executable(auth_server server/auth_server.cpp)
target_include_directories(auth_server PRIVATE ${HIREDIS_INCLUDE_DIR})
target_link_libraries(auth_server auth_proto_lib
    absl::flags absl::flags_parse absl::log absl::check
    ${_REFLECTION} ${_GRPC_GRPCPP} ${_PROTOBUF_LIBPROTOBUF}
    OpenSSL::SSL OpenSSL::Crypto
    ${HIREDIS_LIBRARY})
target_link_libraries(auth_server pthread)

# ---- 数据库代理服务 ----
find_library(MYSQL_LIB mysqlclient
    PATHS /usr/lib/x86_64-linux-gnu /usr/lib/aarch64-linux-gnu /usr/lib)

add_executable(db_server server/db_server.cpp server/db_proxy.cpp)
target_link_libraries(db_server db_proto_lib
    absl::flags absl::flags_parse absl::log absl::check
    ${_REFLECTION} ${_GRPC_GRPCPP} ${_PROTOBUF_LIBPROTOBUF}
    ${MYSQL_LIB})

# ---- gRPC Gateway (Go) ----
find_program(GO_EXECUTABLE go PATHS "$ENV{HOME}/.go/bin" "$ENV{HOME}/go/bin" /usr/local/go/bin /usr/bin)
if(GO_EXECUTABLE)
    set(GATEWAY_DIR "${CMAKE_CURRENT_SOURCE_DIR}/gateway")
    add_custom_target(gateway ALL
        COMMAND ${CMAKE_COMMAND} -E env
            PATH="$ENV{HOME}/.go/bin:$ENV{HOME}/go/bin:/usr/local/go/bin:/usr/bin:${PATH}"
            GOROOT="$ENV{HOME}/.go"
            GOPATH="$ENV{HOME}/go"
            ${GO_EXECUTABLE} build -o "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/gateway" .
        WORKING_DIRECTORY "${GATEWAY_DIR}"
        COMMENT "Building Go gRPC gateway..."
    )
else()
    message(WARNING "Go not found; skipping gateway build")
endif()

# ---- msg.proto 库 ----
add_proto_library(msg_proto_lib  "${PROTO_DIR}/msg.proto")

# ---- msgServer (gRPC + WebSocket + Redis) ----
find_package(Boost REQUIRED COMPONENTS json system)
# hiredis: use versioned .so.1 directly (dev package not installed)
if(NOT HIREDIS_LIB)
    if(EXISTS "/usr/lib/x86_64-linux-gnu/libhiredis.so.1")
        set(HIREDIS_LIB "/usr/lib/x86_64-linux-gnu/libhiredis.so.1")
    else()
        find_library(HIREDIS_LIB hiredis)
    endif()
endif()

# hiredis headers — use bundled third_party headers if system headers not found
if(NOT EXISTS "/usr/include/hiredis/hiredis.h")
    if(EXISTS "${HIREDIS_INCLUDE_DIR}/hiredis/hiredis.h")
        message(STATUS "Using bundled hiredis headers from ${HIREDIS_INCLUDE_DIR}")
    else()
        message(WARNING "hiredis headers not found; msg_server may fail to compile")
    endif()
endif()

add_executable(msg_server server/msg_server.cpp)
target_include_directories(msg_server PRIVATE
    ${HIREDIS_INCLUDE_DIR}
    ${Boost_INCLUDE_DIRS})
target_link_libraries(msg_server msg_proto_lib db_proto_lib auth_proto_lib
    absl::flags absl::flags_parse absl::log absl::check
    ${_REFLECTION} ${_GRPC_GRPCPP} ${_PROTOBUF_LIBPROTOBUF}
    ${HIREDIS_LIB}
    Boost::headers Boost::system Boost::json
    pthread)
