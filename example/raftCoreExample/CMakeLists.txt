set(SRC_LIST raftKvDB.cpp)

add_executable(raftCoreRun ${SRC_LIST})
target_link_libraries(raftCoreRun ${MUDUO_PATH}/lib/libmuduo_net.a)
target_link_libraries(raftCoreRun ${MUDUO_PATH}/lib/libmuduo_base.a)
target_link_libraries(raftCoreRun skip_list_on_raft rpc_lib protobuf pthread )

#################################

set(SRC_LIST2 caller.cpp)
add_executable(callerMain ${src_raftClerk} ${SRC_LIST2}  ${src_common})
target_link_libraries(callerMain skip_list_on_raft  protobuf boost_serialization )
