find_package(RdKafka)
if (RdKafka_FOUND)
    file(GLOB HEADERS "include/eosio/kafka_plugin/*.hpp")
    file(GLOB_RECURSE CPPKAFKA_SRC "vendor/cppkafka/src/*.cpp")

    add_library(kafka_plugin
                kafka_plugin.cpp 
                ${CPPKAFKA_SRC}
                )

    target_include_directories(kafka_plugin PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/vendor/cppkafka/include")
    target_include_directories(kafka_plugin PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/vendor/cppkafka/include/cppkafka")
    target_include_directories(kafka_plugin PUBLIC  "${CMAKE_CURRENT_SOURCE_DIR}/include")


    target_link_libraries(kafka_plugin RdKafka::rdkafka)
    target_link_libraries(kafka_plugin chain_plugin appbase)
else()
    message ("Cannot Found Rdkafka, Please install it")
endif()
