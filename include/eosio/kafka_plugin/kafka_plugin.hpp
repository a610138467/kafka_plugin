#pragma once

#include <chrono>
#include <fc/io/json.hpp>
#include <cppkafka/cppkafka.h>
#include <appbase/application.hpp>
#include <eosio/chain_plugin/chain_plugin.hpp>
namespace eosio {

using std::string;
using std::vector;
using std::queue;
using std::pair;
using std::chrono::system_clock;
using std::chrono::time_point;
using std::chrono::time_point_cast;
using std::chrono::milliseconds;
using fc::time_point_sec;
using chain::bytes;
using eosio::account_name;
using eosio::chain::block_timestamp_type;
using eosio::chain::block_id_type;
using eosio::chain::block_state_ptr;
using eosio::chain::transaction_id_type;
using eosio::chain::packed_transaction;
using eosio::chain::transaction_receipt;
using eosio::chain::transaction_trace_ptr;
using eosio::chain::action_trace;

using namespace appbase;

class kafka_plugin : public appbase::plugin<kafka_plugin> {
public:
    APPBASE_PLUGIN_REQUIRES((chain_plugin))

    void set_program_options(options_description&, options_description& cfg) override;
    void plugin_initialize(const variables_map& options);
    void plugin_startup();
    void plugin_shutdown();

private:

    template<typename Class>
    struct Topic {
        static std::string value;
        Topic(const string& prefix) {
            string class_name = boost::core::demangle(typeid(Class).name());
            const char replace_from[] = "::";
            const char replace_to[] = ".";
            string::size_type pos = class_name.find(replace_from);
            while (pos != string::npos) {
                class_name = class_name.replace(pos, sizeof(replace_from) - 1, replace_to);
                pos = class_name.find(replace_from);
            }
            vector<string> ignore_names = {"eosio.", "kafka."};
            for (auto name : ignore_names) {
                pos = class_name.find(name);
                if (pos != string::npos) 
                    class_name.replace(pos, name.length(), "");
            }
            value = prefix + "." + class_name;
        }
    };

    template<typename Materials> 
    void produce(Materials& materials) {
        auto payload = fc::json::to_string(materials, fc::json::legacy_generator);
        cppkafka::Buffer key (materials.kafka_id.data(), materials.kafka_id.length());
        string topic = Topic<Materials>::value;
        kafka_producer->produce(cppkafka::MessageBuilder(Topic<Materials>::value).partition(0).key(key).payload(payload));
        ilog ("push 1 message to kafka topic ${topic}", ("topic", topic));
        if (debug) {
            dlog ("message : ${message}", ("message", payload));
        }
    }

    boost::signals2::connection on_accepted_block_connection;
    boost::signals2::connection on_irreversible_block_connection;
    boost::signals2::connection on_applied_transaction_connection;
    
    cppkafka::Configuration kafka_config;
    std::unique_ptr<cppkafka::Producer> kafka_producer;
    string topic_prefix;

    uint32_t start_block_num;
    uint32_t stop_block_num;
    uint32_t current_block_num;
    bool debug;
};

}
