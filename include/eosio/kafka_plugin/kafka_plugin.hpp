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


namespace kafka {

const char* const irreversible_stuff = "irreversible";
const char* const reversible_stuff   = "reversible";

struct Block {
    bytes id;
    bool irreversible;
    int64_t create_timestamp;
    
    block_id_type block_id;
    block_id_type block_previous;
    uint32_t block_num;
    account_name block_producer;
    block_timestamp_type block_timestamp;

    bytes block;
    
    Block (const block_state_ptr& block_state, bool irreversible);
};

struct Transaction {
    bytes id;
    bool irreversible;
    int64_t create_timestamp;

    block_id_type block_id;
    int32_t block_num;
    uint16_t block_sequence;
    int32_t transaction_cpu_usage_us;
    fc::unsigned_int transaction_net_usage_words;
    transaction_id_type transaction_id;

    time_point_sec transaction_expiration;
    fc::unsigned_int transaction_delay_sec;

    bytes transaction;

    Transaction (const block_state_ptr& block_state, uint16_t sequence,  bool irreversible);
};

struct Action {
    bytes id;
    bytes parent;
    int64_t create_timestamp;

    transaction_id_type transaction_id;
    int32_t action_sequence;
    account_name account;
    account_name name;
    bytes data;
    account_name receiver;
    int64_t global_sequence;
    int64_t recv_sequence;

    Action (const transaction_trace_ptr& trace, int sequence);
    Action (const Action& action, const action_trace& trace, int sequence);
};

} //kafka



using namespace appbase;

class kafka_plugin : public appbase::plugin<kafka_plugin> {
public:
    APPBASE_PLUGIN_REQUIRES((chain_plugin))

    void set_program_options(options_description&, options_description& cfg) override;
    void plugin_initialize(const variables_map& options);
    void plugin_startup();
    void plugin_shutdown();

private:

    template<typename Materials> 
    void produce(Materials& materials) {
        auto payload = fc::json::to_string(materials, fc::json::legacy_generator);
        cppkafka::Buffer key (materials.id.data(), materials.id.size());
        string topic = topic_prefix + boost::core::demangle(typeid(Materials).name());
        kafka_producer->produce(cppkafka::MessageBuilder(topic).partition(0).key(key).payload(payload));
    }

    boost::signals2::connection on_accepted_block_connection;
    boost::signals2::connection on_irreversible_block_connection;
    boost::signals2::connection on_applied_transaction_connection;
    
    cppkafka::Configuration kafka_config;
    std::unique_ptr<cppkafka::Producer> kafka_producer;
    string topic_prefix;
};

}
FC_REFLECT(eosio::kafka::Block, (id)(irreversible)(block_id)(block_previous)(block_num)
                                (block_producer)(block_timestamp)(block))
FC_REFLECT(eosio::kafka::Transaction, (id)(irreversible)(create_timestamp)(block_id)(block_num)(block_sequence)
                                (transaction_cpu_usage_us)(transaction_net_usage_words)(transaction_id)
                                (transaction_expiration)(transaction_delay_sec))
FC_REFLECT(eosio::kafka::Action, (id)(parent)(create_timestamp)(transaction_id)(action_sequence)
                                (account)(name)(data)(receiver)(global_sequence)(recv_sequence))
