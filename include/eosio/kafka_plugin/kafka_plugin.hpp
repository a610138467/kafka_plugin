#pragma once
#include <queue>
#include <fc/io/json.hpp>
#include <cppkafka/cppkafka.h>
#include <appbase/application.hpp>
#include <eosio/chain_plugin/chain_plugin.hpp>

namespace eosio {

using std::string;
using std::vector;
using std::queue;
using std::unique_ptr;
using namespace chain;
using namespace appbase;

class kafka_plugin : public appbase::plugin<kafka_plugin> {
public:
    APPBASE_PLUGIN_REQUIRES((chain_plugin))

    void set_program_options(options_description&, options_description& cfg) override;
    void plugin_initialize(const variables_map& options);
    void plugin_startup();
    void plugin_shutdown();

private:

    bool enable;

    boost::signals2::connection on_accepted_block_connection;
    boost::signals2::connection on_irreversible_block_connection;
    boost::signals2::connection on_applied_transaction_connection;
    boost::signals2::connection on_accepted_transaction_connection;

    unique_ptr<cppkafka::Producer> kafka_producer;

    template <typename KEY, typename OBJ>
    void produce_data (const string& topic, const KEY& key, const OBJ& obj, int partition = -1);

};

}

FC_REFLECT(eosio::chain::transaction_metadata,
    (id)(signed_id)(packed_trx)(signing_keys)(accepted))
