#include <eosio/kafka_plugin/kafka_plugin.hpp>
#include <boost/algorithm/string/join.hpp>
#include <fc/io/json.hpp>

namespace eosio {

using namespace std;
using bpo::variables_map;
using bpo::options_description;
namespace bpo = boost::program_options;

static auto& _kafka__plugin = app().register_plugin<kafka_plugin>();

void kafka_plugin::set_program_options(options_description&, options_description& cfg) {
    cfg.add_options()
        ("kafka-plugin-enable", bpo::bool_switch()->default_value(false), "weather enable the kafka_plugin")
        ("kafka-plugin-broker", bpo::value<vector<string> >()->composing(),   "kafka broker addr, can have more than one")
        ("kafka-plugin-enable-accepted-block-connection", bpo::bool_switch()->default_value(false), "enable the data stream that from accepted_block callback")
        ("kafka-plugin-accepted-block-topic-name", bpo::value<string>()->default_value("eosio.accepted.block"), "the topic name of accepted_block")
        ("kafka-plugin-enable-irreversible-block-connection", bpo::bool_switch()->default_value(false), "enable the data stream that from irreversible_block callback")
        ("kafka-plugin-irreversible-block-topic-name", bpo::value<string>()->default_value("eosio.irreversible.block"), "the topic name ofr irreversible_block")
        ("kafka-plugin-enable-applied-transaction-connection", bpo::bool_switch()->default_value(false), "enable the data stream that from applied_transaction callback")
        ("kafka-plugin-applied-transaction-topic-name", bpo::value<string>()->default_value("eosio.applied.transaction"), "the topic name of applied_transaction")
        ("kafka-plugin-enable-accepted-transaction-connection", bpo::bool_switch()->default_value(false), "enable the data stream that from accepted_transaction callback")
        ("kafka-plugin-accepted-transaction-topic-name", bpo::value<string>()->default_value("eosio.accepted.transaction"), "the topic name of accepted_transaction")
        ;
}

void kafka_plugin::plugin_initialize(const variables_map& options) {
    //get parameters from options
    enable = options.at("kafka-plugin-enable").as<bool>();
    vector<string> brokers = options.count("kafka-plugin-broker") > 0 ? options.at("kafka-plugin-broker").as<vector<string> >() : vector<string>();
    bool enable_accepted_block_connection = options.at("kafka-plugin-enable-accepted-block-connection").as<bool>();
    string accepted_block_topic_name = options.at("kafka-plugin-accepted-block-topic-name").as<string>();
    bool enable_irreversible_block_connection = options.at("kafka-plugin-enable-irreversible-block-connection").as<bool>();
    string irreversible_block_topic_name = options.at("kafka-plugin-irreversible-block-topic-name").as<string>();
    bool enable_applied_transaction_connection = options.at("kafka-plugin-enable-applied-transaction-connection").as<bool>();
    string applied_transaction_topic_name = options.at("kafka-plugin-applied-transaction-topic-name").as<string>();
    bool enable_accepted_transaction_connection = options.at("kafka-plugin-enable-accepted-transaction-connection").as<bool>();
    string accepted_transaction_topic_name = options.at("kafka-plugin-accepted-transaction-topic-name").as<string>();
    //check parameters, determin weather enable kafka_plugin
    if (!enable) return;
    if (brokers.size() == 0) {
        wlog ("enable kafka_plugin, but no broker addr found, kafka_plugin will be disabled");
        return;
    }
    if (enable_accepted_block_connection && accepted_block_topic_name == "") {
        wlog ("enable data stream from accepted_block, but no topic name defined. data stream from accepted_block will be disabled");
        enable_accepted_block_connection = false;
    }
    if (enable_irreversible_block_connection && irreversible_block_topic_name == "") {
        wlog ("enable data stream from irreversible_block, but no topic name defined. data stream from irreversible_block will be disabled");
        enable_irreversible_block_connection = false;
    }
    if (enable_applied_transaction_connection && applied_transaction_topic_name == "") {
        wlog ("enable data stream from applied_transaction, but no topic name defined. data stream from applied_transaction will be disabled");
        enable_applied_transaction_connection = false;
    }
    if (enable_accepted_transaction_connection && accepted_transaction_topic_name == "") {
        wlog ("enable data stream from accepted_transaction, but no topic name defined. data stream from accepted_transaction will be disabled");
        enable_accepted_transaction_connection = false;
    }
    if (!(enable_accepted_block_connection || enable_irreversible_block_connection || enable_applied_transaction_connection || enable_accepted_transaction_connection)) {
        wlog ("found kafka brokers,but no data stream avaliable, kafka_plugin will be disabled");
        return;
    }
    //create kafka producer
    cppkafka::Configuration kafka_config = {
        {"metadata.broker.list", boost::join(options.at("kafka-plugin-broker").as<vector<string> >(), ",")},
        {"socket.keepalive.enable", true},
        {"request.required.acks", 1},
        {"compression.codec", "gzip"},
    };
    kafka_producer = std::make_unique<cppkafka::Producer>(kafka_config);
    auto conf = kafka_producer->get_configuration().get_all();
    dlog ("Kafka config : ${conf}", ("conf", conf));
    //registe data stream
    auto& chain = app().get_plugin<chain_plugin>().chain();
    if (enable_accepted_block_connection) {
        on_accepted_block_connection = chain.accepted_block.connect([=](const block_state_ptr& block_state) {
            produce_data (accepted_block_topic_name, block_state->id, block_state);
        });
    }
    if (enable_irreversible_block_connection) {
        on_irreversible_block_connection = chain.irreversible_block.connect([=](const block_state_ptr& block_state) {
            produce_data (irreversible_block_topic_name, block_state->id, block_state);
        });
    }
    if (enable_applied_transaction_connection) {
        on_applied_transaction_connection = chain.applied_transaction.connect([=](const transaction_trace_ptr& transaction_trace) {
            produce_data (applied_transaction_topic_name, transaction_trace->id, transaction_trace);
        });
    }
    if (enable_accepted_transaction_connection) {
        on_accepted_transaction_connection = chain.accepted_transaction.connect([=](const transaction_metadata_ptr& transaction_metadata) {
            produce_data (accepted_transaction_topic_name, transaction_metadata->id, transaction_metadata);
        });
    }
}

void kafka_plugin::plugin_startup() {
    if (!enable) return;
    ilog ("kafka_plugin startup");
}

void kafka_plugin::plugin_shutdown() {
    if (!enable) return;
    //sometimes the flush will be failed, try more time will be usefull
    for (int i = 0; i < 5; i++) {
        try {
            kafka_producer->flush();
            ilog ("kafka producer flush finish");
            kafka_producer.reset();
            break;
        } catch (const std::exception& ex) {
            elog ("std Exception when flush kafka producer : ${ex} try again(${i}/5)", ("ex", ex.what())("i", i));
        }
    }
    ilog ("kafka_plugin shutdown");
}

template <typename KEY, typename OBJ>
void kafka_plugin::produce_data (const string& topic, const KEY& key, const OBJ& obj, int partition) {
    try{
        string key_str = string(key);
        cppkafka::Buffer key_kafka(key_str.data(), key_str.length());
        auto tvariant = app().get_plugin<chain_plugin>()
            .chain().to_variant_with_abi(obj, fc::seconds(10));
        auto payload = fc::json::to_string(tvariant, fc::json::legacy_generator);
        kafka_producer->produce(cppkafka::MessageBuilder(topic).key(key_kafka).payload(payload).partition(partition));
    } catch (const std::exception& ex) {
        elog ("std Exception in data_plugin when accept block : ${ex}", ("ex", ex.what()));
    } catch ( fc::exception& ex) {
        wlog( "fc Exception in data_plugin when accept block : ${ex}", ("ex", ex.to_detail_string()) );
    } catch (...) {
        elog ("Unknown Exception in data_plugin when accept block");
    }
}

}
