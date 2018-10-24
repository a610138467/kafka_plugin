#include <queue>
#include <vector>
#include <eosio/kafka_plugin/kafka_plugin.hpp>

namespace eosio {

namespace kafka {

    Block::Block (const block_state_ptr& block_state, bool irreversible) :
        id (block_state->id.data(), block_state->id.data() + sizeof(block_id_type)),
        irreversible (irreversible),
        block_id (block_state->id),
        block_previous (block_state->header.previous),
        block_num (block_state->block_num),
        block_producer (block_state->header.producer),
        block_timestamp (block_state->header.timestamp),
        block (fc::raw::pack(block_state->block))
    {
        const char* stuff = "";
        if (irreversible)
            stuff = irreversible_stuff;
        else 
            stuff = reversible_stuff;
        
        id.push_back('|');
        id.insert(id.end(), stuff, stuff + strlen(stuff));

        time_point<system_clock, milliseconds> now = time_point_cast<milliseconds>(system_clock::now());
        create_timestamp = now.time_since_epoch().count();
    }
    
    Transaction::Transaction (const block_state_ptr& block_state, uint16_t sequence,  bool irreversible) :
        id (block_state->id.data(), block_state->id.data() + sizeof(block_id_type)),
        irreversible (irreversible),
        block_id (block_state->id),
        block_num (block_state->block_num),
        block_sequence (sequence),
        transaction (fc::raw::pack(block_state->block->transactions[sequence]))
    {
        transaction_receipt& receipt = block_state->block->transactions[sequence]; 

        transaction_cpu_usage_us = receipt.cpu_usage_us;
        transaction_net_usage_words = receipt.net_usage_words;
        if (receipt.trx.contains<transaction_id_type>()) {
            transaction_id = receipt.trx.get<transaction_id_type>();
        } else {
            auto signed_trx = receipt.trx.get<packed_transaction>().get_signed_transaction();
            transaction_id = signed_trx.id();
            transaction_expiration = signed_trx.expiration;
            transaction_delay_sec = signed_trx.delay_sec;
        }
        
        id.push_back('_');
        id.insert(id.end(), (char*)(&sequence), (char*)(&sequence) + sizeof(sequence));

        const char* stuff = "";
        if (irreversible)
            stuff = irreversible_stuff;
        else 
            stuff = reversible_stuff;
        id.push_back('|');
        id.insert(id.end(), stuff, stuff + strlen(stuff));

        time_point<system_clock, milliseconds> now = time_point_cast<milliseconds>(system_clock::now());
        create_timestamp = now.time_since_epoch().count();
    }

    Action::Action (const transaction_trace_ptr& trace, int sequence) :
        parent (0),
        transaction_id (trace->id),
        action_sequence (sequence)
    {
        const action_trace& atrace = trace->action_traces[sequence];
        account = atrace.act.account;
        name = atrace.act.name;
        data = atrace.act.data;
        receiver = atrace.receipt.receiver;
        global_sequence = atrace.receipt.global_sequence;
        recv_sequence = atrace.receipt.recv_sequence;

        id.insert (id.end(), (char*)(&global_sequence), (char*)(&global_sequence) + sizeof(global_sequence));
        time_point<system_clock, milliseconds> now = time_point_cast<milliseconds>(system_clock::now());
        create_timestamp = now.time_since_epoch().count();
    }

    Action::Action (const Action& action, const action_trace& trace, int sequence) :
        parent (action.id),
        transaction_id (action.transaction_id),
        action_sequence (sequence)
    {
        const action_trace& atrace = trace.inline_traces[sequence];
        account = atrace.act.account;
        name = atrace.act.name;
        data = atrace.act.data;
        receiver = atrace.receipt.receiver;
        global_sequence = atrace.receipt.global_sequence;
        recv_sequence = atrace.receipt.recv_sequence;

        id.insert (id.end(), (char*)(&global_sequence), (char*)(&global_sequence) + sizeof(global_sequence));
        time_point<system_clock, milliseconds> now = time_point_cast<milliseconds>(system_clock::now());
        create_timestamp = now.time_since_epoch().count();
    }
}

static appbase::abstract_plugin& _kafka_relay_plugin = app().register_plugin<kafka_plugin>();

using eosio::kafka::Block;
using eosio::kafka::Transaction;
using eosio::kafka::Action;

void kafka_plugin::set_program_options(options_description&, options_description& cfg) {
    cfg.add_options()
            ("kafka-broker-list", bpo::value<string>()->default_value("127.0.0.1:9092"), 
                "Kafka initial broker list, formatted as comma separated pairs of host or host:port, e.g., host1:port1,host2:port2")
            ("kafka-topic-prefix", bpo::value<string>()->default_value("eosio"), "Kafka topic for message `block`")
            ;
}

template <typename Class>
std::string kafka_plugin::Topic<Class>::value;

void kafka_plugin::plugin_initialize(const variables_map& options) {

    ilog("Initialize kafka plugin");

    topic_prefix = options.at("kafka-topic-prefix").as<string>();
    auto block_topic = Topic<Block>(topic_prefix);
    auto trans_topic = Topic<Transaction>(topic_prefix);
    auto ation_topic = Topic<Action>(topic_prefix);

    kafka_config = {
        {"metadata.broker.list", options.at("kafka-broker-list").as<string>()},
        {"socket.keepalive.enable", true},
        {"request.required.acks", 1},
    };

    auto& chain = app().get_plugin<chain_plugin>().chain();
    on_accepted_block_connection = chain.accepted_block.connect([=](const block_state_ptr& block_state) {
        try {
            Block block(block_state, false);
            produce(block);
            for (int i = 0; i < block_state->block->transactions.size(); i++) {
                Transaction transaction(block_state, i, false);
                produce(transaction);
            }
        } catch (const std::exception& ex) {
            elog ("std Exception in kafka_plugin when accept block : ${ex}", ("ex", ex.what()));
        } catch (...) {
            elog ("Unknown Exception in kafka_plugin when accept block");
        }
    });
    on_irreversible_block_connection = chain.irreversible_block.connect([=](const block_state_ptr& block_state) {
        try {
            Block block(block_state, true);
            produce(block);
            for (int i = 0; i < block_state->block->transactions.size(); i++) {
                Transaction transaction(block_state, i, true);
                produce(transaction);
            }
        } catch (const std::exception& ex) {
            elog ("std Exception in kafka_plugin when irreversible block : ${ex}", ("ex", ex.what()));
        } catch (...) {
            elog ("Unknown Exception in kafka_plugin when irreversible block");
        }
    });
    on_applied_transaction_connection = chain.applied_transaction.connect([=](const transaction_trace_ptr& trace) {
        try {
            queue<pair<action_trace, Action> > parent_actions;
            for (int i = 0; i < trace->action_traces.size(); i++) {
                Action action(trace, i);
                produce(action);
                if (!trace->action_traces[i].inline_traces.empty())
                    parent_actions.push(std::make_pair(trace->action_traces[i], action));
            }
            while (!parent_actions.empty()) {
                auto children = parent_actions.front();
                parent_actions.pop();
                for (int i = 0; i < children.first.inline_traces.size(); i ++) {
                    Action action(children.second, children.first, i);
                    produce(action);
                    if (!children.first.inline_traces[i].inline_traces.empty()) {
                        parent_actions.push(std::make_pair(children.first.inline_traces[i], action)); 
                    }
                }
            }
        } catch (const std::exception& ex) {
            elog ("std Exception in kafka_plugin when applied transaction : ${ex}", ("ex", ex.what()));
        } catch (...) {
            elog ("Unknown Exception in kafka_plugin when applied transaction");
        }
    });
}

void kafka_plugin::plugin_startup() {
    ilog("Starting kafka_plugin");
    kafka_producer = std::make_unique<cppkafka::Producer>(kafka_config);
    auto conf = kafka_producer->get_configuration().get_all();
    ilog ("Kafka config : ${conf}", ("conf", conf));
}

void kafka_plugin::plugin_shutdown() {
    ilog("Stopping kafka_plugin");
    try {
        on_accepted_block_connection.disconnect();
        on_irreversible_block_connection.disconnect();
        on_applied_transaction_connection.disconnect();
        kafka_producer->flush();
        kafka_producer.reset();
    } catch (const std::exception& ex) {
        elog("std Exception in kafka_plugin when shutdown: ${ex}", ("ex", ex.what()));
    }
}

}
