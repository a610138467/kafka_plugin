#include <queue>
#include <vector>
#include <eosio/kafka_plugin/kafka_plugin.hpp>
#include <eosio/kafka_plugin/hbase_types.hpp>
#include <eosio/kafka_plugin/es_types.hpp>

namespace eosio {

static appbase::abstract_plugin& _kafka_relay_plugin = app().register_plugin<kafka_plugin>();

using eosio::kafka::hbase::BlockState;
using eosio::kafka::hbase::TransactionTrace;
using eosio::kafka::hbase::TransactionReceipt;
using eosio::kafka::hbase::ActionTrace;
using eosio::kafka::es::BlockInfo;
using eosio::kafka::es::TransactionInfo;
using eosio::kafka::es::ActionInfo;
using eosio::kafka::es::TransferLog;
using eosio::kafka::es::SetcodeLog;
using eosio::kafka::es::SetabiLog;
using eosio::kafka::es::TokenInfo;
using eosio::kafka::es::IssueLog;

void kafka_plugin::set_program_options(options_description&, options_description& cfg) {
    cfg.add_options()
            ("kafka-broker-list", bpo::value<string>()->default_value("127.0.0.1:9092"), 
                "Kafka initial broker list, formatted as comma separated pairs of host or host:port, e.g., host1:port1,host2:port2")
            ("kafka-topic-prefix", bpo::value<string>()->default_value("eosio"), "Kafka topic for message `block`")
            ("kafka-start-block-num", bpo::value<uint32_t>()->default_value(0), "from which block begin")
            ( "kafka-stop-block-num", bpo::value<uint32_t>()->default_value(0), "to which block stop. will not stop if less than start-block-num")
            ("kafka-debug", bpo::value<bool>()->default_value(false), "print the kafka message, if true")
            ;
}

template <typename Class>
std::string kafka_plugin::Topic<Class>::value;

void kafka_plugin::plugin_initialize(const variables_map& options) {

    ilog("Initialize kafka plugin");

    topic_prefix = options.at("kafka-topic-prefix").as<string>();
    auto block_topic = Topic<BlockState>(topic_prefix);
    auto transaction_trace = Topic<TransactionTrace>(topic_prefix);
    auto transaction_receipt = Topic<TransactionReceipt>(topic_prefix);
    auto atrace = Topic<ActionTrace>(topic_prefix);
    auto block_info = Topic<BlockInfo>(topic_prefix);
    auto transaction_info = Topic<TransactionInfo>(topic_prefix);
    auto action_info = Topic<ActionInfo>(topic_prefix);
    auto transfer_log = Topic<TransferLog>(topic_prefix);
    auto setcode_log = Topic<SetcodeLog>(topic_prefix);
    auto setabi_log = Topic<SetabiLog>(topic_prefix);
    auto token_info = Topic<TokenInfo>(topic_prefix);
    auto issue_log = Topic<IssueLog>(topic_prefix);

    kafka_config = {
        {"metadata.broker.list", options.at("kafka-broker-list").as<string>()},
        {"socket.keepalive.enable", true},
        {"request.required.acks", 1},
        {"compression.codec", "gzip"},
        {"message.max.bytes", "5000000"},
    };
    this->start_block_num = options.at("kafka-start-block-num").as<uint32_t>();
    this->stop_block_num = options.at("kafka-stop-block-num").as<uint32_t>();
    this->debug = options.at("kafka-debug").as<bool>();
    this->current_block_num = 0;

    auto& chain = app().get_plugin<chain_plugin>().chain();
    on_accepted_block_connection = chain.accepted_block.connect([=](const block_state_ptr& block_state) {
        if (current_block_num < start_block_num) return;
        try {
            BlockState block(block_state, false);
            produce(block);
            BlockInfo block_info(block_state, false);
            produce(block_info);
            for (int i = 0; i < block_state->block->transactions.size(); i++) {
                TransactionReceipt transaction_receipt(block_state, i, false);
                produce(transaction_receipt);
                TransactionInfo transaction_info(block_state, i, false);
                produce(transaction_info);
            }
        } catch (const std::exception& ex) {
            elog ("std Exception in kafka_plugin when accept block : ${ex}", ("ex", ex.what()));
        } catch (...) {
            elog ("Unknown Exception in kafka_plugin when accept block");
        }
    });
    on_irreversible_block_connection = chain.irreversible_block.connect([=](const block_state_ptr& block_state) {
        current_block_num = block_state->block_num;
        if (current_block_num < start_block_num) return;
        try {
            BlockState block(block_state, true);
            produce(block);
            BlockInfo block_info(block_state, true);
            produce(block_info);
            for (int i = 0; i < block_state->block->transactions.size(); i++) {
                TransactionReceipt transaction_receipt(block_state, i, true);
                produce(transaction_receipt);
                TransactionInfo transaction_info(block_state, i, true);
                produce(transaction_info);
            }
        } catch (const std::exception& ex) {
            elog ("std Exception in kafka_plugin when irreversible block : ${ex}", ("ex", ex.what()));
        } catch (...) {
            elog ("Unknown Exception in kafka_plugin when irreversible block");
        }
        ilog ("kafka_plug, current irreversible block_id : ${current_block_num}", ("current_block_num", current_block_num));
        if (stop_block_num > start_block_num && current_block_num >= stop_block_num) {
            ilog ("kafka plugin stopped. [${from}-${to}]", ("from", start_block_num)("to", stop_block_num));
            plugin_shutdown();
            app().quit();
        }
    });
    on_applied_transaction_connection = chain.applied_transaction.connect([=](const transaction_trace_ptr& trace) {
        if (current_block_num < start_block_num) return;
        try {
            TransactionTrace transaction_trace(trace);
            produce(transaction_trace);
            TransactionInfo transaction_info(trace);
            produce(transaction_info);
            queue<pair<action_trace, ActionTrace> > parent_actions;
            for (int i = 0; i < trace->action_traces.size(); i++) {
                ActionTrace atrace(trace, i);
                produce(atrace);
                ActionInfo ainfo (trace, i);
                produce(ainfo);
                auto transfer_log = TransferLog::build_transfer_log(trace->action_traces[i]);
                if (transfer_log)
                    produce(*transfer_log);
                auto setcode_log = SetcodeLog::build_setcode_log(trace->action_traces[i]);
                if (setcode_log)
                    produce(*setcode_log);
                auto setabi_log = SetabiLog::build_setabi_log(trace->action_traces[i]);
                if (setabi_log)
                    produce(*setabi_log);
                auto token_info = TokenInfo::build_token_info(trace->action_traces[i]);
                if (token_info)
                    produce(*token_info);
                auto issue_log = IssueLog::build_issue_log(trace->action_traces[i]);
                if (issue_log)
                    produce(*issue_log);
                if (!trace->action_traces[i].inline_traces.empty())
                    parent_actions.push(std::make_pair(trace->action_traces[i], atrace));
            }
            while (!parent_actions.empty()) {
                auto children = parent_actions.front();
                parent_actions.pop();
                for (int i = 0; i < children.first.inline_traces.size(); i ++) {
                    ActionTrace atrace(children.second, children.first, i);
                    produce(atrace);
                    ActionInfo ainfo(children.first, i);
                    produce(ainfo);
                    auto transfer_log = TransferLog::build_transfer_log(children.first);
                    if (transfer_log)
                        produce(*transfer_log);
                    auto setcode_log = SetcodeLog::build_setcode_log(trace->action_traces[i]);
                    if (setcode_log)
                        produce(*setcode_log);
                    auto setabi_log = SetabiLog::build_setabi_log(trace->action_traces[i]);
                    if (setabi_log)
                        produce(*setabi_log);
                    auto token_info = TokenInfo::build_token_info(trace->action_traces[i]);
                    if (token_info)
                        produce(*token_info);
                    auto issue_log = IssueLog::build_issue_log(trace->action_traces[i]);
                    if (issue_log)
                        produce(*issue_log);
                    if (!children.first.inline_traces[i].inline_traces.empty()) {
                        parent_actions.push(std::make_pair(children.first.inline_traces[i], atrace)); 
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
    on_accepted_block_connection.disconnect();
    on_irreversible_block_connection.disconnect();
    on_applied_transaction_connection.disconnect();
    for (int i = 0; i < 5; i ++) {
        //flush有失败的情况
        try {
            kafka_producer->flush();
            ilog ("kafka flush finish");
            kafka_producer.reset();
            break;
        } catch (const std::exception& ex) {
            elog("std Exception in kafka_plugin when shutdown: ${ex}, try again(${index}/5", ("ex", ex.what())("index", i));
        }
    }
}

}
