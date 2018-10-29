#include <vector>
#include <fc/io/json.hpp>
#include <eosio/kafka_plugin/es_types.hpp>
#include <appbase/application.hpp>
#include <eosio/chain_plugin/chain_plugin.hpp>

namespace eosio{ namespace kafka{ namespace es{

using std::vector;
using namespace appbase;

BlockInfo::BlockInfo (const block_state_ptr& block_state, bool irreversible) {
    kafka_id = string(block_state->id) + (irreversible ? "T" : "F");
    time_point<system_clock, milliseconds> now = time_point_cast<milliseconds>(system_clock::now());
    produce_timestamp = now.time_since_epoch().count();
    primary_key = string(block_state->id);
    block_id_askey = block_state->id;
    previous_askey = block_state->header.previous;
    block_num_askey = block_state->block_num;
    block_signing_key = block_state->block_signing_key;
    sig_digest = block_state->sig_digest();
    trxs_num = block_state->trxs.size();
    timestamp = block_state->header.timestamp;
    producer = block_state->header.producer;
    transaction_mroot = block_state->header.transaction_mroot;
    action_mroot = block_state->header.action_mroot;
    schedule_version = block_state->header.schedule_version;
    producer_signature = block_state->header.producer_signature;
    vector<string> trxid_vector(trxs_num);
    for (auto trx : block_state->trxs) {
        trxid_vector.push_back(trx->id);
    }
    trxs = fc::json::to_string(trxid_vector, fc::json::legacy_generator);
    this->irreversible = irreversible;
    hbase_block_state_key = string(block_state->id) + (irreversible ? "T" : "F");
}

TransactionInfo::TransactionInfo (const transaction_trace_ptr& trace) {
    kafka_id = string(trace->id);
    time_point<system_clock, milliseconds> now = time_point_cast<milliseconds>(system_clock::now());
    produce_timestamp = now.time_since_epoch().count();
    primary_key = string(trace->id); 
    transaction_id_askey = trace->id;
    elapsed = trace->elapsed;
    net_usage = trace->net_usage;
    scheduled = trace->scheduled;
    if (trace->receipt) {
        status = trace->receipt->status;
        cpu_usage_us = trace->receipt->cpu_usage_us;
    }
    action_trace_num = trace->action_traces.size();
    vector<uint64_t> action_global_id_vector;
    for (auto atrace : trace->action_traces) {
        action_global_id_vector.push_back(atrace.receipt.global_sequence);
    }
    actions = fc::json::to_string(action_global_id_vector, fc::json::legacy_generator);
    hbase_transaction_trace_key = string(trace->id);
}

TransactionInfo::TransactionInfo (const block_state_ptr& block_state, uint32_t index_in_block, bool irreversible) {
    auto& transaction_receipt = block_state->block->transactions[index_in_block];
    transaction_id_type transaction_id;
    if (transaction_receipt.trx.contains<packed_transaction>()) {
        transaction_id = transaction_receipt.trx.get<packed_transaction>().id();
    } else {
        transaction_id = transaction_receipt.trx.get<transaction_id_type>();
    }
    kafka_id = string(block_state->id) + (irreversible ? 'T' : 'F') + string(transaction_id);
    time_point<system_clock, milliseconds> now = time_point_cast<milliseconds>(system_clock::now());
    produce_timestamp = now.time_since_epoch().count();
    primary_key = transaction_id;
    transaction_id_askey = transaction_id;
    block_id_askey = block_state->id;
    block_num_askey = block_state->block_num;
    if (transaction_receipt.trx.contains<packed_transaction>()) {
        auto packed_trx = transaction_receipt.trx.get<packed_transaction>();
        auto signed_trx = packed_trx.get_signed_transaction();
        expiration = signed_trx.expiration;
        ref_block_num = signed_trx.ref_block_num;
        ref_block_prefix = signed_trx.ref_block_prefix;
        max_net_usage_words = signed_trx.max_net_usage_words;
        max_cpu_usage_ms = signed_trx.max_cpu_usage_ms;
        delay_sec = signed_trx.delay_sec;
        first_authorizor = signed_trx.first_authorizor();
    }
    if (irreversible) {
        this->irreversible = irreversible;
    }
    hbase_transaction_metadata_key = kafka_id;
}

ActionInfo::ActionInfo (const transaction_trace_ptr& trace, uint32_t index_in_trace) {
    auto& action_trace = trace->action_traces[index_in_trace]; 
    kafka_id = string(action_trace.trx_id);
    const char* global_id_ptr = (char*)(&action_trace.receipt.global_sequence);
    for (int i = 0; i < sizeof(global_sequence); i++) {
        char hex[4];
        sprintf(hex, "%02x", global_id_ptr[i]);
        kafka_id += hex;
    }
    time_point<system_clock, milliseconds> now = time_point_cast<milliseconds>(system_clock::now());
    produce_timestamp = now.time_since_epoch().count();
    primary_key = action_trace.receipt.global_sequence;
    elapsed = action_trace.elapsed;
    cpu_usage = action_trace.cpu_usage;
    total_cpu_usage = action_trace.total_cpu_usage;
    trx_id_askey = action_trace.trx_id;
    inline_trace_num = action_trace.inline_traces.size();
    receiver_askey = action_trace.receipt.receiver;
    act_digest = action_trace.receipt.act_digest;
    global_sequence = action_trace.receipt.global_sequence;
    recv_sequence = action_trace.receipt.recv_sequence;
    code_sequence = action_trace.receipt.code_sequence;
    abi_sequence = action_trace.receipt.abi_sequence;
    account_askey = action_trace.act.account;
    name_askey = action_trace.act.name;
    data = action_trace.act.data;
    vector<uint64_t> action_global_id_vector(action_trace.inline_traces.size());
    for (auto inline_trace : action_trace.inline_traces) {
        action_global_id_vector.push_back(inline_trace.receipt.global_sequence);
    }
    inline_actions = fc::json::to_string(action_global_id_vector, fc::json::legacy_generator);
    hbase_action_trace_key = kafka_id;
}

ActionInfo::ActionInfo (const action_trace& trace, uint32_t index_in_trace) {
    auto& action_trace = trace.inline_traces[index_in_trace];  
    kafka_id = string(trace.trx_id);
    const char* global_id_ptr = (char*)(&action_trace.receipt.global_sequence);
    for (int i = 0; i < sizeof(global_sequence); i++) {
    char hex[4];
    sprintf(hex, "%02x", global_id_ptr[i]);
        kafka_id += hex;
    }
    time_point<system_clock, milliseconds> now = time_point_cast<milliseconds>(system_clock::now());
    produce_timestamp = now.time_since_epoch().count();
    primary_key = action_trace.receipt.global_sequence;
    parent = trace.receipt.global_sequence;
    elapsed = action_trace.elapsed;
    cpu_usage = action_trace.cpu_usage;
    total_cpu_usage = action_trace.total_cpu_usage;
    trx_id_askey = action_trace.trx_id;
    inline_trace_num = action_trace.inline_traces.size();
    receiver_askey = action_trace.receipt.receiver;
    act_digest = action_trace.receipt.act_digest;
    global_sequence = action_trace.receipt.global_sequence;
    recv_sequence = action_trace.receipt.recv_sequence;
    code_sequence = action_trace.receipt.code_sequence;
    abi_sequence = action_trace.receipt.abi_sequence;
    account_askey = action_trace.act.account;
    name_askey = action_trace.act.name;
    data = action_trace.act.data;
    vector<uint64_t> action_global_id_vector(action_trace.inline_traces.size());
    for (auto inline_trace : action_trace.inline_traces) {
        action_global_id_vector.push_back(inline_trace.receipt.global_sequence);
    }
    inline_actions = fc::json::to_string(action_global_id_vector, fc::json::legacy_generator);
    hbase_action_trace_key = kafka_id;
}

fc::optional<TransferLog> TransferLog::build_transfer_log (const action_trace& atrace) {
    fc::optional<TransferLog> res;

    string account = string(atrace.act.account);
    string name = string(atrace.act.name);
    if (!(account == "eosio.token" && name == "transfer")) return res;
    auto atrace_object = app().get_plugin<chain_plugin>().chain().to_variant_with_abi(
                            atrace, fc::seconds(1)).get_object();
    if (atrace_object.find("act") == atrace_object.end()) return res;
    auto act_object = atrace_object["act"].get_object();
    if (act_object.find("data") == act_object.end()) return res;
    auto data_object = act_object["data"].get_object();
    if (! (data_object.find("from") != data_object.end()
        && data_object.find( "to" ) != data_object.end()
        && data_object.find("quantity") != data_object.end())) return res;
    
    TransferLog transfer_log;
    transfer_log.from_askey = data_object["from"].as<string>();
    transfer_log.to_askey = data_object["to"].as<string>();
    transfer_log.amount = data_object["quantity"].as<asset>().to_real();
    if (data_object.find("memo") != data_object.end())
        transfer_log.memo = data_object["memo"].as<string>();

    if (atrace_object.find("producer_block_id") != atrace_object.end())
        transfer_log.producer_block_id = atrace_object["producer_block_id"].as<block_id_type>();
    if (atrace_object.find("block_num") != atrace_object.end())
        transfer_log.block_num= atrace_object["block_num"].as<uint32_t>();
    if (atrace_object.find("trx_id") != atrace_object.end())
        transfer_log.transaction_id = atrace_object["trx_id"].as<transaction_id_type>();
    if (atrace_object.find("block_time") != atrace_object.end())
        transfer_log.block_time = atrace_object["block_time"].as<block_timestamp_type>();
    transfer_log.global_sequence = atrace.receipt.global_sequence;

    transfer_log.kafka_id = string(transfer_log.producer_block_id) + string(transfer_log.transaction_id);
    const char* global_sequence_ptr = (const char*)(&transfer_log.global_sequence);
    for (int i = 0; i < sizeof(transfer_log.global_sequence); i ++) {
        char tmp[8];
        sprintf (tmp, "%02x", global_sequence_ptr[i]);
        transfer_log.kafka_id += tmp;
    }
    time_point<system_clock, milliseconds> now = time_point_cast<milliseconds>(system_clock::now());
    transfer_log.produce_timestamp = now.time_since_epoch().count();
    transfer_log.primary_key = transfer_log.global_sequence;
    transfer_log.hbase_action_trace_key = transfer_log.kafka_id;
    res = transfer_log;
    return res;
}

}}} //eosio::kafka::es
