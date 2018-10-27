#include <vector>
#include <fc/io/json.hpp>
#include <eosio/kafka_plugin/es_types.hpp>

namespace eosio{ namespace kafka{ namespace es{

using std::vector;

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
    vector<uint64_t> action_global_id_vector(trace->action_traces.size());
    for (auto atrace : trace->action_traces) {
        action_global_id_vector.push_back(atrace.receipt.global_sequence);
    }
    actions = fc::json::to_string(action_global_id_vector, fc::json::legacy_generator);
    hbase_transaction_trace_key = string(trace->id);
}

TransactionInfo::TransactionInfo (const block_state_ptr& block_state, uint32_t index_in_block, bool irreversible) {
    auto& transaction_metadata = block_state->trxs[index_in_block];
    kafka_id = string(block_state->id) + (irreversible ? 'T' : 'F') + string(transaction_metadata->id);
    time_point<system_clock, milliseconds> now = time_point_cast<milliseconds>(system_clock::now());
    produce_timestamp = now.time_since_epoch().count();
    primary_key = transaction_metadata->id;
    transaction_id_askey = transaction_metadata->id;
    signed_id_askey = transaction_metadata->signed_id;
    block_id_askey = block_state->id;
    block_num_askey = block_state->block_num;
    expiration = transaction_metadata->trx.expiration;
    ref_block_num = transaction_metadata->trx.ref_block_num;
    ref_block_prefix = transaction_metadata->trx.ref_block_prefix;
    max_net_usage_words = transaction_metadata->trx.max_net_usage_words;
    max_cpu_usage_ms = transaction_metadata->trx.max_cpu_usage_ms;
    delay_sec = transaction_metadata->trx.delay_sec;
    first_authorizor = transaction_metadata->trx.first_authorizor();
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

vector<ContractStream> ContractStream::get_contract_stream(const block_state_ptr& block_state) {
    vector<ContractStream> res;
    auto block_id = block_state->id;
    auto block_num= block_state->block_num;
    auto trxs = block_state->block->transactions;
    auto timestamp = block_state->block->timestamp;
    for (auto trx : trxs) {
        if (trx.trx.contains<packed_transaction>()) {
            auto transaction = trx.trx.get<packed_transaction>().get_transaction();
            auto transaction_id = transaction.id();
            uint32_t action_index = 0;
            for (auto action : transaction.actions) {
                string account = string(action.account);
                string name = string(action.name);
                if (account == "eosio.token" && name == "transfer") {
                    ContractStream cs;
                    char action_index_str[8];
                    sprintf(action_index_str, "%08x", action_index);
                    cs.kafka_id = string(block_id) + string(transaction_id) + action_index_str;
                    time_point<system_clock, milliseconds> now = time_point_cast<milliseconds>(system_clock::now());
                    cs.produce_timestamp = now.time_since_epoch().count();
                    cs.block_id = block_id;
                    cs.block_num= block_num;
                    cs.transaction_id = transaction_id;
                    cs.timestamp = timestamp;
                    res.push_back(cs);
                }
                action_index ++;
            }
        }
    }
    return res;
}

}}} //eosio::kafka::es
