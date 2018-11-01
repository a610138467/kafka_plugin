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
    vector<string> trxid_vector;
    for (auto trx : block_state->block->transactions) {
        if (trx.trx.contains<packed_transaction>()) {
            trxid_vector.push_back(trx.trx.get<packed_transaction>().id());
        } else {
            trxid_vector.push_back(trx.trx.get<transaction_id_type>());
        }
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
    fc::variant_object action_variant = app().get_plugin<chain_plugin>().chain().to_variant_with_abi(
                                    action_trace, fc::seconds(1)).get_object();
    if (action_variant.find("act") != action_variant.end()) {
        auto act_object = action_variant["act"].get_object();
        if (act_object.find("data") != act_object.end()) {
            data = fc::json::to_string(act_object["data"], fc::json::legacy_generator);
        }
    }
    if (action_variant.find("producer_block_id") != action_variant.end())
        producer_block_id_askey = action_variant["producer_block_id"].as<block_id_type>();
    if (action_variant.find("block_num") != action_variant.end())
        block_num_askey = action_variant["block_num"].as<uint32_t>();
    if (action_variant.find("block_time") != action_variant.end())
        block_time = action_variant["block_time"].as<block_timestamp_type>();

    vector<uint64_t> action_global_id_vector;
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
    fc::variant_object action_variant = app().get_plugin<chain_plugin>().chain().to_variant_with_abi(
                                    action_trace, fc::seconds(1)).get_object();
    if (action_variant.find("act") != action_variant.end()) {
        auto act_object = action_variant["act"].get_object();
        if (act_object.find("data") != act_object.end()) {
            data = fc::json::to_string(act_object["data"], fc::json::legacy_generator);
        }
    }
    if (action_variant.find("producer_block_id") != action_variant.end())
        producer_block_id_askey = action_variant["producer_block_id"].as<block_id_type>();
    if (action_variant.find("block_num") != action_variant.end())
        block_num_askey = action_variant["block_num"].as<uint32_t>();
    if (action_variant.find("block_time") != action_variant.end())
        block_time = action_variant["block_time"].as<block_timestamp_type>();

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
    auto quantity = data_object["quantity"].as<asset>();
    transfer_log.amount = quantity.to_real();
    transfer_log.token_symbol_askey = quantity.symbol_name();
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

fc::optional<SetcodeLog> SetcodeLog::build_setcode_log (const action_trace& atrace) {
    fc::optional<SetcodeLog> res;

    string account = string(atrace.act.account);
    string name = string(atrace.act.name);
    if (!(account == "eosio" && name == "setcode")) return res;
    auto atrace_object = app().get_plugin<chain_plugin>().chain().to_variant_with_abi(
                            atrace, fc::seconds(1)).get_object();
    if (atrace_object.find("act") == atrace_object.end()) return res;
    auto act_object = atrace_object["act"].get_object();
    if (act_object.find("data") == act_object.end()) return res;
    auto data_object = act_object["data"].get_object();
    if (! (data_object.find("account") != data_object.end()
        && data_object.find("code") != data_object.end())) return res;
    SetcodeLog setcode_log;
    setcode_log.account_askey = data_object["account"].as<account_name>();
    setcode_log.code = data_object["code"].as<bytes>();
    if (act_object.find("vmtype") != act_object.end())
        setcode_log.vmtype = data_object["vmtype"].as<uint8_t>();
    if (act_object.find("vmversion") != act_object.end())
        setcode_log.vmversion = data_object["vmversion"].as<uint8_t>();
    setcode_log.action_global_id = atrace.receipt.global_sequence;

    if (atrace_object.find("producer_block_id") != atrace_object.end())
        setcode_log.block_id = atrace_object["producer_block_id"].as<block_id_type>();
    if (atrace_object.find("block_num") != atrace_object.end())
        setcode_log.block_num = atrace_object["block_num"].as<uint32_t>();
    if (atrace_object.find("trx_id") != atrace_object.end())
        setcode_log.transaction_id = atrace_object["trx_id"].as<transaction_id_type>();
    if (atrace_object.find("block_time") != atrace_object.end())
        setcode_log.block_time = atrace_object["block_time"].as<block_timestamp_type>(); 

    setcode_log.kafka_id = string(setcode_log.block_id) + string(setcode_log.transaction_id);
    const char* global_sequence_ptr = (const char*)(&setcode_log.action_global_id);
    for (int i = 0; i < sizeof(setcode_log.action_global_id); i ++) {
        char tmp[8];
        sprintf (tmp, "%02x", global_sequence_ptr[i]);
        setcode_log.kafka_id += tmp;
    }
    time_point<system_clock, milliseconds> now = time_point_cast<milliseconds>(system_clock::now());
    setcode_log.produce_timestamp = now.time_since_epoch().count();
    setcode_log.primary_key = setcode_log.action_global_id;
    setcode_log.hbase_action_trace_key = setcode_log.kafka_id;
    res = setcode_log;
    return res;
}

fc::optional<SetabiLog> SetabiLog::build_setabi_log (const action_trace& atrace) {
    fc::optional<SetabiLog> res;

    string account = string(atrace.act.account);
    string name = string(atrace.act.name);
    if (!(account == "eosio" && name == "setabi")) return res;
    auto atrace_object = app().get_plugin<chain_plugin>().chain().to_variant_with_abi(
                            atrace, fc::seconds(1)).get_object();
    if (atrace_object.find("act") == atrace_object.end()) return res;
    auto act_object = atrace_object["act"].get_object();
    if (act_object.find("data") == act_object.end()) return res;
    auto data_object = act_object["data"].get_object();
    if (! (data_object.find("account") != data_object.end()
        && data_object.find("abi") != data_object.end())) return res;
    SetabiLog setabi_log;
    setabi_log.account_askey = data_object["account"].as<account_name>();
    setabi_log.abi = data_object["abi"].as<bytes>();
    setabi_log.action_global_id = atrace.receipt.global_sequence;

    if (atrace_object.find("producer_block_id") != atrace_object.end())
        setabi_log.block_id = atrace_object["producer_block_id"].as<block_id_type>();
    if (atrace_object.find("block_num") != atrace_object.end())
        setabi_log.block_num = atrace_object["block_num"].as<uint32_t>();
    if (atrace_object.find("trx_id") != atrace_object.end())
        setabi_log.transaction_id = atrace_object["trx_id"].as<transaction_id_type>();
    if (atrace_object.find("block_time") != atrace_object.end())
        setabi_log.block_time = atrace_object["block_time"].as<block_timestamp_type>(); 

    setabi_log.kafka_id = string(setabi_log.block_id) + string(setabi_log.transaction_id);
    const char* global_sequence_ptr = (const char*)(&setabi_log.action_global_id);
    for (int i = 0; i < sizeof(setabi_log.action_global_id); i ++) {
        char tmp[8];
        sprintf (tmp, "%02x", global_sequence_ptr[i]);
        setabi_log.kafka_id += tmp;
    }
    time_point<system_clock, milliseconds> now = time_point_cast<milliseconds>(system_clock::now());
    setabi_log.produce_timestamp = now.time_since_epoch().count();
    setabi_log.primary_key = setabi_log.action_global_id;
    setabi_log.hbase_action_trace_key = setabi_log.kafka_id;
    res = setabi_log;
    return res;
}

fc::optional<TokenInfo> TokenInfo::build_token_info (const action_trace& atrace) {
    fc::optional<TokenInfo> res;

    string account = string(atrace.act.account);
    string name = string(atrace.act.name);
    if (!(account == "eosio.token" && name == "create")) return res;
    auto atrace_object = app().get_plugin<chain_plugin>().chain().to_variant_with_abi(
                            atrace, fc::seconds(1)).get_object();
    if (atrace_object.find("act") == atrace_object.end()) return res;
    auto act_object = atrace_object["act"].get_object();
    if (act_object.find("data") == act_object.end()) return res;
    auto data_object = act_object["data"].get_object();
    if (! (data_object.find("issuer") != data_object.end()
        && data_object.find("maximum_supply") != data_object.end())) return res;
    TokenInfo token_info;
    token_info.issuer_askey = data_object["issuer"].as<account_name>();
    auto maximum_supply = data_object["maximum_supply"].as<asset>();
    token_info.total_amount = maximum_supply.to_real();
    token_info.token_symbol_askey = maximum_supply.symbol_name();
    token_info.action_global_id = atrace.receipt.global_sequence;
    
    if (atrace_object.find("producer_block_id") != atrace_object.end())
        token_info.block_id = atrace_object["producer_block_id"].as<block_id_type>();
    if (atrace_object.find("block_num") != atrace_object.end())
        token_info.block_num = atrace_object["block_num"].as<uint32_t>();
    if (atrace_object.find("trx_id") != atrace_object.end())
        token_info.transaction_id = atrace_object["trx_id"].as<transaction_id_type>();
    if (atrace_object.find("block_time") != atrace_object.end())
        token_info.block_time = atrace_object["block_time"].as<block_timestamp_type>();
    
    token_info.kafka_id = string(token_info.block_id) + string(token_info.transaction_id);
    const char* global_sequence_ptr = (const char*)(&token_info.action_global_id);
    for (int i = 0; i < sizeof(token_info.action_global_id); i ++) {
        char tmp[8];
        sprintf (tmp, "%02x", global_sequence_ptr[i]);
        token_info.kafka_id += tmp;
    }
    time_point<system_clock, milliseconds> now = time_point_cast<milliseconds>(system_clock::now());
    token_info.produce_timestamp = now.time_since_epoch().count();
    token_info.primary_key = token_info.action_global_id;
    token_info.hbase_action_trace_key = token_info.kafka_id;
    res = token_info;
    return res;
}

fc::optional<IssueLog> IssueLog::build_issue_log (const action_trace& atrace) {
    fc::optional<IssueLog> res;

    string account = string(atrace.act.account);
    string name = string(atrace.act.name);
    if (!(account == "eosio.token" && name == "issue")) return res;
    auto atrace_object = app().get_plugin<chain_plugin>().chain().to_variant_with_abi(
                            atrace, fc::seconds(1)).get_object();
    if (atrace_object.find("act") == atrace_object.end()) return res;
    auto act_object = atrace_object["act"].get_object();
    if (act_object.find("data") == act_object.end()) return res;
    auto data_object = act_object["data"].get_object();
    if (! (data_object.find("to") != data_object.end()
        && data_object.find("quantity") != data_object.end())
        && data_object.find("memo") != data_object.end()) return res;

    IssueLog issue_log;
    issue_log.to = data_object["to"].as<account_name>();
    auto quantity = data_object["quantity"].as<asset>();
    issue_log.amount = quantity.to_real();
    issue_log.token_symbol_askey = quantity.symbol_name();
    issue_log.memo = data_object["memo"].as<string>();
    issue_log.action_global_id = atrace.receipt.global_sequence;
    
    if (atrace_object.find("producer_block_id") != atrace_object.end())
        issue_log.block_id = atrace_object["producer_block_id"].as<block_id_type>();
    if (atrace_object.find("block_num") != atrace_object.end())
        issue_log.block_num = atrace_object["block_num"].as<uint32_t>();
    if (atrace_object.find("trx_id") != atrace_object.end())
        issue_log.transaction_id = atrace_object["trx_id"].as<transaction_id_type>();
    if (atrace_object.find("block_time") != atrace_object.end())
        issue_log.block_time = atrace_object["block_time"].as<block_timestamp_type>();
    
    issue_log.kafka_id = string(issue_log.block_id) + string(issue_log.transaction_id);
    const char* global_sequence_ptr = (const char*)(&issue_log.action_global_id);
    for (int i = 0; i < sizeof(issue_log.action_global_id); i ++) {
        char tmp[8];
        sprintf (tmp, "%02x", global_sequence_ptr[i]);
        issue_log.kafka_id += tmp;
    }
    time_point<system_clock, milliseconds> now = time_point_cast<milliseconds>(system_clock::now());
    issue_log.produce_timestamp = now.time_since_epoch().count();
    issue_log.primary_key = issue_log.action_global_id;
    issue_log.hbase_action_trace_key = issue_log.kafka_id;
    res = issue_log;
    return res;
}

}}} //eosio::kafka::es
