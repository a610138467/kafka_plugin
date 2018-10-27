#include <fc/io/json.hpp>
#include <eosio/kafka_plugin/hbase_types.hpp>

namespace eosio{ namespace kafka{ namespace hbase{

BlockState::BlockState (const block_state_ptr& block_state, bool irreversible) {
    kafka_id = string(block_state->id) + (irreversible ? "T" : "F"); 
    time_point<system_clock, milliseconds> now = time_point_cast<milliseconds>(system_clock::now());
    produce_timestamp = now.time_since_epoch().count();
    block_id_askey = string(block_state->id) + (irreversible ? "T" : "F");
    id = block_state->id; 
    block_num = block_state->block_num;
    header = fc::json::to_string(block_state->header, fc::json::legacy_generator);
    dpos_proposed_irreversible_blocknum = block_state->dpos_proposed_irreversible_blocknum;
    dpos_irreversible_blocknum = block_state->dpos_irreversible_blocknum;
    bft_irreversible_blocknum = block_state->bft_irreversible_blocknum;
    pending_schedule_lib_num = block_state->pending_schedule_lib_num;
    pending_schedule_hash = block_state->pending_schedule_hash;
    pending_schedule = fc::json::to_string(block_state->pending_schedule, fc::json::legacy_generator);
    active_schedule = fc::json::to_string(block_state->active_schedule, fc::json::legacy_generator);
    blockroot_merkle = fc::json::to_string(block_state->blockroot_merkle, fc::json::legacy_generator);
    producer_to_last_produced = fc::json::to_string(block_state->producer_to_last_produced,
                                                        fc::json::legacy_generator);
    producer_to_last_implied_irb = fc::json::to_string(block_state->producer_to_last_implied_irb,
                                                        fc::json::legacy_generator);
    block_signing_key = block_state->block_signing_key;
    confirm_count = fc::json::to_string(block_state->confirm_count, fc::json::legacy_generator);
    confirmations = fc::json::to_string(block_state->confirmations, fc::json::legacy_generator);
    maybe_promote_pending = block_state->maybe_promote_pending();
    has_pending_producers = block_state->has_pending_producers();
    calc_dpos_last_irreversible = block_state->calc_dpos_last_irreversible();
    previous = block_state->header.previous;
    sig_digest = block_state->sig_digest();
    signee = block_state->signee();
    block = fc::json::to_string(block_state->block, fc::json::legacy_generator);
    validated = block_state->validated;
    in_current_chain = block_state->in_current_chain;
    trxs_num = block_state->trxs.size();
    timestamp = block_state->header.timestamp;
    producer = block_state->header.producer;
    confirmed = block_state->header.confirmed;
    transaction_mroot = block_state->header.transaction_mroot;
    action_mroot = block_state->header.action_mroot;
    schedule_version = block_state->header.schedule_version;
    producer_signature = block_state->header.producer_signature;
    header_extensions = fc::json::to_string(block_state->header.header_extensions, fc::json::legacy_generator);
    transactions = fc::json::to_string(block_state->block->transactions, fc::json::legacy_generator);
    block_extensions = fc::json::to_string(block_state->block->block_extensions, fc::json::legacy_generator);
    this->irreversible = irreversible;
    block_json = fc::json::to_string(block_state, fc::json::legacy_generator);
    block_bytes= fc::raw::pack(block_state);
}

TransactionTrace::TransactionTrace (const transaction_trace_ptr& transaction_trace) {
    kafka_id = string(transaction_trace->id);
    time_point<system_clock, milliseconds> now = time_point_cast<milliseconds>(system_clock::now());
    produce_timestamp = now.time_since_epoch().count();
    transaction_id_askey = string(transaction_trace->id);
    id = transaction_trace->id;
    fc::variant transaction_trace_variant(transaction_trace);
    if (transaction_trace_variant.is_object()) {
        auto transaction_trace_variant_object = transaction_trace_variant.get_object();
        producer_block_id = transaction_trace_variant_object["producer_block_id"].as<block_id_type>();
        block_num = transaction_trace_variant_object["block_num"].as<uint32_t>();
        block_time = transaction_trace_variant_object["block_time"].as<block_timestamp_type>();
    }
    receipt = fc::json::to_string(transaction_trace->receipt, fc::json::legacy_generator);
    elapsed = transaction_trace->elapsed;
    net_usage = transaction_trace->net_usage;
    scheduled = transaction_trace->scheduled;
    action_traces = fc::json::to_string(transaction_trace->action_traces, fc::json::legacy_generator);
    failed_dtrx_trace = fc::json::to_string(transaction_trace->failed_dtrx_trace, fc::json::legacy_generator);
    if (transaction_trace->except) {
        except = transaction_trace->except->what();
    }
    if (transaction_trace->except_ptr) {
        try {
            std::rethrow_exception(transaction_trace->except_ptr);
        } catch(const std::exception& ex) {
            except_ptr = ex.what();
        }
    }
    action_trace_num = transaction_trace->action_traces.size();
    if (transaction_trace->receipt) {
        status = transaction_trace->receipt->status;
        cpu_usage_us = transaction_trace->receipt->cpu_usage_us;
        net_usage_words = transaction_trace->receipt->net_usage_words;
    }
    transaction_json = fc::json::to_string(transaction_trace, fc::json::legacy_generator);
    transaction_bytes=fc::raw::pack(transaction_trace);
}

TransactionMetadata::TransactionMetadata (const block_state_ptr& block_state, uint32_t index_in_block, bool irreversible) {
    auto& transaction_metadata = block_state->trxs[index_in_block];
    kafka_id = string(block_state->id) + (irreversible ? 'T' : 'F') + string(transaction_metadata->id);
    time_point<system_clock, milliseconds> now = time_point_cast<milliseconds>(system_clock::now());
    produce_timestamp = now.time_since_epoch().count();
    transaction_id_askey = kafka_id;
    block_id = block_state->id;
    block_num = block_state->block_num;
    id = transaction_metadata->id;
    signed_id = transaction_metadata->signed_id;
    trx = fc::json::to_string(transaction_metadata->trx, fc::json::legacy_generator);
    packed_trx = fc::json::to_string(transaction_metadata->packed_trx, fc::json::legacy_generator);
    signing_keys = fc::json::to_string(transaction_metadata->signing_keys, fc::json::legacy_generator);
    accepted = transaction_metadata->accepted;
    expiration = transaction_metadata->trx.expiration;
    ref_block_num = transaction_metadata->trx.ref_block_num;
    ref_block_prefix = transaction_metadata->trx.ref_block_prefix;
    max_net_usage_words = transaction_metadata->trx.max_net_usage_words;
    max_cpu_usage_ms = transaction_metadata->trx.max_cpu_usage_ms;
    delay_sec = transaction_metadata->trx.delay_sec;
    context_free_actions = fc::json::to_string(transaction_metadata->trx.context_free_actions,
                                            fc::json::legacy_generator);
    actions = fc::json::to_string(transaction_metadata->trx.actions, fc::json::legacy_generator);
    transaction_extensions = fc::json::to_string(transaction_metadata->trx.transaction_extensions,
                                            fc::json::legacy_generator);
    first_authorizor = transaction_metadata->trx.first_authorizor();
    signatures = fc::json::to_string(transaction_metadata->trx.signatures, 
                                            fc::json::legacy_generator);
    context_free_data = fc::json::to_string(transaction_metadata->trx.context_free_data,
                                            fc::json::legacy_generator);
    unprunable_size = transaction_metadata->packed_trx.get_unprunable_size();
    prunable_size = transaction_metadata->packed_trx.get_prunable_size();
    packed_digest = transaction_metadata->packed_trx.packed_digest();
    compression = transaction_metadata->packed_trx.compression;
    packed_context_free_data = transaction_metadata->packed_trx.packed_context_free_data;
    packed_transaction_packed_trx = transaction_metadata->packed_trx.packed_trx;
    packed_transaction_expiration = transaction_metadata->packed_trx.expiration();
    this->irreversible = irreversible;
}

ActionTrace::ActionTrace (const transaction_trace_ptr& trace, uint32_t index_in_trace) {
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
    action_id_askey = kafka_id;
    parent = "";
    receipt = fc::json::to_string(action_trace.receipt, fc::json::legacy_generator);
    act = fc::json::to_string(action_trace.act, fc::json::legacy_generator);
    elapsed = action_trace.elapsed;
    cpu_usage = action_trace.cpu_usage;
    console = action_trace.console;
    total_cpu_usage = action_trace.total_cpu_usage;
    trx_id = action_trace.trx_id;
    inline_traces = fc::json::to_string(action_trace.inline_traces, fc::json::legacy_generator);
    inline_trace_num = action_trace.inline_traces.size();
    receiver = action_trace.receipt.receiver;
    act_digest = action_trace.receipt.act_digest;
    global_sequence = action_trace.receipt.global_sequence;
    recv_sequence = action_trace.receipt.recv_sequence;
    auth_sequence = fc::json::to_string(action_trace.receipt.auth_sequence, 
                                    fc::json::legacy_generator);
    code_sequence = action_trace.receipt.code_sequence;
    abi_sequence = action_trace.receipt.abi_sequence;
    digest = action_trace.receipt.digest();
    account = action_trace.act.account;
    name = action_trace.act.name;
    authorization = fc::json::to_string(action_trace.act.authorization,
                                    fc::json::legacy_generator);
    data = action_trace.act.data;
    action_json = fc::json::to_string(action_trace, fc::json::legacy_generator);
    action_bytes= fc::raw::pack(action_trace);
}

ActionTrace::ActionTrace (const ActionTrace& parent, const action_trace& trace, uint32_t index_in_trace) {
    auto& action_trace = trace.inline_traces[index_in_trace]; 
    kafka_id = parent.kafka_id;
    const char* global_id_ptr = (char*)(&action_trace.receipt.global_sequence);
    for (int i = 0 ; i < sizeof(global_sequence) ; i++) {
        char hex[4];
        sprintf(hex, "%02X", global_id_ptr[i]);
        kafka_id += hex;
    }
    time_point<system_clock, milliseconds> now = time_point_cast<milliseconds>(system_clock::now());
    produce_timestamp = now.time_since_epoch().count();
    action_id_askey = kafka_id;
    this->parent = parent.action_id_askey;
    receipt = fc::json::to_string(action_trace.receipt, fc::json::legacy_generator);
    act = fc::json::to_string(action_trace.act, fc::json::legacy_generator);
    elapsed = action_trace.elapsed;
    cpu_usage = action_trace.cpu_usage;
    console = action_trace.console;
    total_cpu_usage = action_trace.total_cpu_usage;
    trx_id = action_trace.trx_id;
    inline_traces = fc::json::to_string(action_trace.inline_traces, fc::json::legacy_generator);
    inline_trace_num = action_trace.inline_traces.size();
    receiver = action_trace.receipt.receiver;
    act_digest = action_trace.receipt.act_digest;
    global_sequence = action_trace.receipt.global_sequence;
    recv_sequence = action_trace.receipt.recv_sequence;
    auth_sequence = fc::json::to_string(action_trace.receipt.auth_sequence, 
                                    fc::json::legacy_generator);
    code_sequence = action_trace.receipt.code_sequence;
    abi_sequence = action_trace.receipt.abi_sequence;
    digest = action_trace.receipt.digest();
    account = action_trace.act.account;
    name = action_trace.act.name;
    authorization = fc::json::to_string(action_trace.act.authorization,
                                    fc::json::legacy_generator);
    data = action_trace.act.data;
    action_json = fc::json::to_string(action_trace, fc::json::legacy_generator);
    action_bytes= fc::raw::pack(action_trace);
}

}}} //eosio::kafka::hbase
