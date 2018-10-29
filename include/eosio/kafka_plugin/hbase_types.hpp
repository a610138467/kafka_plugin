/*
 * 这个文件里定义的结构体是用于保存到hbase中的结构体,这些结构体的定义有以下规则
 * 1. 命名空间在 eosio::kafka::hbase下
 * 2. 由于要和表对应，因此字段类型只能是 string, int, float, bool等基础类型
 * 3. 字段名以 _askey 结尾的字段会作为主键,每个结构体下只能有一个主键
 * 4. 由于要将类型解析为json,需要使用FC_REFLECT来定义反射字段
 * 5. 为了保证字段可以直接被public访问,定义成struct而不实用class
 * 6. 为了简单,尽量不实用继承
 * 7. 要包含一个string类型的kafka_id唯一标志对象,用于kafka去重
 * 8. 要包含一个int64_t类型的produce_timestamp用于指名推送到kafka的时间
 */

 #include <string>
 #include <chrono>
 #include <eosio/chain/trace.hpp>
 #include <eosio/chain/block_state.hpp>
namespace eosio{ namespace kafka{ namespace hbase{
    using std::string;
    using std::chrono::system_clock;
    using std::chrono::time_point;
    using std::chrono::milliseconds;
    using std::chrono::time_point_cast;
    using fc::time_point_sec;
    using chain::bytes;
    using chain::account_name;
    using chain::digest_type;
    using chain::block_timestamp_type;
    using chain::block_id_type;
    using chain::block_state_ptr;
    using chain::transaction_id_type;
    using chain::action_trace;
    using chain::public_key_type;
    using chain::checksum256_type;
    using chain::signature_type;
    using chain::action_name;
    using chain::transaction_trace_ptr;
    /*
     * 这个结构体记录了on_accepted_block和on_irreversible_block两个回调中block_state
     * 的全部信息
     */
    struct BlockState {
        string kafka_id; 
        int64_t produce_timestamp;
        string block_id_askey;
        //对照block_state结构体.对于复合类型使用json字符串表示
        block_id_type id;
        uint32_t block_num;
        string header;
        uint32_t dpos_proposed_irreversible_blocknum;
        uint32_t dpos_irreversible_blocknum;
        uint32_t bft_irreversible_blocknum;
        uint32_t pending_schedule_lib_num;
        digest_type pending_schedule_hash;
        string pending_schedule;
        string active_schedule;
        string blockroot_merkle;
        string producer_to_last_produced;
        string producer_to_last_implied_irb;
        public_key_type block_signing_key;
        string confirm_count;
        string confirmations;
        bool maybe_promote_pending;
        bool has_pending_producers;
        uint32_t calc_dpos_last_irreversible;
        block_id_type previous;
        digest_type sig_digest;
        public_key_type signee;
        string block;
        bool validated;
        bool in_current_chain;
        //自定义字段,预期可能用到
        uint32_t trxs_num;
        //signed_block和signed_block_header中可能用到的值
        block_timestamp_type timestamp;
        account_name producer;
        uint16_t confirmed;
        checksum256_type transaction_mroot;
        checksum256_type action_mroot;
        uint32_t schedule_version;
        signature_type producer_signature; 
        string header_extensions;
        string transactions;
        string block_extensions;
        //表示该区块是否不可逆
        bool irreversible;
        //整体信息
        string block_json;
        bytes block_bytes;

        BlockState (const block_state_ptr& block_state, bool irreversible);
    };

    /*
     * 这个结构体中记录了applied_transaction回调中transaction_trace的
     * 全部信息
     */
    struct TransactionTrace {
        string kafka_id; 
        int64_t produce_timestamp;
        string transaction_id_askey;
        //对照transaction_trace
        transaction_id_type id;
        block_id_type producer_block_id;
        uint32_t block_num;
        block_timestamp_type block_time;
        string receipt;
        fc::microseconds elapsed;
        uint64_t net_usage;
        bool scheduled;
        string action_traces;
        string failed_dtrx_trace;
        string except;
        string except_ptr;
        //自定义字段,可能用到的值
        uint32_t action_trace_num;
        //transaction_receipt_header中可能用到的值
        fc::enum_type<uint8_t,chain::transaction_receipt::status_enum> status;
        uint32_t cpu_usage_us;
        fc::unsigned_int net_usage_words;
        //整体信息
        string transaction_json;
        bytes transaction_bytes;

        TransactionTrace (const transaction_trace_ptr& transaction_trace);
    };
    
    /*
     * 这个结构体中记录了区块的包含的交易transaction_metadata的交易
     */
    struct TransactionMetadata {
        string kafka_id;
        int64_t produce_timestamp;
        string transaction_id_askey;
        block_id_type block_id;
        uint32_t block_num;
        //对照transaction_metadata
        transaction_id_type id;
        transaction_id_type signed_id;
        string trx;
        string packed_trx;
        string signing_keys;
        bool accepted;
        //signed_transaction的内容
        time_point_sec expiration;
        uint16_t ref_block_num;
        uint32_t ref_block_prefix;
        fc::unsigned_int max_net_usage_words;
        uint8_t max_cpu_usage_ms;
        fc::unsigned_int delay_sec;
        string context_free_actions;
        string actions;
        string transaction_extensions;
        account_name first_authorizor;
        string signatures;
        string context_free_data;
        //packed_transaction的内容
        uint32_t unprunable_size;
        uint32_t prunable_size;
        digest_type packed_digest;
        fc::enum_type<uint8_t,chain::packed_transaction::compression_type> compression;
        bytes packed_context_free_data;
        bytes packed_transaction_packed_trx;
        time_point_sec packed_transaction_expiration;
        //该交易目前是否不可逆
        bool irreversible;

        TransactionMetadata (const block_state_ptr& block_state, uint32_t index_in_block, bool irreversible);
    };

    /*
     * 这个结构体记录了transaction_trace中的action_trace的信息和其inline_traces的
     * action_trace信息
     */
    struct ActionTrace {
        string kafka_id; 
        int64_t produce_timestamp;
        string action_id_askey;
        string parent;
        //对照action_trace
        string receipt;
        string act;
        fc::microseconds elapsed;
        uint64_t cpu_usage;
        string console;
        uint64_t total_cpu_usage;
        transaction_id_type trx_id;
        string inline_traces;
        //自定义字段,可能用到的值
        uint32_t inline_trace_num;
        //receipt中可能用到的值
        account_name receiver;
        digest_type act_digest;
        uint64_t global_sequence;
        uint64_t recv_sequence;
        string auth_sequence;
        fc::unsigned_int code_sequence;
        fc::unsigned_int abi_sequence;
        digest_type digest;
        //action中可能用到的数据
        account_name account;
        action_name  name;
        string authorization;
        string data;
        //整体信息
        string action_json;
        bytes action_bytes;

        ActionTrace (const transaction_trace_ptr& trace, uint32_t index_in_trace);
        ActionTrace (const ActionTrace& parent, const action_trace& trace, uint32_t index_in_trace);
    };
}}} //eosio::kafka::hbase

FC_REFLECT(eosio::kafka::hbase::BlockState,
        (produce_timestamp)(block_id_askey)(id)(block_num)
        (header)(dpos_proposed_irreversible_blocknum)
        (dpos_irreversible_blocknum)(bft_irreversible_blocknum)
        (pending_schedule_lib_num)(pending_schedule_hash)
        (pending_schedule)(active_schedule)(blockroot_merkle)
        (producer_to_last_produced)(producer_to_last_implied_irb)
        (block_signing_key)(confirm_count)(confirmations)
        (maybe_promote_pending)(has_pending_producers)
        (calc_dpos_last_irreversible)(previous)(sig_digest)(signee)
        (block)(validated)(in_current_chain)(trxs_num)(timestamp)
        (producer)(confirmed)(transaction_mroot)(action_mroot)
        (schedule_version)(producer_signature)(header_extensions)
        (transactions)(block_extensions)(irreversible)(block_json)
        (block_bytes))

FC_REFLECT(eosio::kafka::hbase::TransactionTrace,
        (produce_timestamp)(transaction_id_askey)(producer_block_id)
        (block_num)(block_time)(id)
        (receipt)(elapsed)(net_usage)(scheduled)
        (action_traces)(failed_dtrx_trace)(except)
        (except_ptr)(action_trace_num)(status)
        (cpu_usage_us)(net_usage_words)
        (transaction_json)(transaction_bytes))

FC_REFLECT(eosio::kafka::hbase::TransactionMetadata,
        (produce_timestamp)(transaction_id_askey)(id)(signed_id)
        (trx)(packed_trx)(signing_keys)(accepted)(expiration)
        (ref_block_num)(ref_block_prefix)(max_net_usage_words)
        (max_cpu_usage_ms)(delay_sec)(context_free_actions)
        (actions)(transaction_extensions)(first_authorizor)
        (signatures)(context_free_data)(unprunable_size)
        (prunable_size)(packed_digest)(compression)
        (packed_context_free_data)(packed_transaction_packed_trx)
        (packed_transaction_expiration)(irreversible))

FC_REFLECT(eosio::kafka::hbase::ActionTrace,
        (produce_timestamp)(action_id_askey)(receipt)(act)
        (elapsed)(cpu_usage)(console)(total_cpu_usage)(trx_id)
        (inline_traces)(inline_trace_num)(receiver)(act_digest)
        (global_sequence)(recv_sequence)(auth_sequence)
        (code_sequence)(abi_sequence)(digest)(account)(name)
        (authorization)(data))
