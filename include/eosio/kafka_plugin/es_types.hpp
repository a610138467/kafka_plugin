/*
 * 这个文件中的结构体定义保存在es中，es提供了更快速的查询功能
 * 可以建立多个索引，同时允许快速修改。每个结构体只需要定义必须
 * 的字段，而不用定义全部信息。每个结构体可以通过一个字段和
 * hbase中的主键建立连接，从hbase中查询更详细的信息
 */

 #include <string>
 #include <vector>
 #include <chrono>
 #include <eosio/chain/block_state.hpp>
 #include <eosio/chain/trace.hpp>
namespace eosio{ namespace kafka{ namespace es{
    using std::string;
    using std::vector;
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
    using chain::packed_transaction;
    /*
     * 这个结构体提供了Block的基本信息可以通过
     * block_id block_num 快速查询block的必须信息
     */
    struct BlockInfo {
        string kafka_id;
        uint64_t produce_timestamp;
        string primary_key;

        block_id_type block_id_askey;
        block_id_type previous_askey;
        uint32_t block_num_askey;
        public_key_type block_signing_key;
        digest_type sig_digest;
        uint32_t trxs_num;
        block_timestamp_type timestamp;
        account_name producer;
        checksum256_type transaction_mroot;
        checksum256_type action_mroot;
        uint32_t schedule_version;
        signature_type producer_signature;
        string trxs;

        bool irreversible;
        string hbase_block_state_key;

        BlockInfo (const block_state_ptr& block_state, bool irreversible);
    };

    /*
     * 这个结构体提供了Transaction的基本信息
     */
    struct TransactionInfo {
        string kafka_id;
        uint64_t produce_timestamp;
        string primary_key;
        transaction_id_type transaction_id_askey;
        fc::optional<transaction_id_type> signed_id_askey;
        fc::optional<block_id_type> block_id_askey;
        uint32_t block_num_askey;
        fc::optional<fc::microseconds> elapsed;
        fc::optional<uint64_t> net_usage;
        fc::optional<bool> scheduled;
        fc::optional<uint32_t> action_trace_num;
        fc::optional<fc::enum_type<uint8_t,chain::transaction_receipt::status_enum>> status;
        fc::optional<uint32_t> cpu_usage_us;
        fc::optional<time_point_sec> expiration;
        fc::optional<uint16_t> ref_block_num;
        fc::optional<uint32_t> ref_block_prefix;
        fc::optional<fc::unsigned_int> max_net_usage_words;
        fc::optional<uint8_t> max_cpu_usage_ms;
        fc::optional<fc::unsigned_int> delay_sec;
        fc::optional<account_name> first_authorizor;
        fc::optional<string> actions;

        fc::optional<bool> irreversible;
        string hbase_transaction_trace_key;
        string hbase_transaction_metadata_key;

        TransactionInfo (const transaction_trace_ptr& trace);
        TransactionInfo (const block_state_ptr& block_state, uint32_t index_in_block, bool irreversible);
    };

    /*
     * 这个结构体记录了Action的基本信息
     */
    struct ActionInfo {
        string kafka_id;
        uint64_t produce_timestamp;
        uint64_t primary_key;
        fc::optional<uint64_t> parent;
        fc::microseconds elapsed;
        uint64_t cpu_usage;
        uint64_t total_cpu_usage;
        transaction_id_type trx_id_askey;
        block_id_type producer_block_id_askey;
        uint32_t block_num_askey;
        block_timestamp_type block_time;
        uint32_t inline_trace_num;
        account_name receiver_askey;
        digest_type act_digest;
        uint64_t global_sequence;
        uint64_t recv_sequence;
        fc::unsigned_int code_sequence;
        fc::unsigned_int abi_sequence;
        account_name account_askey;
        action_name  name_askey;
        string data;
        string inline_actions;
        
        string hbase_action_trace_key;

        ActionInfo (const transaction_trace_ptr& trace, uint32_t index_in_trace);
        ActionInfo (const action_trace& trace, uint32_t index_in_trace);
    };

    /*
     * 记录转账信息
     */
    struct TransferLog {
        string kafka_id;
        uint64_t produce_timestamp;
        uint64_t primary_key;
        block_id_type producer_block_id;
        uint32_t block_num;
        transaction_id_type transaction_id;
        uint64_t global_sequence;
        block_timestamp_type block_time;
        account_name from_askey;
        account_name to_askey;
        double amount;
        string token_symbol_askey;
        string memo;

        string hbase_action_trace_key;
        
        static fc::optional<TransferLog> build_transfer_log (const action_trace& atrace);

     };
     /*
      * 记录了合约的创建和更新的日志
      */
     struct SetcodeLog {
        string kafka_id;
        uint64_t produce_timestamp;
        uint64_t primary_key;
        account_name account_askey;
        block_id_type block_id;
        uint32_t block_num;
        transaction_id_type transaction_id;
        block_timestamp_type block_time;
        uint64_t action_global_id;
        uint8_t vmtype;
        uint8_t vmversion;
        bytes code;

        string hbase_action_trace_key;

        static fc::optional<SetcodeLog> build_setcode_log (const action_trace& atrace);
    };
    /*
     * 记录了合约接口的更新日志
     */
    struct SetabiLog {
        string kafka_id;     
        uint64_t produce_timestamp;
        uint64_t primary_key;
        account_name account_askey;
        block_id_type block_id;
        uint32_t block_num;
        transaction_id_type transaction_id;
        block_timestamp_type block_time;
        uint64_t action_global_id;
        bytes abi;

        string hbase_action_trace_key;

        static fc::optional<SetabiLog> build_setabi_log (const action_trace& atrace);
    };
    /*
     * 记录了代币的发行信息
     */
    struct TokenInfo {
        string kafka_id;
        uint64_t produce_timestamp;
        uint64_t primary_key;
        account_name issuer_askey;
        double total_amount;
        string token_symbol_askey;
        block_id_type block_id;
        uint32_t block_num;
        transaction_id_type transaction_id;
        block_timestamp_type block_time;
        uint64_t action_global_id;
        
        string hbase_action_trace_key;

        static fc::optional<TokenInfo> build_token_info (const action_trace& atrace);

    };

    struct IssueLog {
        string kafka_id;
        uint64_t produce_timestamp;
        uint64_t primary_key;
        account_name to;
        double amount;
        string token_symbol_askey;
        string memo;
        block_id_type block_id;
        uint32_t block_num;
        transaction_id_type transaction_id;
        block_timestamp_type block_time;
        uint64_t action_global_id;

        string hbase_action_trace_key;

        static fc::optional<IssueLog> build_issue_log (const action_trace& atrace);

    };
}}} //eosio::kafka::es

FC_REFLECT (eosio::kafka::es::BlockInfo,
            (produce_timestamp)(primary_key)(block_id_askey)(previous_askey)
            (block_num_askey)(block_signing_key)(sig_digest)(trxs_num)
            (timestamp)(producer)(transaction_mroot)(action_mroot)
            (schedule_version)(producer_signature)(trxs)
            (irreversible)(hbase_block_state_key))

FC_REFLECT (eosio::kafka::es::TransactionInfo,
            (produce_timestamp)(primary_key)(transaction_id_askey)
            (signed_id_askey)(block_id_askey)(elapsed)(net_usage)
            (scheduled)(action_trace_num)(status)(cpu_usage_us)
            (expiration)(ref_block_num)(ref_block_prefix)(max_net_usage_words)
            (max_cpu_usage_ms)(delay_sec)(first_authorizor)(actions)
            (irreversible)(hbase_transaction_trace_key)
            (hbase_transaction_metadata_key))

FC_REFLECT (eosio::kafka::es::ActionInfo,
            (produce_timestamp)(primary_key)(parent)(elapsed)
            (cpu_usage)(total_cpu_usage)(trx_id_askey)(inline_trace_num)
            (receiver_askey)(act_digest)(global_sequence)
            (recv_sequence)(code_sequence)(abi_sequence)(account_askey)
            (producer_block_id_askey)(block_num_askey)(block_time)(name_askey)(data)
            (inline_actions)(hbase_action_trace_key))

FC_REFLECT (eosio::kafka::es::TransferLog,
            (produce_timestamp)(primary_key)(producer_block_id)(block_num)(transaction_id)
            (global_sequence)(block_time)(from_askey)(to_askey)(amount)(memo)
            (hbase_action_trace_key))

FC_REFLECT (eosio::kafka::es::SetcodeLog,
            (produce_timestamp)(primary_key)(account_askey)(block_id)(block_num)
            (transaction_id)(block_time)(action_global_id)(vmtype)(vmversion)(code)
            (hbase_action_trace_key))

FC_REFLECT (eosio::kafka::es::SetabiLog,
            (produce_timestamp)(primary_key)(account_askey)(block_id)(block_num)
            (transaction_id)(block_time)(action_global_id)(abi)(hbase_action_trace_key))

FC_REFLECT (eosio::kafka::es::TokenInfo,
            (kafka_id)(produce_timestamp)(primary_key)(issuer_askey)
            (total_amount)(token_symbol_askey)(block_id)(block_num)
            (transaction_id)(block_time)(action_global_id)
            (hbase_action_trace_key))

FC_REFLECT (eosio::kafka::es::IssueLog,
            (kafka_id)(produce_timestamp)(primary_key)(to)(amount)
            (token_symbol_askey)(memo)(block_id)(block_num)
            (transaction_id)(block_time)(action_global_id)
            (hbase_action_trace_key))
