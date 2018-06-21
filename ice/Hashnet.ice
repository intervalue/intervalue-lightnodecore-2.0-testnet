module one{
    module inve{
        module rpc {
            module bgossip {
                // LocalFullNode&FullNode: topology maintenance
                interface Gossip {
                    void initView();
                    void shuffling(string s);
                };
            }

            module seed {
                struct BaseNode {
                    string pubKey;
                    string ip;
                    int rpcPort;
                    int gossipPort;
                };
                sequence<BaseNode> BaseNodeList;
                sequence<BaseNode> InShardLocalfullnodeList;

                // register to the seed node
                interface Regist {
                    //void registerLocalFullNode4gossip(BaseNode localfullnode);
                    //BaseNodeList getNeighborList(BaseNode localfullnode);
                    InShardLocalfullnodeList getLocalfullnodeListInShard(string pubkey ); //for light node
                }
            }

            module localfullnode {

                struct Balance {
                    int stable;
                    int pending;
                };

                struct Transaction {
                    string fromAddress;
                    string toAddress;
                    int amount;
                    long fee;
                    string unitId;
                    long time;
                    int isStable;
                };
                sequence<Transaction> TransactionList;

                struct Hash {
                    string hash;
                    int hashMapSeed;
                };

                struct Event {
                    string selfId;
                    string selfSeq;
                    string otherId;
                    string otherSeq;
                    string timeCreated;
                    string signature;
                    TransactionList transactions;
                };

                sequence<long> LastSeqOneShard;
                dictionary<long, LastSeqOneShard> LastSeqOutshard;
                sequence<Event> EventList;
                // LocalFullNode: event distribution, shard data sharing;
                interface Local2local {
                    void gossipMyMaxSeqList4Consensus(string pubkey, string sig, LastSeqOneShard seqs);
                    void gossipHashGraph4Consensus(string pubkey, string sig, EventList events);
                    void gossipMyMaxSeqList4Sync(string pubkey, string sig, LastSeqOutshard seqs);
                    void gossipHashGraph4Sync(string pubkey, string sig, EventList events);
                };

                interface Light2local {
                    ["amd"]string sendMessage(string walletId, string unit);
                    Balance getBalance(string walletId);
                    TransactionList getTransactionHistory(string walletId);
                    Transaction getTransactionInfo(string unitId);
                };

                // local2full{}
            }

            module fullnode {
                struct Question {
                    string question;
                };

                struct Answer {
                    string answer;
                };

                interface ApplyFullNode {
                    Question requestQuestion();
                    void submitAnswer(string pubkey, string sig, Answer answer);
                };

                // FullNode: Sharding
                //interface Sharding {
                //    void syncLocalFullNodeCandidator();
                //};
            }

            module demo {

                ["java:serializable:one.inve.rpc.localfullnode.Event"] sequence<byte> EventObj;
                interface Printer {
                    void printString(string s);
                    int  add(int a, int b);
                    int  sub(int inp1, int inp2, out bool outp1, out long outp2);
                    void printEvent(EventObj obj);
                };

                sequence<float> Row;
                sequence<Row> Grid;

                exception RangeError {}

                interface Model
                {
                    ["amd"] Grid interpolate(Grid data, float factor)
                            throws RangeError;
                }

            }
        };
    };
};