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
                    InShardLocalfullnodeList getLocalfullnodeListInShard(string pubkey); //for light node
                    InShardLocalfullnodeList getLocalfullnodeListByShardId(string shardId); //for local full node
                    string getShardInfo(string pubkey); //for local full node
                }
            }

            module localfullnode {

                struct Balance {
                    int stable;
                    int pending;
                };

                struct Hash {
                    string hash;
                    int hashMapSeed;
                };

                sequence<byte> Transaction;
                sequence<Transaction> TransactionList;
                sequence<byte> Signature;

                struct Event {
                    long selfId;
                    long selfSeq;
                    long otherId;
                    long otherSeq;
                    TransactionList transactions;
                    string timeCreated;
                    Signature sign;
                };

                sequence<long> LastSeqOneShard;
                dictionary<long, LastSeqOneShard> LastSeqOutshard;
                sequence<Event> EventList;
                // LocalFullNode: event distribution, shard data sharing;
                interface Local2local {
                    EventList gossipMyMaxSeqList4Consensus(string pubkey, string sig, LastSeqOneShard seqs);
                    void gossipHashGraph4Consensus(string pubkey, string sig, EventList events);
                    EventList gossipMyMaxSeqList4Sync(string pubkey, string sig, int otherShardId, LastSeqOneShard seqs);
                    void gossipHashGraph4Sync(string pubkey, string sig, EventList events);
                };


                struct Ltransaction {
                    string fromAddress;
                    string toAddress;
                    long amount;
                    long fee;
                    string unitId;
                    long time;
                    int isStable;
                };
                sequence<Ltransaction> LtransactionList;
                sequence<string> unitList;
                interface Light2local {
                    ["amd"]string sendMessage(string unit);
                    Balance getBalance(string address);
                    LtransactionList getTransactionHistory(string address);
                    Ltransaction getTransactionInfo(string unitId);
                    unitList getUnitInfoList(string address);
                    string getUnitInfo(string unitId);
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