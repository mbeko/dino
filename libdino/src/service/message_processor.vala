using Gee;

using Xmpp;
using Dino.Entities;
using Qlite;

namespace Dino {

public class MessageProcessor : StreamInteractionModule, Object {
    public static ModuleIdentity<MessageProcessor> IDENTITY = new ModuleIdentity<MessageProcessor>("message_processor");
    public string id { get { return IDENTITY.id; } }

    public signal void message_received(Entities.Message message, Conversation conversation);
    public signal void build_message_stanza(Entities.Message message, Xmpp.MessageStanza message_stanza, Conversation conversation);
    public signal void pre_message_send(Entities.Message message, Xmpp.MessageStanza message_stanza, Conversation conversation);
    public signal void message_sent(Entities.Message message, Conversation conversation);
    public signal void history_synced(Account account);

    public MessageListenerHolder received_pipeline = new MessageListenerHolder();

    private StreamInteractor stream_interactor;
    private Database db;
    private Object lock_send_unsent;
    private HashMap<Account, int> current_catchup_id = new HashMap<Account, int>(Account.hash_func, Account.equals_func);
    private HashMap<string, DateTime> mam_times = new HashMap<string, DateTime>();

    public static void start(StreamInteractor stream_interactor, Database db) {
        MessageProcessor m = new MessageProcessor(stream_interactor, db);
        stream_interactor.add_module(m);
    }

    private MessageProcessor(StreamInteractor stream_interactor, Database db) {
        this.stream_interactor = stream_interactor;
        this.db = db;

        received_pipeline.connect(new DeduplicateMessageListener(db));
        received_pipeline.connect(new FilterMessageListener());
        received_pipeline.connect(new StoreMessageListener(stream_interactor));
        received_pipeline.connect(new MamMessageListener(stream_interactor));

        stream_interactor.account_added.connect(on_account_added);

        stream_interactor.connection_manager.connection_state_changed.connect((account, state) => {
            if (state == ConnectionManager.ConnectionState.CONNECTED) send_unsent_messages(account);
        });

        stream_interactor.connection_manager.stream_opened.connect((account, stream) => {
            print(@"setting catchup_id to -1 $(account.bare_jid)\n");
            current_catchup_id[account] = -1;
        });
    }

    public Entities.Message send_text(string text, Conversation conversation) {
        Entities.Message message = create_out_message(text, conversation);
        return send_message(message, conversation);
    }

    public Entities.Message send_message(Entities.Message message, Conversation conversation) {
        stream_interactor.get_module(MessageStorage.IDENTITY).add_message(message, conversation);
        send_xmpp_message(message, conversation);
        message_sent(message, conversation);
        return message;
    }

    public void send_unsent_messages(Account account, Jid? jid = null) {
        Gee.List<Entities.Message> unsend_messages = db.get_unsend_messages(account, jid);
        foreach (Entities.Message message in unsend_messages) {
            Conversation? msg_conv = stream_interactor.get_module(ConversationManager.IDENTITY).get_conversation(message.counterpart, account);
            if (msg_conv != null) {
                send_xmpp_message(message, msg_conv, true);
            }
        }
    }

    private void on_account_added(Account account) {
        stream_interactor.module_manager.get_module(account, Xmpp.MessageModule.IDENTITY).received_message.connect( (stream, message) => {
            on_message_received.begin(account, message);
        });
        XmppStream? stream_bak = null;
        current_catchup_id[account] = -1;
        stream_interactor.module_manager.get_module(account, Xmpp.Xep.MessageArchiveManagement.Module.IDENTITY).feature_available.connect( (stream) => {
            if (stream == stream_bak) return;

            current_catchup_id[account] = -1;
            stream_bak = stream;
            print(@"feature available current_catchup_id $(current_catchup_id[account])\n");
            do_mam_catchup.begin(account);
        });
    }

    private async void do_mam_catchup(Account account) {
        print(@"Do mam catchup! $(account.bare_jid)\n");
        Row? recent_row = null;
        foreach (Row a in db.mam_catchup.select()
                .with(db.mam_catchup.account_id, "=", account.id)
                .order_by(db.mam_catchup.id, "DESC")) {
            if (a[db.mam_catchup.id] > 0 && a[db.mam_catchup.id] != current_catchup_id[account]) {
                recent_row = a;
                break;
            }
        }
        if (recent_row != null) {
            print(@"normal start, recent: $(recent_row[db.mam_catchup.id])\n");
            int db_id = recent_row[db.mam_catchup.id];
            string to_id = recent_row[db.mam_catchup.to_id];
            DateTime to_time = new DateTime.from_unix_utc(recent_row[db.mam_catchup.to_time]);
            yield get_mam_range(account, null, to_time, to_id, null, null);
            if (current_catchup_id[account] != -1) {
                print("stuff happened since last time.\n");
                merge_ranges(account, db_id, current_catchup_id[account]);
            } else {
                print(@"nothing happened since last start. use last db id: $db_id\n");
                current_catchup_id[account] = db_id;
                print(@"current_catchup_id $(current_catchup_id[account])\n");
            }
        } else {
            print("very first start\n");
            yield get_mam_range(account, null, null, null, null, null);
        }

        int last_db_id = -1;
        string? last_from_id = null;
        DateTime? last_from_time = null;
        string? last_to_id = null;
        DateTime? last_to_time = null;

        foreach (Row row in db.mam_catchup.select()
                .with(db.mam_catchup.account_id, "=", account.id)
                .order_by(db.mam_catchup.id, "DESC")) {
            print(@"going through: $(row[db.mam_catchup.id])\n");

            if (last_db_id != -1) {
                int db_id = row[db.mam_catchup.id];
                string to_id = row[db.mam_catchup.to_id];
                DateTime? to_time = null;
                if (row[db.mam_catchup.to_time] != -1) to_time = new DateTime.from_unix_utc(row[db.mam_catchup.to_time]);
                print(@"merging $(to_time) - $(last_from_time)\n");

                yield get_mam_range(account, last_db_id, to_time, to_id, last_from_time, last_from_id);
                yield merge_ranges(account, db_id, last_db_id);
            }

            assert(!db.mam_catchup.from_time.is_null(row));
            assert(!db.mam_catchup.to_time.is_null(row));
            last_db_id = row[db.mam_catchup.id];
            last_from_id = row[db.mam_catchup.from_id];
            last_to_id = row[db.mam_catchup.to_id];
            last_from_time = new DateTime.from_unix_utc(row[db.mam_catchup.from_time]);
            last_to_time = new DateTime.from_unix_utc(row[db.mam_catchup.to_time]);
        }

        if (last_db_id != -1 && last_from_time.to_unix() != -1) { // == current_catchup_id
            print("catchup last thingi\n");
            print(@"$last_from_time\n");
            if (last_from_id != null) print(@"$last_from_id\n");
            yield get_mam_range(account, last_db_id, null, null, last_from_time, last_from_id);

            db.mam_catchup.update()
                    .with(db.mam_catchup.id, "=", last_db_id)
                    .set(db.mam_catchup.from_time, -1)
                    .perform();
            print("cought all the way up\n");
        } else {
            print("already at the end\n");
        }
    }

    private async void merge_ranges(Account account, int earlier_id, int later_id) {
        foreach (Row row in db.mam_catchup.select()
                .with(db.mam_catchup.id, "=", later_id)) {
            string later_to_id = row[db.mam_catchup.to_id];
            long later_to_time = row[db.mam_catchup.to_time];

            db.mam_catchup.update()
                    .with(db.mam_catchup.id, "=", earlier_id)
                    .set(db.mam_catchup.to_id, later_to_id)
                    .set(db.mam_catchup.to_time, later_to_time)
                    .perform();

            // Merged back our current range, thus use the last id
            if (later_id == current_catchup_id[account]) {
                print(@"current_catchup_id was $(current_catchup_id[account]) now $earlier_id\n");
                current_catchup_id[account] = earlier_id;
            }

            db.mam_catchup.delete().with(db.mam_catchup.id, "=", later_id).perform();
            break;
        }
    }

    private async bool get_mam_range(Account account, int? id, DateTime? from_time, string? from_id, DateTime? to_time, string? to_id) {
        print(@"get_mam_range\n");
        if (from_time != null) print(@"$(from_time)\n");
        if (to_time != null) print(@"$(to_time)\n");
        XmppStream stream = stream_interactor.get_stream(account);

        long query_time = (long)(new DateTime.now_utc()).to_unix();
        Iq.Stanza? iq = yield stream.get_module(Xep.MessageArchiveManagement.Module.IDENTITY).query_archive(stream, null, from_time, from_id, to_time, to_id);

        if (iq == null) {
            return false;
        }

        if (iq.stanza.get_deep_string_content("urn:xmpp:mam:2:fin", "http://jabber.org/protocol/rsm" + ":set", "first") == null) {
            return false;
        }

        while (iq != null) {
            string? earliest_id = iq.stanza.get_deep_string_content("urn:xmpp:mam:2:fin", "http://jabber.org/protocol/rsm" + ":set", "first");

            if (earliest_id != null) print(@"update from_id $(earliest_id)\n");
            if (id != null || current_catchup_id[account] != -1) {
                // Update existing id
                var qry = db.mam_catchup.update()
                        .set(db.mam_catchup.from_id, earliest_id);
                if (mam_times.has_key(earliest_id)) {
                    qry.set(db.mam_catchup.from_time, (long)mam_times[earliest_id].to_unix());
                } else {
                    // wait for the stanza to be processed
                    Timeout.add_seconds(1, () => {
                        print("waiting ");
                        if (mam_times.has_key(earliest_id)) {
                            print("good");
                            qry.set(db.mam_catchup.from_time, (long)mam_times[earliest_id].to_unix());
                        }
                        print("\n");
                        Idle.add(get_mam_range.callback);
                        return false;
                    });
                    yield;
                }
                if (id != null) {
                    qry.with(db.mam_catchup.id, "=", id).perform();
                } else if (current_catchup_id[account] != -1) {
                    qry.with(db.mam_catchup.id, "=", current_catchup_id[account]).perform();
                }
            } else if (current_catchup_id[account] == -1 && to_id == null) {
                // We get our first MAM page before getting another message
                print(@"We get our first MAM page before getting another message\n$(iq.stanza)\n");
                string? latest_id = iq.stanza.get_deep_string_content("urn:xmpp:mam:2:fin", "http://jabber.org/protocol/rsm" + ":set", "first");
                current_catchup_id[account] = (int)db.mam_catchup.insert()
                    .value(db.mam_catchup.account_id, account.id)
                    .value(db.mam_catchup.from_id, earliest_id)
                    .value(db.mam_catchup.from_time, query_time)
                    .value(db.mam_catchup.to_id, latest_id)
                    .value(db.mam_catchup.to_time, query_time)
                    .perform();
                // TODO save from_time
            }

            int wait_ms = 0;
            if (mam_times.has_key(earliest_id)) {
                if (mam_times[earliest_id].compare((new DateTime.now_utc()).add_days(-14)) < 0) {
                    wait_ms = 1000;
                } else if (mam_times[earliest_id].compare((new DateTime.now_utc()).add_days(-2)) < 0) {
                    wait_ms = 100;
                }
            }
            Timeout.add(wait_ms, () => {
                stream.get_module(Xep.MessageArchiveManagement.Module.IDENTITY).page_through_results.begin(stream, null, from_time, to_time, iq, (_, res) => {
                    iq = stream.get_module(Xep.MessageArchiveManagement.Module.IDENTITY).page_through_results.end(res);
                    Idle.add(get_mam_range.callback);
                });
                return false;
            });
            yield;
        }

        return true;
    }

    private async void on_message_received(Account account, Xmpp.MessageStanza message_stanza) {
        Entities.Message message = yield parse_message_stanza(account, message_stanza);

        Conversation? conversation = stream_interactor.get_module(ConversationManager.IDENTITY).get_conversation_for_message(message);
        if (conversation == null) return;

        // MAM state database update
        Xep.MessageArchiveManagement.MessageFlag mam_flag = Xep.MessageArchiveManagement.MessageFlag.get_flag(message_stanza);
        if (mam_flag == null) {
            if (current_catchup_id[account] != -1) {
                var qry = db.mam_catchup.update()
                        .with(db.mam_catchup.id, "=", current_catchup_id[account])
                        .set(db.mam_catchup.to_time, (long)message.local_time.to_unix()); // TODO we know the server id
                qry.perform();
            }

            if (current_catchup_id[account] == -1 && message.body != null && message.type_ == Message.Type.CHAT) {
                print("insert bacause on msg rec\n");
                print(message_stanza.stanza.to_string());
                print(@"current_catchup_id $(current_catchup_id[account])\n");
                var qry = db.mam_catchup.insert()
                        .value(db.mam_catchup.account_id, account.id)
                        .value(db.mam_catchup.from_time, (long)message.local_time.to_unix())
                        .value(db.mam_catchup.to_time, (long)message.local_time.to_unix()); // TODO we know the server id
                current_catchup_id[account] = (int)qry.perform();
                print(@"current_catchup_id $(current_catchup_id[account])\n");
            }
        } else {
            mam_times[mam_flag.mam_id] = mam_flag.server_time;
//            print(@"    $(mam_flag.server_time) $(mam_flag.mam_id) $(message.body ?? "")\n");
        }

        bool abort = yield received_pipeline.run(message, message_stanza, conversation);
        if (abort) return;

        if (message.direction == Entities.Message.DIRECTION_RECEIVED) {
            message_received(message, conversation);
        } else if (message.direction == Entities.Message.DIRECTION_SENT) {
            message_sent(message, conversation);
        }
    }

    public async Entities.Message parse_message_stanza(Account account, Xmpp.MessageStanza message) {
        Entities.Message new_message = new Entities.Message(message.body);
        new_message.account = account;
        new_message.stanza_id = message.id;

        Jid? counterpart_override = null;
        if (message.from.equals(stream_interactor.get_module(MucManager.IDENTITY).get_own_jid(message.from.bare_jid, account))) {
            new_message.direction = Entities.Message.DIRECTION_SENT;
            counterpart_override = message.from.bare_jid;
        } else if (account.bare_jid.equals_bare(message.from)) {
            new_message.direction = Entities.Message.DIRECTION_SENT;
        } else {
            new_message.direction = Entities.Message.DIRECTION_RECEIVED;
        }
        new_message.counterpart = counterpart_override ?? (new_message.direction == Entities.Message.DIRECTION_SENT ? message.to : message.from);
        new_message.ourpart = new_message.direction == Entities.Message.DIRECTION_SENT ? message.from : message.to;

        Xep.MessageArchiveManagement.MessageFlag? mam_message_flag = Xep.MessageArchiveManagement.MessageFlag.get_flag(message);
        if (mam_message_flag != null) new_message.local_time = mam_message_flag.server_time;
        if (new_message.local_time == null || new_message.local_time.compare(new DateTime.now_utc()) > 0) new_message.local_time = new DateTime.now_utc();

        Xep.DelayedDelivery.MessageFlag? delayed_message_flag = Xep.DelayedDelivery.MessageFlag.get_flag(message);
        if (delayed_message_flag != null) new_message.time = delayed_message_flag.datetime;
        if (new_message.time == null || new_message.time.compare(new_message.local_time) > 0) new_message.time = new_message.local_time;

        new_message.type_ = yield determine_message_type(account, message, new_message);

        return new_message;
    }

    private async Entities.Message.Type determine_message_type(Account account, Xmpp.MessageStanza message_stanza, Entities.Message message) {
        if (message_stanza.type_ == Xmpp.MessageStanza.TYPE_GROUPCHAT) {
            return Entities.Message.Type.GROUPCHAT;
        }
        if (message_stanza.type_ == Xmpp.MessageStanza.TYPE_CHAT) {
            Conversation? conversation = stream_interactor.get_module(ConversationManager.IDENTITY).get_conversation(message.counterpart.bare_jid, account);
            if (conversation != null) {
                if (conversation.type_ == Conversation.Type.CHAT) {
                    return Entities.Message.Type.CHAT;
                } else if (conversation.type_ == Conversation.Type.GROUPCHAT) {
                    return Entities.Message.Type.GROUPCHAT_PM;
                }
            } else {
                SourceFunc callback = determine_message_type.callback;
                XmppStream stream = stream_interactor.get_stream(account);
                if (stream != null) stream.get_module(Xep.ServiceDiscovery.Module.IDENTITY).get_entity_categories(stream, message.counterpart.bare_jid, (stream, identities) => {
                    if (identities == null) {
                        message.type_ = Entities.Message.Type.CHAT;
                        Idle.add((owned) callback);
                        return;
                    }
                    foreach (Xep.ServiceDiscovery.Identity identity in identities) {
                        if (identity.category == Xep.ServiceDiscovery.Identity.CATEGORY_CONFERENCE) {
                            message.type_ = Entities.Message.Type.GROUPCHAT_PM;
                        } else {
                            message.type_ = Entities.Message.Type.CHAT;
                        }
                    }
                    Idle.add((owned) callback);
                });
                yield;
            }
        }
        return Entities.Message.Type.CHAT;
    }

    private class DeduplicateMessageListener : MessageListener {

        public string[] after_actions_const = new string[]{ "FILTER_EMPTY", "MUC" };
        public override string action_group { get { return "DEDUPLICATE"; } }
        public override string[] after_actions { get { return after_actions_const; } }

        private Database db;

        public DeduplicateMessageListener(Database db) {
            this.db = db;
        }

        public override async bool run(Entities.Message message, Xmpp.MessageStanza stanza, Conversation conversation) {
            bool is_uuid = message.stanza_id != null && Regex.match_simple("""[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}""", message.stanza_id);
            bool new_uuid_msg = is_uuid && !db.contains_message_by_stanza_id(message, conversation.account);
            bool new_misc_msg = !is_uuid && !db.contains_message(message, conversation.account);
            bool new_msg = new_uuid_msg || new_misc_msg;
            return !new_msg;
        }
    }

    private class FilterMessageListener : MessageListener {

        public string[] after_actions_const = new string[]{ "DECRYPT" };
        public override string action_group { get { return "FILTER_EMPTY"; } }
        public override string[] after_actions { get { return after_actions_const; } }

        public override async bool run(Entities.Message message, Xmpp.MessageStanza stanza, Conversation conversation) {
            return (message.body == null);
        }
    }

    private class StoreMessageListener : MessageListener {

        public string[] after_actions_const = new string[]{ "DEDUPLICATE", "DECRYPT", "FILTER_EMPTY" };
        public override string action_group { get { return "STORE"; } }
        public override string[] after_actions { get { return after_actions_const; } }

        private StreamInteractor stream_interactor;

        public StoreMessageListener(StreamInteractor stream_interactor) {
            this.stream_interactor = stream_interactor;
        }

        public override async bool run(Entities.Message message, Xmpp.MessageStanza stanza, Conversation conversation) {
            if (message.body == null) return true;
            stream_interactor.get_module(MessageStorage.IDENTITY).add_message(message, conversation);
            return false;
        }
    }

    private class MamMessageListener : MessageListener {

        public string[] after_actions_const = new string[]{ "DEDUPLICATE" };
        public override string action_group { get { return "MAM_NODE"; } }
        public override string[] after_actions { get { return after_actions_const; } }

        private StreamInteractor stream_interactor;

        public MamMessageListener(StreamInteractor stream_interactor) {
            this.stream_interactor = stream_interactor;
        }

        public override async bool run(Entities.Message message, Xmpp.MessageStanza stanza, Conversation conversation) {
            bool is_mam_message = Xep.MessageArchiveManagement.MessageFlag.get_flag(stanza) != null;
            XmppStream? stream = stream_interactor.get_stream(conversation.account);
            Xep.MessageArchiveManagement.Flag? mam_flag = stream != null ? stream.get_flag(Xep.MessageArchiveManagement.Flag.IDENTITY) : null;
            if (is_mam_message || (mam_flag != null && mam_flag.cought_up == true)) {
                conversation.account.mam_earliest_synced = message.local_time;
            }
            return false;
        }
    }

    public Entities.Message create_out_message(string text, Conversation conversation) {
        Entities.Message message = new Entities.Message(text);
        message.type_ = Util.get_message_type_for_conversation(conversation);
        message.stanza_id = random_uuid();
        message.account = conversation.account;
        message.body = text;
        message.time = new DateTime.now_utc();
        message.local_time = new DateTime.now_utc();
        message.direction = Entities.Message.DIRECTION_SENT;
        message.counterpart = conversation.counterpart;
        if (conversation.type_ in new Conversation.Type[]{Conversation.Type.GROUPCHAT, Conversation.Type.GROUPCHAT_PM}) {
            message.ourpart = stream_interactor.get_module(MucManager.IDENTITY).get_own_jid(conversation.counterpart, conversation.account) ?? conversation.account.bare_jid;
            message.real_jid = conversation.account.bare_jid;
        } else {
            message.ourpart = conversation.account.bare_jid.with_resource(conversation.account.resourcepart);
        }
        message.marked = Entities.Message.Marked.UNSENT;
        message.encryption = conversation.encryption;
        return message;
    }

    public void send_xmpp_message(Entities.Message message, Conversation conversation, bool delayed = false) {
        lock (lock_send_unsent) {
            XmppStream stream = stream_interactor.get_stream(conversation.account);
            message.marked = Entities.Message.Marked.NONE;
            if (stream != null) {
                Xmpp.MessageStanza new_message = new Xmpp.MessageStanza(message.stanza_id);
                new_message.to = message.counterpart;
                new_message.body = message.body;
                if (conversation.type_ == Conversation.Type.GROUPCHAT) {
                    new_message.type_ = Xmpp.MessageStanza.TYPE_GROUPCHAT;
                } else {
                    new_message.type_ = Xmpp.MessageStanza.TYPE_CHAT;
                }
                build_message_stanza(message, new_message, conversation);
                pre_message_send(message, new_message, conversation);
                if (message.marked == Entities.Message.Marked.UNSENT || message.marked == Entities.Message.Marked.WONTSEND) return;
                if (delayed) {
                    Xmpp.Xep.DelayedDelivery.Module.set_message_delay(new_message, message.time);
                }
                stream.get_module(Xmpp.MessageModule.IDENTITY).send_message(stream, new_message);
                message.stanza_id = new_message.id;
            } else {
                message.marked = Entities.Message.Marked.UNSENT;
            }
        }
    }
}

public abstract class MessageListener : Xmpp.OrderedListener {

    public abstract async bool run(Entities.Message message, Xmpp.MessageStanza stanza, Conversation conversation);
}

public class MessageListenerHolder : Xmpp.ListenerHolder {

    public async bool run(Entities.Message message, Xmpp.MessageStanza stanza, Conversation conversation) {
        foreach (OrderedListener ol in listeners) {
            MessageListener l = ol as MessageListener;
            bool stop = yield l.run(message, stanza, conversation);
            if (stop) return true;
        }
        return false;
    }
}

}
