-- postgres/02-triggers.sql
-- Change capture for the Memory Concierge: AFTER ROW triggers append to
-- agent_memory.change_log (01-schema.sql). Contract: docs/PORT-CONTRACT.md
-- section 4. Idempotent: CREATE OR REPLACE FUNCTION, DROP TRIGGER IF EXISTS.
--
-- GantrySQL rules learned live, which is why the bodies look the way they do:
--   - to_jsonb(NEW) loses the column names (keys come back as f1, f2 ...), so
--     the documents are built with jsonb_build_object, column by column
--   - casts of NEW/OLD fields inside a trigger body fail, so nothing is cast
--     (all keys are BIGINT, timestamps and numerics go in as they are)
--   - RETURN COALESCE(NEW, OLD) is rejected; each branch returns NEW or OLD
--
-- The account document leaves out account_number (sensitive) and customer_id:
-- the change stream never carries them, so neither does the topic or the audit.

SET search_path TO agent_memory, public;

CREATE OR REPLACE FUNCTION agent_memory.log_memory_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    INSERT INTO agent_memory.change_log (table_name, op, row_key, subject, before_doc, after_doc)
    VALUES ('memories', 'insert', NEW.memory_id, NEW.subject, NULL,
            jsonb_build_object('memory_id', NEW.memory_id, 'subject', NEW.subject, 'kind', NEW.kind,
                               'content', NEW.content, 'importance', NEW.importance, 'source', NEW.source,
                               'source_ref', NEW.source_ref, 'created_at', NEW.created_at,
                               'updated_at', NEW.updated_at, 'expires_at', NEW.expires_at));
    RETURN NEW;
  ELSIF TG_OP = 'UPDATE' THEN
    INSERT INTO agent_memory.change_log (table_name, op, row_key, subject, before_doc, after_doc)
    VALUES ('memories', 'update', NEW.memory_id, NEW.subject,
            jsonb_build_object('memory_id', OLD.memory_id, 'subject', OLD.subject, 'kind', OLD.kind,
                               'content', OLD.content, 'importance', OLD.importance, 'source', OLD.source,
                               'source_ref', OLD.source_ref, 'created_at', OLD.created_at,
                               'updated_at', OLD.updated_at, 'expires_at', OLD.expires_at),
            jsonb_build_object('memory_id', NEW.memory_id, 'subject', NEW.subject, 'kind', NEW.kind,
                               'content', NEW.content, 'importance', NEW.importance, 'source', NEW.source,
                               'source_ref', NEW.source_ref, 'created_at', NEW.created_at,
                               'updated_at', NEW.updated_at, 'expires_at', NEW.expires_at));
    RETURN NEW;
  ELSE
    INSERT INTO agent_memory.change_log (table_name, op, row_key, subject, before_doc, after_doc)
    VALUES ('memories', 'delete', OLD.memory_id, OLD.subject,
            jsonb_build_object('memory_id', OLD.memory_id, 'subject', OLD.subject, 'kind', OLD.kind,
                               'content', OLD.content, 'importance', OLD.importance, 'source', OLD.source,
                               'source_ref', OLD.source_ref, 'created_at', OLD.created_at,
                               'updated_at', OLD.updated_at, 'expires_at', OLD.expires_at),
            NULL);
    RETURN OLD;
  END IF;
END
$$;

DROP TRIGGER IF EXISTS tr_memories_log ON agent_memory.memories;
CREATE TRIGGER tr_memories_log
  AFTER INSERT OR UPDATE OR DELETE ON agent_memory.memories
  FOR EACH ROW EXECUTE FUNCTION agent_memory.log_memory_change();

CREATE OR REPLACE FUNCTION agent_memory.log_account_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO agent_memory.change_log (table_name, op, row_key, subject, before_doc, after_doc)
  VALUES ('accounts', 'update', NEW.account_id, NEW.owner_email,
          jsonb_build_object('account_id', OLD.account_id, 'owner_email', OLD.owner_email,
                             'nickname', OLD.nickname, 'account_type', OLD.account_type,
                             'balance', OLD.balance, 'currency', OLD.currency,
                             'overdraft_limit', OLD.overdraft_limit, 'status', OLD.status,
                             'updated_at', OLD.updated_at),
          jsonb_build_object('account_id', NEW.account_id, 'owner_email', NEW.owner_email,
                             'nickname', NEW.nickname, 'account_type', NEW.account_type,
                             'balance', NEW.balance, 'currency', NEW.currency,
                             'overdraft_limit', NEW.overdraft_limit, 'status', NEW.status,
                             'updated_at', NEW.updated_at));
  RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS tr_accounts_log ON agent_memory.accounts;
CREATE TRIGGER tr_accounts_log
  AFTER UPDATE ON agent_memory.accounts
  FOR EACH ROW EXECUTE FUNCTION agent_memory.log_account_change();
