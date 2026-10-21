-- postgres/03-seed.sql
-- Demo data for the Memory Concierge. Contract: docs/DESIGN.md section 4. Ported from the CockroachDB demo: identical data.
-- Idempotent: every INSERT is ON CONFLICT DO NOTHING and every key is
-- deterministic, so re-running never duplicates rows and never resets state
-- that the demo changed (scripts/demo-reset.sh does that).
--
-- The demo user is zeke.dean@redpanda.com. alice@example.com and
-- bob@example.com exist so the "only WHERE owner_email = caller" scoping rule
-- is testable. No memories are seeded: the demo starts from an empty memory.

SET search_path TO agent_memory, public;

-- Customers (deterministic UUIDs so accounts can reference them without a lookup).
INSERT INTO customers (customer_id, email, full_name, segment, tax_id, phone) VALUES
  ('a0000000-0000-4000-8000-000000000001', 'zeke.dean@redpanda.com', 'Zeke Dean',    'premier',  '987-65-4321', '+1-415-555-0142'),
  ('a0000000-0000-4000-8000-000000000002', 'alice@example.com',      'Alice Nguyen', 'retail',   '123-45-6789', '+1-206-555-0177'),
  ('a0000000-0000-4000-8000-000000000003', 'bob@example.com',        'Bob Okafor',   'business', '555-12-3456', '+1-312-555-0133')
ON CONFLICT DO NOTHING;

-- Accounts. Zeke: 1001 checking (overdraft_limit 500), 1002 savings, 1003 credit.
INSERT INTO accounts (account_id, customer_id, owner_email, account_number, nickname, account_type, balance, currency, overdraft_limit, status) VALUES
  (1001, 'a0000000-0000-4000-8000-000000000001', 'zeke.dean@redpanda.com', '4419-8832-1001', 'Everyday Checking',  'checking',  2450.00, 'USD',  500.00, 'open'),
  (1002, 'a0000000-0000-4000-8000-000000000001', 'zeke.dean@redpanda.com', '4419-8832-1002', 'House Fund',         'savings',  41200.00, 'USD',    0.00, 'open'),
  (1003, 'a0000000-0000-4000-8000-000000000001', 'zeke.dean@redpanda.com', '4419-8832-1003', 'Travel Card',        'credit',    -812.40, 'USD',    0.00, 'open'),
  (2001, 'a0000000-0000-4000-8000-000000000002', 'alice@example.com',      '4419-8832-2001', 'Main Checking',      'checking',  1830.55, 'USD',  200.00, 'open'),
  (2002, 'a0000000-0000-4000-8000-000000000002', 'alice@example.com',      '4419-8832-2002', 'Rainy Day',          'savings',   9400.00, 'USD',    0.00, 'open'),
  (3001, 'a0000000-0000-4000-8000-000000000003', 'bob@example.com',        '4419-8832-3001', 'Business Operating', 'checking', 15320.10, 'USD', 1000.00, 'open'),
  (3002, 'a0000000-0000-4000-8000-000000000003', 'bob@example.com',        '4419-8832-3002', 'Business Credit',    'credit',   -2240.00, 'USD',    0.00, 'open')
ON CONFLICT DO NOTHING;

-- Transactions: 25 on Zeke's Everyday Checking (1001) over the last 30 days.
-- Deterministic ids c0ffee00-0000-4000-8000-0000000000NN (a uuid range, not a text
-- cast: casting the key inside this UPDATE tripped a GantrySQL covering-scan bug). Negative = debit.
-- The 25 amounts sum to exactly 2450.00, the seeded balance of account 1001.
-- Timestamps are relative to the run that first inserts a row (ON CONFLICT DO
-- NOTHING keeps them); the re-anchor UPDATE at the end of this file keeps the
-- ledger inside the agent's 30-day window on every re-run.
INSERT INTO transactions (txn_id, account_id, amount, merchant, category, occurred_at) VALUES
  ('c0ffee00-0000-4000-8000-000000000001', 1001,  3200.00, 'Acme Corp Payroll',       'payroll',       now() - '29 days 9 hours'::INTERVAL),
  ('c0ffee00-0000-4000-8000-000000000002', 1001, -1850.00, 'Riverside Property Mgmt', 'rent',          now() - '28 days 7 hours'::INTERVAL),
  ('c0ffee00-0000-4000-8000-000000000003', 1001,  -112.48, 'Whole Foods Market',      'groceries',     now() - '27 days 5 hours'::INTERVAL),
  ('c0ffee00-0000-4000-8000-000000000004', 1001,    -5.75, 'Blue Bottle Coffee',      'coffee',        now() - '27 days 1 hour'::INTERVAL),
  ('c0ffee00-0000-4000-8000-000000000005', 1001,   -64.20, 'Shell Gas Station',       'transport',     now() - '26 days 3 hours'::INTERVAL),
  ('c0ffee00-0000-4000-8000-000000000006', 1001,   -15.99, 'Netflix',                 'subscriptions', now() - '25 days 2 hours'::INTERVAL),
  ('c0ffee00-0000-4000-8000-000000000007', 1001,   -48.30, 'Trader Joe''s',           'groceries',     now() - '24 days 6 hours'::INTERVAL),
  ('c0ffee00-0000-4000-8000-000000000008', 1001,    -6.25, 'Blue Bottle Coffee',      'coffee',        now() - '23 days 1 hour'::INTERVAL),
  ('c0ffee00-0000-4000-8000-000000000009', 1001,  -142.17, 'PG&E',                    'utilities',     now() - '22 days 4 hours'::INTERVAL),
  ('c0ffee00-0000-4000-8000-000000000010', 1001,  -500.00, 'Transfer to House Fund',  'transfer',      now() - '21 days 8 hours'::INTERVAL),
  ('c0ffee00-0000-4000-8000-000000000011', 1001,   -38.90, 'Uber',                    'transport',     now() - '20 days 11 hours'::INTERVAL),
  ('c0ffee00-0000-4000-8000-000000000012', 1001,   -89.99, 'Comcast Xfinity',         'utilities',     now() - '19 days 2 hours'::INTERVAL),
  ('c0ffee00-0000-4000-8000-000000000013', 1001,   -27.60, 'Chipotle',                'dining',        now() - '18 days 5 hours'::INTERVAL),
  ('c0ffee00-0000-4000-8000-000000000014', 1001,    -5.75, 'Blue Bottle Coffee',      'coffee',        now() - '16 days 1 hour'::INTERVAL),
  ('c0ffee00-0000-4000-8000-000000000015', 1001,  3200.00, 'Acme Corp Payroll',       'payroll',       now() - '15 days 9 hours'::INTERVAL),
  ('c0ffee00-0000-4000-8000-000000000016', 1001,  -131.05, 'Whole Foods Market',      'groceries',     now() - '14 days 6 hours'::INTERVAL),
  ('c0ffee00-0000-4000-8000-000000000017', 1001,   -10.99, 'Spotify',                 'subscriptions', now() - '13 days 2 hours'::INTERVAL),
  ('c0ffee00-0000-4000-8000-000000000018', 1001,   -72.40, 'Olive Garden',            'dining',        now() - '12 days 4 hours'::INTERVAL),
  ('c0ffee00-0000-4000-8000-000000000019', 1001,  -500.00, 'Transfer to House Fund',  'transfer',      now() - '11 days 8 hours'::INTERVAL),
  ('c0ffee00-0000-4000-8000-000000000020', 1001,    -6.25, 'Blue Bottle Coffee',      'coffee',        now() - '9 days 1 hour'::INTERVAL),
  ('c0ffee00-0000-4000-8000-000000000021', 1001,   -55.12, 'Trader Joe''s',           'groceries',     now() - '8 days 5 hours'::INTERVAL),
  ('c0ffee00-0000-4000-8000-000000000022', 1001,  -120.00, 'Travel Card Payment',     'transfer',      now() - '6 days 7 hours'::INTERVAL),
  ('c0ffee00-0000-4000-8000-000000000023', 1001,   -42.75, 'Uber',                    'transport',     now() - '4 days 10 hours'::INTERVAL),
  ('c0ffee00-0000-4000-8000-000000000024', 1001,   -98.31, 'Whole Foods Market',      'groceries',     now() - '2 days 3 hours'::INTERVAL),
  ('c0ffee00-0000-4000-8000-000000000025', 1001,    -5.75, 'Blue Bottle Coffee',      'coffee',        now() - '1 day 1 hour'::INTERVAL)
ON CONFLICT DO NOTHING;

-- A little activity for the other customers so cross-customer scoping is testable.
INSERT INTO transactions (txn_id, account_id, amount, merchant, category, occurred_at) VALUES
  ('c0ffee00-0000-4000-8000-0000000000a1', 2001,  2100.00, 'Northwind Payroll',   'payroll',   now() - '12 days 9 hours'::INTERVAL),
  ('c0ffee00-0000-4000-8000-0000000000a2', 2001,   -76.14, 'Safeway',             'groceries', now() - '3 days 6 hours'::INTERVAL),
  ('c0ffee00-0000-4000-8000-0000000000b1', 3001,  8500.00, 'Invoice 2041 - Contoso', 'revenue', now() - '10 days 4 hours'::INTERVAL),
  ('c0ffee00-0000-4000-8000-0000000000b2', 3001, -1299.00, 'Dell Business',       'equipment', now() - '5 days 2 hours'::INTERVAL)
ON CONFLICT DO NOTHING;

-- Re-anchor the seeded ledger. ON CONFLICT DO NOTHING freezes occurred_at at the
-- first seed run, and the agent's spending queries (adp/system-prompt.md) look
-- at the last 30 days, so without this the seeded activity drifts out of the
-- window (half of it gone after two weeks). Shift every seeded transaction by
-- one delta, preserving the spacing, so the newest one on account 1001 (...025,
-- seeded as "1 day 1 hour ago") lands there again. Runs on every setup-db and on
-- scripts/demo-reset.sh, which re-applies this file. transactions has no
-- trigger, so this is side-effect free. The demo rent debit
-- (c0ffee00-0000-4000-8000-000000000999) is not a seeded row and is excluded
-- (outside the 0000000000NN range).
-- Idempotent: a re-run shifts by the seconds elapsed since the last run.
WITH anchor AS (
  SELECT (now() - '1 day 1 hour'::INTERVAL) - max(occurred_at) AS delta
  FROM transactions
  WHERE account_id = 1001
    AND txn_id BETWEEN 'c0ffee00-0000-4000-8000-000000000000' AND 'c0ffee00-0000-4000-8000-0000000000ff'
)
UPDATE transactions
SET occurred_at = occurred_at + (SELECT delta FROM anchor)
WHERE txn_id BETWEEN 'c0ffee00-0000-4000-8000-000000000000' AND 'c0ffee00-0000-4000-8000-0000000000ff'
  AND txn_id <> 'c0ffee00-0000-4000-8000-000000000999'
  AND (SELECT delta FROM anchor) IS NOT NULL;
