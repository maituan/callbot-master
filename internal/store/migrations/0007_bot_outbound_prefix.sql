-- 0007: outbound carrier-routing prefix per bot.
-- The FreeSWITCH dialplan picks the gateway by matching e.g. ^3323(\d{10})$
-- (minhphuc-vina) or ^3317(\d{10})$ (leeon-viettel). Master prepends this
-- prefix to the dialed phone before issuing originate, so the user can
-- enter clean Vietnamese numbers (0971...) on the form.
ALTER TABLE bots
    ADD COLUMN IF NOT EXISTS outbound_prefix TEXT NOT NULL DEFAULT '';
