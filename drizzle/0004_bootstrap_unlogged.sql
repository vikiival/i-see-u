-- Bulk staging skips WAL: a PostgreSQL crash truncates it, which the loader detects and reloads (see src/bootstrap).
ALTER TABLE "bootstrap_transfers" SET UNLOGGED;
