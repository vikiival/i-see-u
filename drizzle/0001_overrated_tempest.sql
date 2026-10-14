CREATE TABLE "balance_snapshots" (
	"network" text NOT NULL,
	"account" text NOT NULL,
	"token" text NOT NULL,
	"balance" text,
	"error" text,
	"block_number" bigint NOT NULL,
	"block_hash" text NOT NULL,
	CONSTRAINT "balance_snapshots_network_account_token_pk" PRIMARY KEY("network","account","token")
);
--> statement-breakpoint
CREATE TABLE "follower_cursor" (
	"network" text PRIMARY KEY NOT NULL,
	"block_number" bigint NOT NULL,
	"block_hash" text NOT NULL,
	"block_timestamp" bigint NOT NULL,
	"updated_at" bigint NOT NULL
);
--> statement-breakpoint
CREATE TABLE "tracked_accounts" (
	"network" text NOT NULL,
	"account" text NOT NULL,
	"backfill_target" bigint NOT NULL,
	"tracked_at" bigint NOT NULL,
	CONSTRAINT "tracked_accounts_network_account_pk" PRIMARY KEY("network","account")
);
