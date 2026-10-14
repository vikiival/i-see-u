CREATE TABLE "account_cursor" (
	"network" text NOT NULL,
	"account" text NOT NULL,
	"scanned_to" bigint NOT NULL,
	"scanned_to_hash" text NOT NULL,
	"scanned_to_timestamp" bigint NOT NULL,
	CONSTRAINT "account_cursor_network_account_pk" PRIMARY KEY("network","account")
);
--> statement-breakpoint
CREATE TABLE "account_scan_progress" (
	"network" text NOT NULL,
	"account" text NOT NULL,
	"progress_to" bigint NOT NULL,
	CONSTRAINT "account_scan_progress_network_account_pk" PRIMARY KEY("network","account")
);
--> statement-breakpoint
CREATE TABLE "account_scan_range" (
	"network" text NOT NULL,
	"account" text NOT NULL,
	"logs_range" bigint NOT NULL,
	CONSTRAINT "account_scan_range_network_account_pk" PRIMARY KEY("network","account")
);
--> statement-breakpoint
CREATE TABLE "holdings" (
	"network" text NOT NULL,
	"holder" text NOT NULL,
	"token" text NOT NULL,
	"standard" text NOT NULL,
	"first_block" bigint NOT NULL,
	"last_block" bigint NOT NULL,
	"sent_count" integer NOT NULL,
	"received_count" integer NOT NULL,
	CONSTRAINT "holdings_network_holder_token_pk" PRIMARY KEY("network","holder","token")
);
--> statement-breakpoint
CREATE TABLE "nft_items" (
	"network" text NOT NULL,
	"holder" text NOT NULL,
	"token" text NOT NULL,
	"token_id" text NOT NULL,
	"held" boolean NOT NULL,
	"last_block" bigint NOT NULL,
	"last_log_index" integer NOT NULL,
	CONSTRAINT "nft_items_network_holder_token_token_id_pk" PRIMARY KEY("network","holder","token","token_id")
);
