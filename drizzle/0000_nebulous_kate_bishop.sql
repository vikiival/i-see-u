CREATE TABLE "balance_snapshots" (
	"network" smallint NOT NULL,
	"account" "bytea" NOT NULL,
	"token" "bytea" NOT NULL,
	"balance" numeric(78,0),
	"error" text,
	"block_number" bigint NOT NULL,
	"block_hash" "bytea" NOT NULL,
	"read_at" bigint NOT NULL,
	CONSTRAINT "balance_snapshots_network_account_token_pk" PRIMARY KEY("network","account","token"),
	CONSTRAINT "balance_addresses" CHECK (octet_length("balance_snapshots"."account") = 20 AND octet_length("balance_snapshots"."token") = 20 AND octet_length("balance_snapshots"."block_hash") = 32),
	CONSTRAINT "balance_result" CHECK (("balance_snapshots"."balance" IS NULL) <> ("balance_snapshots"."error" IS NULL)),
	CONSTRAINT "balance_uint" CHECK ("balance_snapshots"."balance" >= 0 AND "balance_snapshots"."balance" <= 115792089237316195423570985008687907853269984665640564039457584007913129639935)
);
--> statement-breakpoint
CREATE TABLE "block_headers" (
	"network" smallint NOT NULL,
	"number" bigint NOT NULL,
	"hash" "bytea" NOT NULL,
	"parent_hash" "bytea" NOT NULL,
	"timestamp" bigint NOT NULL,
	CONSTRAINT "block_headers_network_number_pk" PRIMARY KEY("network","number"),
	CONSTRAINT "header_hash_width" CHECK (octet_length("block_headers"."hash") = 32 AND octet_length("block_headers"."parent_hash") = 32)
);
--> statement-breakpoint
CREATE TABLE "holdings" (
	"network" smallint NOT NULL,
	"holder" "bytea" NOT NULL,
	"token" "bytea" NOT NULL,
	"standard" smallint NOT NULL,
	"first_block" bigint NOT NULL,
	"last_block" bigint NOT NULL,
	"sent_count" bigint NOT NULL,
	"received_count" bigint NOT NULL,
	CONSTRAINT "holdings_network_holder_token_pk" PRIMARY KEY("network","holder","token"),
	CONSTRAINT "holding_addresses" CHECK (octet_length("holdings"."holder") = 20 AND octet_length("holdings"."token") = 20),
	CONSTRAINT "holding_counts" CHECK ("holdings"."sent_count" >= 0 AND "holdings"."received_count" >= 0 AND "holdings"."first_block" <= "holdings"."last_block"),
	CONSTRAINT "holding_standard" CHECK ("holdings"."standard" IN (20, 721))
);
--> statement-breakpoint
CREATE TABLE "ingestion_state" (
	"network" smallint PRIMARY KEY NOT NULL,
	"start_block" bigint NOT NULL,
	"block_number" bigint NOT NULL,
	"block_hash" "bytea" NOT NULL,
	"block_timestamp" bigint NOT NULL,
	"updated_at" bigint NOT NULL,
	"revision" bigint DEFAULT 0 NOT NULL,
	"head_number" bigint,
	"head_hash" "bytea",
	"head_timestamp" bigint,
	"checked_at" bigint,
	CONSTRAINT "state_hash_width" CHECK (octet_length("ingestion_state"."block_hash") = 32 AND ("ingestion_state"."head_hash" IS NULL OR octet_length("ingestion_state"."head_hash") = 32)),
	CONSTRAINT "state_progress" CHECK ("ingestion_state"."start_block" >= 0 AND "ingestion_state"."block_number" >= "ingestion_state"."start_block" - 1 AND "ingestion_state"."revision" >= 0)
);
--> statement-breakpoint
CREATE TABLE "nft_ownership" (
	"network" smallint NOT NULL,
	"token" "bytea" NOT NULL,
	"token_id" numeric(78,0) NOT NULL,
	"owner" "bytea" NOT NULL,
	"last_block" bigint NOT NULL,
	"last_log_index" integer NOT NULL,
	CONSTRAINT "nft_ownership_network_token_token_id_pk" PRIMARY KEY("network","token","token_id"),
	CONSTRAINT "nft_addresses" CHECK (octet_length("nft_ownership"."owner") = 20 AND octet_length("nft_ownership"."token") = 20),
	CONSTRAINT "nft_id" CHECK ("nft_ownership"."token_id" >= 0 AND "nft_ownership"."token_id" <= 115792089237316195423570985008687907853269984665640564039457584007913129639935)
);
--> statement-breakpoint
CREATE TABLE "repair_queue" (
	"network" smallint NOT NULL,
	"from_block" bigint NOT NULL,
	"to_block" bigint NOT NULL,
	"reason" text NOT NULL,
	"created_at" bigint NOT NULL,
	CONSTRAINT "repair_queue_network_from_block_to_block_pk" PRIMARY KEY("network","from_block","to_block"),
	CONSTRAINT "repair_range" CHECK ("repair_queue"."from_block" >= 0 AND "repair_queue"."to_block" >= "repair_queue"."from_block")
);
--> statement-breakpoint
CREATE TABLE "transfers" (
	"network" smallint NOT NULL,
	"block_number" bigint NOT NULL,
	"log_index" integer NOT NULL,
	"block_hash" "bytea" NOT NULL,
	"transaction_hash" "bytea" NOT NULL,
	"token" "bytea" NOT NULL,
	"standard" smallint NOT NULL,
	"from" "bytea" NOT NULL,
	"to" "bytea" NOT NULL,
	"value" numeric(78,0) NOT NULL,
	CONSTRAINT "transfers_network_block_number_log_index_pk" PRIMARY KEY("network","block_number","log_index"),
	CONSTRAINT "transfer_addresses" CHECK (octet_length("transfers"."token") = 20 AND octet_length("transfers"."from") = 20 AND octet_length("transfers"."to") = 20),
	CONSTRAINT "transfer_hashes" CHECK (octet_length("transfers"."block_hash") = 32 AND octet_length("transfers"."transaction_hash") = 32),
	CONSTRAINT "transfer_quantities" CHECK ("transfers"."block_number" >= 0 AND "transfers"."log_index" >= 0 AND "transfers"."value" >= 0 AND "transfers"."value" <= 115792089237316195423570985008687907853269984665640564039457584007913129639935),
	CONSTRAINT "transfer_standard" CHECK ("transfers"."standard" IN (20, 721))
);
--> statement-breakpoint
CREATE INDEX "balance_blocks" ON "balance_snapshots" USING btree ("network","block_number");--> statement-breakpoint
CREATE INDEX "holdings_token_holders" ON "holdings" USING btree ("network","token","holder");--> statement-breakpoint
CREATE INDEX "nft_owner_items" ON "nft_ownership" USING btree ("network","owner","token","token_id");--> statement-breakpoint
CREATE INDEX "transfers_from_history" ON "transfers" USING btree ("network","from","block_number" DESC NULLS LAST,"log_index" DESC NULLS LAST);--> statement-breakpoint
CREATE INDEX "transfers_to_history" ON "transfers" USING btree ("network","to","block_number" DESC NULLS LAST,"log_index" DESC NULLS LAST);--> statement-breakpoint
CREATE INDEX "transfers_nft_history" ON "transfers" USING btree ("network","token","value","block_number" DESC NULLS LAST,"log_index" DESC NULLS LAST) WHERE "transfers"."standard" = 721;