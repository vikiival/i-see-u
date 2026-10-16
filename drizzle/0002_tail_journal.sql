CREATE TABLE "tail_blocks" (
	"network" smallint NOT NULL,
	"number" bigint NOT NULL,
	"hash" "bytea" NOT NULL,
	"parent_hash" "bytea" NOT NULL,
	"timestamp" bigint NOT NULL,
	CONSTRAINT "tail_blocks_network_number_pk" PRIMARY KEY("network","number"),
	CONSTRAINT "tail_block_hash_width" CHECK (octet_length("tail_blocks"."hash") = 32 AND octet_length("tail_blocks"."parent_hash") = 32)
);
--> statement-breakpoint
CREATE TABLE "tail_transfers" (
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
	CONSTRAINT "tail_transfers_network_block_number_log_index_pk" PRIMARY KEY("network","block_number","log_index"),
	CONSTRAINT "tail_transfer_addresses" CHECK (octet_length("tail_transfers"."token") = 20 AND octet_length("tail_transfers"."from") = 20 AND octet_length("tail_transfers"."to") = 20),
	CONSTRAINT "tail_transfer_hashes" CHECK (octet_length("tail_transfers"."block_hash") = 32 AND octet_length("tail_transfers"."transaction_hash") = 32),
	CONSTRAINT "tail_transfer_quantities" CHECK ("tail_transfers"."block_number" >= 0 AND "tail_transfers"."log_index" >= 0 AND "tail_transfers"."value" >= 0 AND "tail_transfers"."value" <= 115792089237316195423570985008687907853269984665640564039457584007913129639935),
	CONSTRAINT "tail_transfer_standard" CHECK ("tail_transfers"."standard" IN (20, 721))
);
--> statement-breakpoint
CREATE INDEX "tail_transfers_from_history" ON "tail_transfers" USING btree ("network","from","block_number" DESC NULLS LAST,"log_index" DESC NULLS LAST);--> statement-breakpoint
CREATE INDEX "tail_transfers_to_history" ON "tail_transfers" USING btree ("network","to","block_number" DESC NULLS LAST,"log_index" DESC NULLS LAST);