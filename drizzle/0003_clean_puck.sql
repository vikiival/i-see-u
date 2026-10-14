CREATE TABLE "tail_blocks" (
	"network" text NOT NULL,
	"number" bigint NOT NULL,
	"hash" text NOT NULL,
	"parent_hash" text NOT NULL,
	"timestamp" bigint NOT NULL,
	CONSTRAINT "tail_blocks_network_number_pk" PRIMARY KEY("network","number")
);
--> statement-breakpoint
CREATE TABLE "tail_transfers" (
	"network" text NOT NULL,
	"block_number" bigint NOT NULL,
	"log_index" integer NOT NULL,
	"block_hash" text NOT NULL,
	"token" text NOT NULL,
	"standard" text NOT NULL,
	"from" text NOT NULL,
	"to" text NOT NULL,
	"value" text NOT NULL,
	"token_id" text,
	"tx_hash" text NOT NULL,
	CONSTRAINT "tail_transfers_network_block_number_log_index_pk" PRIMARY KEY("network","block_number","log_index")
);
--> statement-breakpoint
CREATE INDEX "tail_transfers_network_from_idx" ON "tail_transfers" USING btree ("network","from");--> statement-breakpoint
CREATE INDEX "tail_transfers_network_to_idx" ON "tail_transfers" USING btree ("network","to");