CREATE TABLE "bootstrap_headers" (
	"network" smallint NOT NULL,
	"number" bigint NOT NULL,
	"hash" "bytea" NOT NULL,
	"parent_hash" "bytea" NOT NULL,
	"timestamp" bigint NOT NULL,
	"transfer_bloom" boolean NOT NULL,
	CONSTRAINT "bootstrap_headers_network_number_pk" PRIMARY KEY("network","number")
);
--> statement-breakpoint
CREATE TABLE "bootstrap_runs" (
	"network" smallint PRIMARY KEY NOT NULL,
	"start_block" bigint NOT NULL,
	"target_block" bigint NOT NULL,
	"target_hash" "bytea" NOT NULL,
	"status" text NOT NULL,
	"started_at" bigint NOT NULL,
	"resumed_at" bigint NOT NULL,
	"updated_at" bigint NOT NULL,
	"error" text,
	CONSTRAINT "bootstrap_run_hash_width" CHECK (octet_length("bootstrap_runs"."target_hash") = 32),
	CONSTRAINT "bootstrap_run_range" CHECK ("bootstrap_runs"."start_block" >= 1 AND "bootstrap_runs"."target_block" >= "bootstrap_runs"."start_block")
);
--> statement-breakpoint
CREATE TABLE "bootstrap_segments" (
	"network" smallint NOT NULL,
	"from_block" bigint NOT NULL,
	"to_block" bigint NOT NULL,
	"done" boolean DEFAULT false NOT NULL,
	"source" text,
	"logs" bigint DEFAULT 0 NOT NULL,
	"calls" integer DEFAULT 0 NOT NULL,
	"attempts" integer DEFAULT 0 NOT NULL,
	"error" text,
	"updated_at" bigint,
	CONSTRAINT "bootstrap_segments_network_from_block_pk" PRIMARY KEY("network","from_block"),
	CONSTRAINT "bootstrap_segment_range" CHECK ("bootstrap_segments"."to_block" >= "bootstrap_segments"."from_block")
);
--> statement-breakpoint
CREATE TABLE "bootstrap_transfers" (
	"network" smallint NOT NULL,
	"block_number" bigint NOT NULL,
	"log_index" integer NOT NULL,
	"block_hash" "bytea" NOT NULL,
	"transaction_hash" "bytea" NOT NULL,
	"token" "bytea" NOT NULL,
	"standard" smallint NOT NULL,
	"from" "bytea" NOT NULL,
	"to" "bytea" NOT NULL,
	"value" numeric(78,0) NOT NULL
);
