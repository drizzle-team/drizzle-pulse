CREATE SCHEMA "realtime";
--> statement-breakpoint
CREATE TABLE "realtime"."events_public_orders" (
	"id" integer NOT NULL,
	"driver_id" integer,
	"pickup" text,
	"dropoff" text,
	"price" numeric,
	"status" text,
	"accepted_at" timestamp with time zone,
	"created_at" timestamp with time zone,
	"updated_at" timestamp with time zone,
	"$old_id" integer,
	"$old_driver_id" integer,
	"$old_pickup" text,
	"$old_dropoff" text,
	"$old_price" numeric,
	"$old_status" text,
	"$old_accepted_at" timestamp with time zone,
	"$old_created_at" timestamp with time zone,
	"$old_updated_at" timestamp with time zone,
	"$snapshot" integer GENERATED ALWAYS AS IDENTITY (sequence name "realtime"."events_public_orders_$snapshot_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START WITH 1 CACHE 1),
	"$op" text NOT NULL,
	"$timestamp" timestamp with time zone DEFAULT now() NOT NULL
);
