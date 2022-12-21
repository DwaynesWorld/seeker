CREATE KEYSPACE IF NOT EXISTS adm
    WITH REPLICATION = {
        'class': 'SimpleStrategy',
        'replication_factor': 1
    };

USE "adm";

CREATE TABLE IF NOT EXISTS clusters (
	"id" bigint,
    "kind" int,
    "name" text,
    "meta" map<text, text>,
    "created_at" timestamp,
    "updated_at" timestamp,
    PRIMARY KEY (id)
) WITH CLUSTERING ORDER BY (id DESC);


CREATE TABLE IF NOT EXISTS subscriptions (
	"id" bigint,
    "cluster_id" bigint,
    "topic_name" text,
    "meta" map<text, text>,
    "created_at" timestamp,
    "updated_at" timestamp,
    PRIMARY KEY (cluster_id, id)
) WITH CLUSTERING ORDER BY (id DESC);
