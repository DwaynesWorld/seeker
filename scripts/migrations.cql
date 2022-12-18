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
    PRIMARY KEY (kind, id)
) WITH CLUSTERING ORDER BY (id DESC);
