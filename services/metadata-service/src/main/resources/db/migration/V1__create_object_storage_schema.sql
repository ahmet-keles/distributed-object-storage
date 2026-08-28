-- Object metadata and chunk locations. The bytes themselves live on the
-- storage nodes; these tables record how an object was cut into chunks,
-- what every piece must hash to, and which nodes hold each copy. Reads are
-- served from chunk_replicas (what was actually written), never re-derived
-- from the placement function.

CREATE TABLE objects (
    id UUID NOT NULL,
    object_key VARCHAR(200) NOT NULL,
    size_bytes BIGINT NOT NULL,
    chunk_size_bytes INTEGER NOT NULL,
    chunk_count INTEGER NOT NULL,
    sha256 VARCHAR(64) NOT NULL,
    created_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,

    CONSTRAINT objects_pkey PRIMARY KEY (id),
    CONSTRAINT objects_object_key_unique UNIQUE (object_key),
    CONSTRAINT objects_size_nonnegative CHECK (size_bytes >= 0),
    CONSTRAINT objects_chunk_size_positive CHECK (chunk_size_bytes > 0),
    CONSTRAINT objects_chunk_count_positive CHECK (chunk_count >= 1)
);

CREATE TABLE chunks (
    id UUID NOT NULL,
    object_id UUID NOT NULL,
    chunk_index INTEGER NOT NULL,
    size_bytes INTEGER NOT NULL,
    sha256 VARCHAR(64) NOT NULL,

    CONSTRAINT chunks_pkey PRIMARY KEY (id),
    CONSTRAINT chunks_object_fk FOREIGN KEY (object_id)
        REFERENCES objects (id) ON DELETE CASCADE,
    CONSTRAINT chunks_index_unique UNIQUE (object_id, chunk_index),
    CONSTRAINT chunks_index_nonnegative CHECK (chunk_index >= 0),
    CONSTRAINT chunks_size_positive CHECK (size_bytes > 0)
);

CREATE INDEX idx_chunks_object ON chunks (object_id);

CREATE TABLE chunk_replicas (
    id UUID NOT NULL,
    chunk_id UUID NOT NULL,
    node_id VARCHAR(64) NOT NULL,
    priority INTEGER NOT NULL,

    CONSTRAINT chunk_replicas_pkey PRIMARY KEY (id),
    CONSTRAINT chunk_replicas_chunk_fk FOREIGN KEY (chunk_id)
        REFERENCES chunks (id) ON DELETE CASCADE,
    CONSTRAINT chunk_replicas_node_unique UNIQUE (chunk_id, node_id),
    CONSTRAINT chunk_replicas_priority_unique UNIQUE (chunk_id, priority),
    CONSTRAINT chunk_replicas_priority_nonnegative CHECK (priority >= 0)
);

CREATE INDEX idx_chunk_replicas_chunk ON chunk_replicas (chunk_id);
