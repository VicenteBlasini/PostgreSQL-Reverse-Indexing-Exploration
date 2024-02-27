-- Step 1: Create the 'test' table to hold document data
CREATE TABLE test (
    id SERIAL, -- Unique identifier for each document
    doc TEXT, -- The document text
    PRIMARY KEY (id)
);

-- Step 2: Insert sample data into the 'test' table with excerpts from the poem
INSERT INTO test (doc) VALUES
    ('Do not go gentle into that good night'),
    ('Old age should burn and rave at close of day'),
    ('Rage, rage against the dying of the light');

-- Step 3: Create a table 'test_gin' for reverse indexing
CREATE TABLE test_gin (
    keyword TEXT, 
    doc_id INTEGER REFERENCES test(id) ON DELETE CASCADE
);

-- Step 4: Populate the 'test_gin' table with keywords extracted from documents
INSERT INTO test_gin (doc_id, keyword)
SELECT DISTINCT id, s.keyword
FROM test t, UNNEST(string_to_array(LOWER(t.doc), ' ')) s(keyword)
ORDER BY id;

-- Step 5: Look inside the index for a specific keyword
SELECT DISTINCT keyword, doc_id FROM test_gin WHERE keyword = 'rage';

-- Step 6: Check the content of the 'test_gin' table
SELECT * FROM test_gin;

-- Step 7: Perform a join operation to retrieve documents associated with a specific keyword
SELECT DISTINCT keyword, doc_id 
FROM test 
JOIN test_gin ON test.id = test_gin.doc_id
WHERE test_gin.keyword = 'rage';

-- Step 8: Perform a join operation to retrieve documents associated with multiple keywords
SELECT DISTINCT keyword, doc_id
FROM test 
JOIN test_gin ON test.id = test_gin.doc_id
WHERE test_gin.keyword IN ('night', 'light');

-- Step 9: Handle a phrase or a sentence search
SELECT DISTINCT keyword, doc_id
FROM test 
JOIN test_gin ON test.id = test_gin.doc_id
WHERE test_gin.keyword = ANY(string_to_array('Do not go gentle', ' '));

-- Step 10: Create a table 'stop_words' for common words filtering
CREATE TABLE stop_words (
    word TEXT UNIQUE
);

-- Step 11: Populate the 'stop_words' table with common words
INSERT INTO stop_words (word) VALUES ('do'), ('not'), ('into'), ('that'), ('and'), ('the', 'good', 'of', 'at');

-- Step 12: Drop the existing 'test_gin' table
DROP TABLE IF EXISTS test_gin;

-- Step 13: Recreate the 'test_gin' table with stop words filtering
CREATE TABLE test_gin (
    keyword TEXT, 
    doc_id INTEGER REFERENCES test(id) ON DELETE CASCADE
);

-- Step 14: Populate the 'test_gin' table with keywords, excluding stop words
INSERT INTO test_gin (doc_id, keyword)
SELECT DISTINCT id, s.keyword
FROM test t, UNNEST(string_to_array(LOWER(t.doc), ' ')) s(keyword)
WHERE keyword NOT IN (SELECT word FROM stop_words)
ORDER BY id;

-- Step 15: Perform a search operation using a keyword with stop words filtered out
SELECT DISTINCT doc 
FROM test t 
JOIN test_gin tg ON t.id = tg.doc_id
WHERE tg.keyword = LOWER('rage');

-- Step 16: Perform a search operation using multiple keywords with stop words filtered out
SELECT DISTINCT doc 
FROM test t 
JOIN test_gin tg ON t.id = tg.doc_id
WHERE tg.keyword = ANY(string_to_array(LOWER('good night'), ' '));

-- Step 17: Perform a search operation for a stop word
SELECT DISTINCT doc 
FROM test t 
JOIN test_gin tg ON t.id = tg.doc_id
WHERE tg.keyword = LOWER('and');

-- Step 18: Create a table 'test_stem' for word stemming
CREATE TABLE test_stem (
    word TEXT, 
    stem TEXT
);

-- Step 19: Populate the 'test_stem' table with word stems
INSERT INTO test_stem VALUES ('gentle', 'gentl'), ('dying', 'die');

-- Step 20: Join tables to replace keywords with stems
SELECT id, 
       CASE WHEN stem IS NOT NULL THEN stem ELSE keyword END AS awesome,
       keyword,
       stem 
FROM (
    SELECT DISTINCT id, s.keyword AS keyword 
    FROM test AS t, UNNEST(string_to_array(LOWER(t.doc), ' ')) s(keyword)
) K
LEFT JOIN test_stem AS ts ON K.keyword = ts.word;

-- Step 21: Use null coalesce to handle null values and select appropriate keyword or stem
DELETE FROM test_gin;
INSERT INTO test_gin (doc_id, keyword) 
SELECT id, COALESCE(stem, keyword)
FROM (
    SELECT DISTINCT id, s.keyword AS keyword 
    FROM test t, UNNEST(string_to_array(LOWER(t.doc), ' ')) s(keyword)
) K 
LEFT JOIN test_stem ts ON K.keyword = ts.word;

-- Step 22: Create a new table 'test2' for document indexing
CREATE TABLE test2 (
    id SERIAL, 
    doc TEXT, 
    PRIMARY KEY (id)
);

-- Step 23: Insert excerpts from the poem into the 'test2' table
INSERT INTO test2 (doc) VALUES
    ('Do not go gentle into that good night'),
    ('Old age should burn and rave at close of day'),
    ('Rage, rage against the dying of the light');

-- Step 24: Create a GIN index on the 'doc' column of the 'test2' table
CREATE INDEX gin_index_test2 ON test2 USING gin(to_tsvector('english', doc));

-- Step 25: Insert additional rows into the 'test2' table to ensure index usage
INSERT INTO test2 (doc) SELECT 'PRUEBA' || generate_series(10000, 20000);

-- Step 26: Perform a full-text search using to_tsquery and to_tsvector
SELECT id, doc
FROM test2
WHERE to_tsquery('english', 'rage') @@ to_tsvector('english', doc);

-- Step 27: Explain the query plan for the full-text search
EXPLAIN SELECT id, doc
FROM test2
WHERE to_tsquery('english', 'rage') @@ to_tsvector('english', doc);

-- Step 28: Check available operations for each type of index
SELECT am.amname AS index_method, opc.opcname AS opclass_name
FROM pg_am am, pg_opclass opc
WHERE opc.opcmethod = am.oid
ORDER BY index_method, opclass_name;

-- Step 29: Get information about all the indexes
SELECT
    t.tablename,
    indexname,
    c.reltuples AS num_rows,
    pg_size_pretty(pg_relation_size(quote_ident(t.tablename)::text)) AS table_size,
    pg_size_pretty(pg_relation_size(quote_ident(indexrelname)::text)) AS index_size,
    CASE WHEN indisunique THEN 'Y' ELSE 'N' END AS UNIQUE,
    idx_scan AS number_of_scans,
    idx_tup_read AS tuples_read,
    idx_tup_fetch AS tuples_fetched
FROM pg_tables t
LEFT OUTER JOIN pg_class c ON t.tablename = c.relname
LEFT OUTER JOIN (
    SELECT
        c.relname AS ctablename,
        ipg.relname AS indexname,
        x.indnatts AS number_of_columns,
        idx_scan,
        idx_tup_read,
        idx_tup_fetch,
        indexrelname,
        indisunique
    FROM pg_index x
    JOIN pg_class c ON c.oid = x.indrelid
    JOIN pg_class ipg ON ipg.oid = x.indexrelid
    JOIN pg_stat_all_indexes psai ON x.indexrelid = psai.indexrelid
) AS foo ON t.tablename = foo.ctablename
WHERE t.schemaname = 'public'
ORDER BY 1, 2;

-- Step 30: Utilize various text search functions for advanced search operations

-- 1. plainto_tsquery(): Converts plain text into a tsquery suitable for text search.
SELECT plainto_tsquery('english', 'rage against the dying light');

-- 2. phraseto_tsquery(): Converts a phrase into a tsquery suitable for text search.
SELECT phraseto_tsquery('english', 'do not go gentle');

-- 3. websearch_to_tsquery(): Converts a web search query into a tsquery suitable for text search.
--    It's useful for more advanced searches, similar to Google's advanced search.
SELECT websearch_to_tsquery('english', 'rage -not gentle');

-- 4. to_tsvector() and @@: These functions are used together for full-text search.
--    They convert documents into tsvector and perform a match against a tsquery, respectively.
SELECT to_tsquery('english', 'dying') @@
       to_tsvector('english', 'Do not go gentle into that good night');

