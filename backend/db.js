const fs = require('fs-extra')
const path = require('path')
const { MongoClient } = require("mongodb");
const _ = require('lodash');

const config_api = require('./config');
const utils = require('./utils')
const logger = require('./logger');

const Database = require('better-sqlite3');
const { BehaviorSubject } = require('rxjs');

let local_db = null;
let database = null;
exports.database_initialized = false;
exports.database_initialized_bs = new BehaviorSubject(false);

const tables = {
    files: {
        name: 'files',
        primary_key: 'uid',
        text_search: {
            title: 'text',
            uploader: 'text',
            uid: 'text'
        }
    },
    playlists: {
        name: 'playlists',
        primary_key: 'id'
    },
    categories: {
        name: 'categories',
        primary_key: 'uid'
    },
    subscriptions: {
        name: 'subscriptions',
        primary_key: 'id'
    },
    downloads: {
        name: 'downloads'
    },
    users: {
        name: 'users',
        primary_key: 'uid'
    },
    roles: {
        name: 'roles',
        primary_key: 'key'
    },
    download_queue: {
        name: 'download_queue',
        primary_key: 'uid'
    },
    tasks: {
        name: 'tasks',
        primary_key: 'key'
    },
    notifications: {
        name: 'notifications',
        primary_key: 'uid'
    },
    archives: {
        name: 'archives'
    },
    test: {
        name: 'test'
    }
}

const tables_list = Object.keys(tables);

let using_local_db = null; 

function setDB(input_db, input_users_db) {
    db = input_db; users_db = input_users_db;
    exports.db = input_db;
    exports.users_db = input_users_db
}

const createTableSQL = {
    files: `CREATE TABLE IF NOT EXISTS files (uid TEXT PRIMARY KEY, title TEXT, uploader TEXT, body TEXT NOT NULL)`,
    playlists: `CREATE TABLE IF NOT EXISTS playlists (id TEXT PRIMARY KEY, body TEXT NOT NULL)`,
    categories: `CREATE TABLE IF NOT EXISTS categories (uid TEXT PRIMARY KEY, body TEXT NOT NULL)`,
    subscriptions: `CREATE TABLE IF NOT EXISTS subscriptions (id TEXT PRIMARY KEY, body TEXT NOT NULL)`,
    downloads: `CREATE TABLE IF NOT EXISTS downloads (key TEXT PRIMARY KEY, body TEXT NOT NULL)`,
    users: `CREATE TABLE IF NOT EXISTS users (uid TEXT PRIMARY KEY, body TEXT NOT NULL)`,
    roles: `CREATE TABLE IF NOT EXISTS roles (key TEXT PRIMARY KEY, body TEXT NOT NULL)`,
    download_queue: `CREATE TABLE IF NOT EXISTS download_queue (uid TEXT PRIMARY KEY, body TEXT NOT NULL)`,
    tasks: `CREATE TABLE IF NOT EXISTS tasks (key TEXT PRIMARY KEY, body TEXT NOT NULL)`,
    notifications: `CREATE TABLE IF NOT EXISTS notifications (uid TEXT PRIMARY KEY, body TEXT NOT NULL)`,
    archives: `CREATE TABLE IF NOT EXISTS archives (uid TEXT PRIMARY KEY, body TEXT NOT NULL)`,
    test: `CREATE TABLE IF NOT EXISTS test (body TEXT NOT NULL)`,
    migration_meta: `CREATE TABLE IF NOT EXISTS migration_meta (key TEXT PRIMARY KEY, value TEXT)`,
    settings: `CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)`
};

const createIndexSQL = [
    `CREATE INDEX IF NOT EXISTS idx_files_title ON files(title)`,
    `CREATE INDEX IF NOT EXISTS idx_files_uploader ON files(uploader)`
];

function initSQLite(sqlitePath) {
    const db = new Database(sqlitePath);
    db.pragma('journal_mode = WAL');
    db.pragma('synchronous = NORMAL');
    db.pragma('foreign_keys = ON');
    db.pragma('busy_timeout = 5000');

    // Register a regex UDF so $regex filters use real JS RegExp, not LIKE
    db.function('regexp', (pattern, text) => {
        try {
            const re = new RegExp(pattern);
            return re.test(text) ? 1 : 0;
        } catch (e) {
            return 0;
        }
    });

    // Register a Unicode-aware lowercase UDF for case-insensitive matching
    db.function('unicode_lower', (text) => {
        return text ? text.toLocaleLowerCase() : text;
    });

    for (const table of Object.keys(createTableSQL)) {
        db.exec(createTableSQL[table]);
    }
    for (const idxSQL of createIndexSQL) {
        db.exec(idxSQL);
    }
    return db;
}

function extractKeyColumns(table, doc) {
    const tableDef = tables[table];
    const cols = {};
    if (tableDef && tableDef.primary_key) {
        const pk = tableDef.primary_key;
        if (doc[pk] !== undefined) cols[pk] = doc[pk];
    }
    // extract text_search columns
    if (tableDef && tableDef.text_search) {
        for (const col of Object.keys(tableDef.text_search)) {
            if (doc[col] !== undefined) cols[col] = doc[col];
        }
    }
    return cols;
}

function docToObject(row) {
    if (!row) return null;
    const obj = JSON.parse(row.body);
    // restore PK columns that may not be in body
    for (const key of ['uid', 'id', 'key']) {
        if (row[key] !== undefined && obj[key] === undefined) {
            obj[key] = row[key];
        }
    }
    // restore text_search columns
    if (row.title !== undefined && obj.title === undefined) obj.title = row.title;
    if (row.uploader !== undefined && obj.uploader === undefined) obj.uploader = row.uploader;
    return obj;
}

function jsonExtractPath(key) {
    const path = '$.' + key.replace(/\./g, '.');
    return path;
}

// Get the list of real (materialized) columns for a table
function getTableColumns(table) {
    const cols = new Set();
    const tableDef = tables[table];
    if (tableDef && tableDef.primary_key) {
        cols.add(tableDef.primary_key);
    }
    if (tableDef && tableDef.text_search) {
        for (const col of Object.keys(tableDef.text_search)) {
            cols.add(col);
        }
    }
    return cols;
}

function buildWhereClause(filter_obj, table) {
    if (!filter_obj) return { sql: '', params: [] };
    const filter_props = Object.keys(filter_obj);
    if (filter_props.length === 0) return { sql: '', params: [] };

    const realCols = table ? getTableColumns(table) : new Set();
    const clauses = [];
    const params = [];

    for (const filter_prop of filter_props) {
        const filter_prop_value = filter_obj[filter_prop];
        const isDotPath = filter_prop.includes('.');

        // Determine column reference: use real column if it exists, otherwise json_extract from body
        let col;
        if (isDotPath) {
            col = `json_extract(body, '${jsonExtractPath(filter_prop)}')`;
        } else if (realCols.has(filter_prop)) {
            col = filter_prop;
        } else {
            col = `json_extract(body, '${jsonExtractPath(filter_prop)}')`;
        }

        if (filter_prop_value === undefined || filter_prop_value === null) {
            // null/undefined: match missing or null
            clauses.push(`(${col} IS NULL OR json_type(${col}) = 'null')`);
        } else if (typeof filter_prop_value === 'object' && filter_prop_value !== null) {
            if ('$regex' in filter_prop_value) {
                const regex = filter_prop_value['$regex'];
                clauses.push(`regexp(?, ${col}) = 1`);
                params.push(regex);
            } else if ('$ne' in filter_prop_value) {
                const val = filter_prop_value['$ne'];
                if (val === null || val === undefined) {
                    clauses.push(`(${col} IS NOT NULL AND json_type(${col}) != 'null')`);
                } else {
                    clauses.push(`(${col} != ? OR ${col} IS NULL)`);
                    params.push(typeof val === 'object' ? JSON.stringify(val) : val);
                }
            } else if ('$lt' in filter_prop_value) {
                clauses.push(`${col} < ?`);
                params.push(typeof filter_prop_value['$lt'] === 'object' ? JSON.stringify(filter_prop_value['$lt']) : filter_prop_value['$lt']);
            } else if ('$gt' in filter_prop_value) {
                clauses.push(`${col} > ?`);
                params.push(typeof filter_prop_value['$gt'] === 'object' ? JSON.stringify(filter_prop_value['$gt']) : filter_prop_value['$gt']);
            } else if ('$lte' in filter_prop_value) {
                clauses.push(`${col} <= ?`);
                params.push(typeof filter_prop_value['$lte'] === 'object' ? JSON.stringify(filter_prop_value['$lte']) : filter_prop_value['$lte']);
            } else if ('$gte' in filter_prop_value) {
                clauses.push(`${col} >= ?`);
                params.push(typeof filter_prop_value['$gte'] === 'object' ? JSON.stringify(filter_prop_value['$gte']) : filter_prop_value['$gte']);
            } else {
                // Non-operator object: serialize and match against json_extract
                clauses.push(`${col} = ?`);
                params.push(JSON.stringify(filter_prop_value));
            }
        } else {
            clauses.push(`${col} = ?`);
            params.push(filter_prop_value);
        }
    }

    return {
        sql: clauses.length > 0 ? ' WHERE ' + clauses.join(' AND ') : '',
        params
    };
}

function getPrimaryKeyColumn(table) {
    const tableDef = tables[table];
    return tableDef && tableDef.primary_key ? tableDef.primary_key : null;
}

exports.initialize = (input_db, input_users_db, db_name = 'local_db.json') => {
    setDB(input_db, input_users_db);

    // must be done here to prevent getConfigItem from being called before init
    using_local_db = config_api.getConfigItem('ytdl_use_local_db');

    if (using_local_db) {
        // SQLite mode
        const sqlitePath = config_api.getConfigItem('ytdl_sqlite_path') || './appdata/local_db.sqlite';
        
        // Check if SQLite DB exists
        if (fs.existsSync(sqlitePath)) {
            // Already migrated
            local_db = initSQLite(sqlitePath);
            logger.info(`SQLite DB loaded from ${sqlitePath}`);
        } else {
            // Check for legacy JSON
            const legacyDbPath = './appdata/db.json';
            const legacyUsersPath = './appdata/users.json';
            if (fs.existsSync(legacyDbPath) || fs.existsSync(legacyUsersPath)) {
                logger.info('Legacy JSON detected, migrating to SQLite...');
                local_db = initSQLite(sqlitePath);
                
                // Read legacy JSON
                const db_json = fs.existsSync(legacyDbPath) ? fs.readJSONSync(legacyDbPath) : {};
                const users_json = fs.existsSync(legacyUsersPath) ? fs.readJSONSync(legacyUsersPath) : { users: [], roles: {} };
                
                // Backup
                const timestamp = Date.now() / 1000;
                fs.copySync(legacyDbPath, `./appdata/db.json.${timestamp}.bak`);
                if (fs.existsSync(legacyUsersPath)) {
                    fs.copySync(legacyUsersPath, `./appdata/users.json.${timestamp}.bak`);
                }
                
                // Idempotency marker: write BEFORE import so a crash between
                // SQLite init and insert doesn't skip migration and lose data.
                // On restart, if SQLite exists but this marker is missing, we
                // know the previous migration attempt was incomplete.
                local_db.prepare('INSERT OR REPLACE INTO migration_meta (key, value) VALUES (?, ?)').run('sqlite_migration', JSON.stringify({ version: 1, status: 'in_progress', started_at: Date.now() }));
                
                // Migrate: wrap in a transaction so partial imports roll back cleanly.
                const tables_obj = exports.generateJSONTables(db_json, users_json);
                const table_keys = Object.keys(tables_obj);
                const insertTransaction = local_db.transaction(() => {
                    for (const table_key of table_keys) {
                        if (tables_obj[table_key] && tables_obj[table_key].length > 0) {
                            const keyCols = tables[table_key] ? (tables[table_key].primary_key ? [tables[table_key].primary_key] : []) : [];
                            const textCols = tables[table_key] && tables[table_key].text_search ? Object.keys(tables[table_key].text_search) : [];
                            const allKeyCols = [...new Set([...keyCols, ...textCols])];
                            
                            for (const doc of tables_obj[table_key]) {
                                const keyValues = {};
                                for (const k of allKeyCols) {
                                    if (doc[k] !== undefined) keyValues[k] = doc[k];
                                }
                                const cols = ['body', ...Object.keys(keyValues)];
                                const placeholders = cols.map(() => '?').join(',');
                                const values = [JSON.stringify(doc), ...Object.values(keyValues)];
                                local_db.prepare(`INSERT OR IGNORE INTO ${table_key} (${cols.join(',')}) VALUES (${placeholders})`).run(values);
                            }
                        }
                    }
                });
                insertTransaction();
                
                // Mark migration as complete
                local_db.prepare('INSERT OR REPLACE INTO migration_meta (key, value) VALUES (?, ?)').run('sqlite_migration', JSON.stringify({ version: 1, status: 'complete', migrated_at: Date.now() }));
                
                // Rename legacy files
                fs.renameSync(legacyDbPath, `./appdata/db.json.migrated`);
                if (fs.existsSync(legacyUsersPath)) {
                    fs.renameSync(legacyUsersPath, `./appdata/users.json.migrated`);
                }
                logger.info('Migration to SQLite complete!');
            } else {
                // Fresh install
                local_db = initSQLite(sqlitePath);
                logger.info('Fresh SQLite DB created');
            }
        }
    } else {
        // MongoDB mode - no local DB needed; will fall back to SQLite if connection fails
        logger.info('Using MongoDB mode');
    }
}

exports.connectToDB = async (retries = 5, no_fallback = false, custom_connection_string = null) => {
    const success = await exports._connectToDB(custom_connection_string);
    if (success) return true;

    if (retries) {
        logger.warn(`MongoDB connection failed! Retrying ${retries} times...`);
        const retry_delay_ms = 2000;
        for (let i = 0; i < retries; i++) {
            const retry_succeeded = await exports._connectToDB();
            if (retry_succeeded) {
                logger.info(`Successfully connected to DB after ${i+1} attempt(s)`);
                return true;
            }

            if (i !== retries - 1) {
                logger.warn(`Retry ${i+1} failed, waiting ${retry_delay_ms}ms before trying again.`);
                await utils.wait(retry_delay_ms);
            } else {
                logger.warn(`Retry ${i+1} failed.`);
            }
        }
    }
    
    if (no_fallback) {
        logger.error('Failed to connect to MongoDB. Verify your connection string is valid.');
        return;
    }
    using_local_db = true;
    config_api.setConfigItem('ytdl_use_local_db', true);
    const sqlitePath = config_api.getConfigItem('ytdl_sqlite_path') || './appdata/local_db.sqlite';
    local_db = initSQLite(sqlitePath);
    logger.error('Failed to connect to MongoDB, using Local DB (SQLite) as a fallback. Make sure your MongoDB instance is accessible, or set Local DB as a default through the config.');
    return true;
}

exports._connectToDB = async (custom_connection_string = null) => {
    const uri = !custom_connection_string ? config_api.getConfigItem('ytdl_mongodb_connection_string') : custom_connection_string;
    const client = new MongoClient(uri, {
    });

    try {
        await client.connect();
        database = client.db('ytdl_material');

        if (custom_connection_string) return true;

        const existing_collections = (await database.listCollections({}, { nameOnly: true }).toArray()).map(collection => collection.name);

        const missing_tables = tables_list.filter(table => !(existing_collections.includes(table)));
        missing_tables.forEach(async table => {
            await database.createCollection(table);
        });

        tables_list.forEach(async table => {
            const primary_key = tables[table]['primary_key'];
            if (primary_key) {
                await database.collection(table).createIndex({[primary_key]: 1}, { unique: true });
            }
            const text_search = tables[table]['text_search'];
            if (text_search) {
                await database.collection(table).createIndex(text_search);
            }
        });
        using_local_db = false;
        return true;
    } catch(err) {
        logger.error(err);
        return false;
    } finally {
        // Ensures that the client will close when you finish/error
        // await client.close();
    }
}

exports.setVideoProperty = async (file_uid, assignment_obj) => {
    await exports.updateRecord('files', {uid: file_uid}, assignment_obj);
}

exports.getFileDirectoriesAndDBs = async () => {
    let dirs_to_check = [];
    let subscriptions_to_check = [];
    const subscriptions_base_path = config_api.getConfigItem('ytdl_subscriptions_base_path');
    const multi_user_mode = config_api.getConfigItem('ytdl_multi_user_mode');
    const usersFileFolder = config_api.getConfigItem('ytdl_users_base_path');
    const subscriptions_enabled = config_api.getConfigItem('ytdl_allow_subscriptions');
    if (multi_user_mode) {
        const users = await exports.getRecords('users');
        for (let i = 0; i < users.length; i++) {
            const user = users[i];

            dirs_to_check.push({
                basePath: path.join(usersFileFolder, user.uid, 'audio'),
                user_uid: user.uid,
                type: 'audio',
                archive_path: utils.getArchiveFolder('audio', user.uid)
            });

            dirs_to_check.push({
                basePath: path.join(usersFileFolder, user.uid, 'video'),
                user_uid: user.uid,
                type: 'video',
                archive_path: utils.getArchiveFolder('video', user.uid)
            });
        }
    } else {
        const audioFolderPath = config_api.getConfigItem('ytdl_audio_folder_path');
        const videoFolderPath = config_api.getConfigItem('ytdl_video_folder_path');

        dirs_to_check.push({
            basePath: audioFolderPath,
            type: 'audio',
            archive_path: utils.getArchiveFolder('audio')
        });

        dirs_to_check.push({
            basePath: videoFolderPath,
            type: 'video',
            archive_path: utils.getArchiveFolder('video')
        });
    }

    if (subscriptions_enabled) {
        const subscriptions = await exports.getRecords('subscriptions');
        subscriptions_to_check = subscriptions_to_check.concat(subscriptions);
    }

    for (let i = 0; i < subscriptions_to_check.length; i++) {
        let subscription_to_check = subscriptions_to_check[i];
        if (!subscription_to_check.name) {
            continue;
        }
        dirs_to_check.push({
            basePath: subscription_to_check.user_uid ? path.join(usersFileFolder, subscription_to_check.user_uid, 'subscriptions', subscription_to_check.isPlaylist ? 'playlists/' : 'channels/', subscription_to_check.name)
                                      : path.join(subscriptions_base_path, subscription_to_check.isPlaylist ? 'playlists/' : 'channels/', subscription_to_check.name),
            user_uid: subscription_to_check.user_uid,
            type: subscription_to_check.type,
            sub_id: subscription_to_check['id'],
            archive_path: utils.getArchiveFolder(subscription_to_check.type, subscription_to_check.user_uid, subscription_to_check)
        });
    }

    return dirs_to_check;
}

// Basic DB functions

// Create

exports.insertRecordIntoTable = async (table, doc, replaceFilter = null) => {
    if (using_local_db) {
        try {
            const pk = getPrimaryKeyColumn(table);
            const keyCols = extractKeyColumns(table, doc);
            
            if (replaceFilter && pk && doc[pk] !== undefined) {
                const existing = exports.getRecord(table, replaceFilter);
                if (existing) {
                    exports.removeRecord(table, replaceFilter);
                }
            } else if (replaceFilter) {
                // If no PK but have filter, remove matching
                exports.removeRecord(table, replaceFilter);
            }

            const cols = ['body', ...Object.keys(keyCols)];
            const placeholders = cols.map(() => '?').join(',');
            const values = [JSON.stringify(doc), ...Object.values(keyCols)];
            
            const sql = `INSERT INTO ${table} (${cols.join(',')}) VALUES (${placeholders})`;
            local_db.prepare(sql).run(values);
            return true;
        } catch (err) {
            logger.error(`insertRecordIntoTable error: ${err.message}`);
            return false;
        }
    }

    if (replaceFilter) {
        const output = await database.collection(table).bulkWrite([
            {
                deleteMany: {
                    filter: replaceFilter
                }
            },
            {
                insertOne: {
                    document: doc
                }
            }
        ]);
        logger.debug(`Inserted doc into ${table} with filter: ${JSON.stringify(replaceFilter)}`);
        return !!(output['result']['ok']);
    }

    const output = await database.collection(table).insertOne(doc);
    logger.debug(`Inserted doc into ${table}`);
    return !!(output['result']['ok']);
}

exports.insertRecordsIntoTable = async (table, docs, ignore_errors = false) => {
    if (using_local_db) {
        try {
            const pk = getPrimaryKeyColumn(table);
            const insert = local_db.transaction(() => {
                for (const doc of docs) {
                    const keyCols = extractKeyColumns(table, doc);
                    const cols = ['body', ...Object.keys(keyCols)];
                    const placeholders = cols.map(() => '?').join(',');
                    const values = [JSON.stringify(doc), ...Object.values(keyCols)];
                    
                    const sql = `INSERT INTO ${table} (${cols.join(',')}) VALUES (${placeholders})`;
                    try {
                        local_db.prepare(sql).run(values);
                    } catch (err) {
                        if (!ignore_errors) throw err;
                        // skip on ignore_errors
                    }
                }
            });
            insert();
            return true;
        } catch (err) {
            logger.error(`insertRecordsIntoTable error: ${err.message}`);
            return false;
        }
    }
    const output = await database.collection(table).insertMany(docs, {ordered: !ignore_errors});
    logger.debug(`Inserted ${output.insertedCount} docs into ${table}`);
    return !!(output['result']['ok']);
}

exports.bulkInsertRecordsIntoTable = async (table, docs) => {
    return await exports.insertRecordsIntoTable(table, docs, false);
}

// Read

exports.getRecord = async (table, filter_obj) => {
    if (using_local_db) {
        const { sql: whereSql, params } = buildWhereClause(filter_obj, table);
        const sql = `SELECT * FROM ${table}${whereSql} LIMIT 1`;
        const row = local_db.prepare(sql).get(params);
        return docToObject(row);
    }

    return await database.collection(table).findOne(filter_obj);
}

exports.getRecords = async (table, filter_obj = null, return_count = false, sort = null, range = null) => {
    if (using_local_db) {
        const { sql: whereSql, params } = buildWhereClause(filter_obj, table);
        let sql = `SELECT * FROM ${table}${whereSql}`;
        if (sort) {
            const order = sort['order'] === 1 ? 'ASC' : 'DESC';
            sql += ` ORDER BY ${sort['by']} ${order}`;
        }
        if (range) {
            sql += ` LIMIT ${range[1] - range[0]} OFFSET ${range[0]}`;
        }
        const rows = local_db.prepare(sql).all(params);
        if (return_count) return rows.length;
        return rows.map(docToObject);
    }

    const cursor = filter_obj ? database.collection(table).find(filter_obj) : database.collection(table).find();
    if (sort) {
        cursor.sort({[sort['by']]: sort['order']});
    }
    if (range) {
        cursor.skip(range[0]).limit(range[1] - range[0]);
    }

    return !return_count ? await cursor.toArray() : await cursor.count();
}

// Update

exports.updateRecord = async (table, filter_obj, update_obj, nested_mode = false) => {
    if (using_local_db) {
        try {
            let actualUpdate = update_obj;
            if (nested_mode) {
                actualUpdate = utils.convertFlatObjectToNestedObject(update_obj);
            }

            const records = await exports.getRecords(table, filter_obj);
            if (records.length === 0) return false;

            const pk = getPrimaryKeyColumn(table);
            
            for (const record of records) {
                const merged = { ...record, ...actualUpdate };
                delete merged['_id'];

                const keyCols = extractKeyColumns(table, merged);
                
                // Build SET clause for key columns + body
                const setCols = ['body = ?'];
                const setParams = [JSON.stringify(merged)];
                for (const [k, v] of Object.entries(keyCols)) {
                    setCols.push(`${k} = ?`);
                    setParams.push(v);
                }
                
                if (pk) {
                    setParams.push(record[pk]);
                    const sql = `UPDATE ${table} SET ${setCols.join(', ')} WHERE ${pk} = ?`;
                    local_db.prepare(sql).run(setParams);
                } else {
                    // No PK: use WHERE from filter
                    const { sql: whereSql, params: whereParams } = buildWhereClause(filter_obj, table);
                    const sql = `UPDATE ${table} SET ${setCols.join(', ')}${whereSql}`;
                    local_db.prepare(sql).run([...setParams, ...whereParams]);
                }
            }
            return true;
        } catch (err) {
            logger.error(`updateRecord error: ${err.message}`);
            return false;
        }
    }

    // sometimes _id will be in the update obj, this breaks mongodb
    if (update_obj['_id']) delete update_obj['_id'];
    const output = await database.collection(table).updateOne(filter_obj, {$set: update_obj});
    return !!(output['result']['ok']);
}

exports.updateRecords = async (table, filter_obj, update_obj) => {
    if (using_local_db) {
        try {
            const records = await exports.getRecords(table, filter_obj);
            const pk = getPrimaryKeyColumn(table);
            const props_to_update = Object.keys(update_obj);

            for (const record of records) {
                for (const prop of props_to_update) {
                    record[prop] = update_obj[prop];
                }
                const keyCols = extractKeyColumns(table, record);
                const setCols = ['body = ?'];
                const setParams = [JSON.stringify(record)];
                for (const [k, v] of Object.entries(keyCols)) {
                    setCols.push(`${k} = ?`);
                    setParams.push(v);
                }
                if (pk) {
                    setParams.push(record[pk]);
                    local_db.prepare(`UPDATE ${table} SET ${setCols.join(', ')} WHERE ${pk} = ?`).run(setParams);
                } else {
                    const { sql: whereSql, params: whereParams } = buildWhereClause(filter_obj, table);
                    local_db.prepare(`UPDATE ${table} SET ${setCols.join(', ')}${whereSql}`).run([...setParams, ...whereParams]);
                }
            }
            return true;
        } catch (err) {
            logger.error(`updateRecords error: ${err.message}`);
            return false;
        }
    }

    const output = await database.collection(table).updateMany(filter_obj, {$set: update_obj});
    return !!(output['result']['ok']);
}

exports.removePropertyFromRecord = async (table, filter_obj, remove_obj) => {
    if (using_local_db) {
        try {
            const props_to_remove = Object.keys(remove_obj);
            const records = await exports.getRecords(table, filter_obj);
            const pk = getPrimaryKeyColumn(table);

            for (const record of records) {
                for (const prop of props_to_remove) {
                    delete record[prop];
                }
                const keyCols = extractKeyColumns(table, record);
                const setCols = ['body = ?'];
                const setParams = [JSON.stringify(record)];
                for (const [k, v] of Object.entries(keyCols)) {
                    setCols.push(`${k} = ?`);
                    setParams.push(v);
                }
                if (pk) {
                    setParams.push(record[pk]);
                    local_db.prepare(`UPDATE ${table} SET ${setCols.join(', ')} WHERE ${pk} = ?`).run(setParams);
                } else {
                    const { sql: whereSql, params: whereParams } = buildWhereClause(filter_obj, table);
                    local_db.prepare(`UPDATE ${table} SET ${setCols.join(', ')}${whereSql}`).run([...setParams, ...whereParams]);
                }
            }
            return true;
        } catch (err) {
            logger.error(`removePropertyFromRecord error: ${err.message}`);
            return false;
        }
    }

    const output = await database.collection(table).updateOne(filter_obj, {$unset: remove_obj});
    return !!(output['result']['ok']);
}

exports.bulkUpdateRecordsByKey = async (table, key_label, update_obj) => {
    if (using_local_db) {
        try {
            const item_ids_to_update = Object.keys(update_obj);
            const records = await exports.getRecords(table);
            
            for (const record of records) {
                const item_id_to_update = record[key_label];
                if (!item_id_to_update || !update_obj[item_id_to_update]) continue;

                const props_to_update = Object.keys(update_obj[item_id_to_update]);
                for (const prop of props_to_update) {
                    record[prop] = update_obj[item_id_to_update][prop];
                }

                const keyCols = extractKeyColumns(table, record);
                const setCols = ['body = ?'];
                const setParams = [JSON.stringify(record)];
                for (const [k, v] of Object.entries(keyCols)) {
                    setCols.push(`${k} = ?`);
                    setParams.push(v);
                }
                const pk = getPrimaryKeyColumn(table);
                if (pk) {
                    setParams.push(record[pk]);
                    local_db.prepare(`UPDATE ${table} SET ${setCols.join(', ')} WHERE ${pk} = ?`).run(setParams);
                } else {
                    local_db.prepare(`UPDATE ${table} SET ${setCols.join(', ')} WHERE ${key_label} = ?`).run(setParams.concat([item_id_to_update]));
                }
            }
            return true;
        } catch (err) {
            logger.error(`bulkUpdateRecordsByKey error: ${err.message}`);
            return false;
        }
    }

    const table_collection = database.collection(table);
        
    let bulk = table_collection.initializeOrderedBulkOp();

    const item_ids_to_update = Object.keys(update_obj);

    for (let i = 0; i < item_ids_to_update.length; i++) {
        const item_id_to_update = item_ids_to_update[i];
        bulk.find({[key_label]: item_id_to_update }).updateOne({
            "$set": update_obj[item_id_to_update]
        });
    }

    const output = await bulk.execute();
    return !!(output['result']['ok']);
}

exports.pushToRecordsArray = async (table, filter_obj, key, value) => {
    if (using_local_db) {
        try {
            const records = await exports.getRecords(table, filter_obj);
            const pk = getPrimaryKeyColumn(table);
            for (const record of records) {
                if (!Array.isArray(record[key])) record[key] = [];
                record[key].push(value);

                const keyCols = extractKeyColumns(table, record);
                const setCols = ['body = ?'];
                const setParams = [JSON.stringify(record)];
                for (const [k, v] of Object.entries(keyCols)) {
                    setCols.push(`${k} = ?`);
                    setParams.push(v);
                }
                if (pk) {
                    setParams.push(record[pk]);
                    local_db.prepare(`UPDATE ${table} SET ${setCols.join(', ')} WHERE ${pk} = ?`).run(setParams);
                } else {
                    const { sql: whereSql, params: whereParams } = buildWhereClause(filter_obj, table);
                    local_db.prepare(`UPDATE ${table} SET ${setCols.join(', ')}${whereSql}`).run([...setParams, ...whereParams]);
                }
            }
            return true;
        } catch (err) {
            logger.error(`pushToRecordsArray error: ${err.message}`);
            return false;
        }
    }

    const output = await database.collection(table).updateOne(filter_obj, {$push: {[key]: value}});
    return !!(output['result']['ok']);
}

exports.pullFromRecordsArray = async (table, filter_obj, key, value) => {
    if (using_local_db) {
        try {
            const records = await exports.getRecords(table, filter_obj);
            const pk = getPrimaryKeyColumn(table);
            for (const record of records) {
                if (!Array.isArray(record[key])) record[key] = [];
                record[key] = record[key].filter(item => item !== value);

                const keyCols = extractKeyColumns(table, record);
                const setCols = ['body = ?'];
                const setParams = [JSON.stringify(record)];
                for (const [k, v] of Object.entries(keyCols)) {
                    setCols.push(`${k} = ?`);
                    setParams.push(v);
                }
                if (pk) {
                    setParams.push(record[pk]);
                    local_db.prepare(`UPDATE ${table} SET ${setCols.join(', ')} WHERE ${pk} = ?`).run(setParams);
                } else {
                    const { sql: whereSql, params: whereParams } = buildWhereClause(filter_obj, table);
                    local_db.prepare(`UPDATE ${table} SET ${setCols.join(', ')}${whereSql}`).run([...setParams, ...whereParams]);
                }
            }
            return true;
        } catch (err) {
            logger.error(`pullFromRecordsArray error: ${err.message}`);
            return false;
        }
    }

    const output = await database.collection(table).updateOne(filter_obj, {$pull: {[key]: value}});
    return !!(output['result']['ok']);
}

// Delete

exports.removeRecord = async (table, filter_obj) => {
    if (using_local_db) {
        try {
            const { sql: whereSql, params } = buildWhereClause(filter_obj, table);
            local_db.prepare(`DELETE FROM ${table}${whereSql}`).run(params);
            return true;
        } catch (err) {
            logger.error(`removeRecord error: ${err.message}`);
            return false;
        }
    }

    const output = await database.collection(table).deleteOne(filter_obj);
    return !!(output['result']['ok']);
}

exports.removeAllRecords = async (table = null, filter_obj = null) => {
    const tables_to_remove = table ? [table] : tables_list;
    logger.debug(`Removing all records from: ${tables_to_remove} with filter: ${JSON.stringify(filter_obj)}`)
    if (using_local_db) {
        try {
            for (const table_to_remove of tables_to_remove) {
                if (filter_obj) {
                    const { sql: whereSql, params } = buildWhereClause(filter_obj, table_to_remove);
                    local_db.prepare(`DELETE FROM ${table_to_remove}${whereSql}`).run(params);
                } else {
                    local_db.prepare(`DELETE FROM ${table_to_remove}`).run();
                }
                logger.debug(`Successfully removed records from ${table_to_remove}`);
            }
            return true;
        } catch (err) {
            logger.error(`removeAllRecords error: ${err.message}`);
            return false;
        }
    }

    let success = true;
    for (let i = 0; i < tables_to_remove.length; i++) {
        const table_to_remove = tables_to_remove[i];

        const output = await database.collection(table_to_remove).deleteMany(filter_obj ? filter_obj : {});
        logger.debug(`Successfully removed records from ${table_to_remove}`);
        success &= !!(output['result']['ok']);
    }
    return success;
}

// Query

exports.findDuplicatesByKey = async (table, key) => {
    let duplicates = [];
    if (using_local_db) {
        const all_records = await exports.getRecords(table);
        const existing_records = {};
        for (let i = 0; i < all_records.length; i++) {
            const record = all_records[i];
            const value = record[key];

            if (existing_records[value]) {
                duplicates.push(record);
            }

            existing_records[value] = true;
        }
        return duplicates;
    }
    
    const duplicated_values = await database.collection(table).aggregate([
        {"$group" : { "_id": `$${key}`, "count": { "$sum": 1 } } },
        {"$match": {"_id" :{ "$ne" : null } , "count" : {"$gt": 1} } }, 
        {"$project": {[key] : "$_id", "_id" : 0} }
    ]).toArray();

    for (let i = 0; i < duplicated_values.length; i++) {
        const duplicated_value = duplicated_values[i];
        const duplicated_records = await exports.getRecords(table, duplicated_value, false);
        if (duplicated_records.length > 1) {
            duplicates = duplicates.concat(duplicated_records.slice(1, duplicated_records.length));
        }
    }
    return duplicates;
}

// Stats

exports.getDBStats = async () => {
    const stats_by_table = {};
    for (let i = 0; i < tables_list.length; i++) {
        const table = tables_list[i];
        if (table === 'test') continue;

        stats_by_table[table] = await getDBTableStats(table);
    }
    return {stats_by_table: stats_by_table, using_local_db: using_local_db};
}

const getDBTableStats = async (table) => {
    const table_stats = {};
    if (using_local_db) {
        const row = local_db.prepare(`SELECT COUNT(*) as cnt FROM ${table}`).get();
        table_stats['records_count'] = row ? row.cnt : 0;
    } else {
        const stats = await database.collection(table).stats();
        table_stats['records_count'] = stats.count;
    }
    return table_stats;
}

// JSON to DB

exports.generateJSONTables = async (db_json, users_json) => {
    // create records
    let files = db_json['files'] || [];
    let playlists = db_json['playlists'] || [];
    let categories = db_json['categories'] || [];
    let subscriptions = db_json['subscriptions'] || [];

    const users = users_json['users'];

    for (let i = 0; i < users.length; i++) {
        const user = users[i];

        if (user['files']) {
            user['files'] = user['files'].map(file => ({ ...file, user_uid: user['uid'] }));
            files = files.concat(user['files']);
        }
        if (user['playlists']) {
            user['playlists'] = user['playlists'].map(playlist => ({ ...playlist, user_uid: user['uid'] }));
            playlists = playlists.concat(user['playlists']);
        }
        if (user['categories']) {
            user['categories'] = user['categories'].map(category => ({ ...category, user_uid: user['uid'] }));
            categories = categories.concat(user['categories']);
        }

        if (user['subscriptions']) {
            user['subscriptions'] = user['subscriptions'].map(subscription => ({ ...subscription, user_uid: user['uid'] }));
            subscriptions = subscriptions.concat(user['subscriptions']);
        }
    }

    const tables_obj = {};
    
    tables_obj.files = createFilesRecords(files, subscriptions);
    tables_obj.playlists = playlists;
    tables_obj.categories = categories;
    tables_obj.subscriptions = createSubscriptionsRecords(subscriptions);
    tables_obj.users = createUsersRecords(users);
    tables_obj.roles = createRolesRecords(users_json['roles']);
    tables_obj.downloads = createDownloadsRecords(db_json['downloads'])
    
    return tables_obj;
}

exports.importJSONToDB = async (db_json, users_json) => {
    await fs.writeFile(`appdata/db.json.${Date.now()/1000}.bak`, JSON.stringify(db_json, null, 2));
    await fs.writeFile(`appdata/users_db.json.${Date.now()/1000}.bak`, JSON.stringify(users_json, null, 2));

    await exports.removeAllRecords();
    const tables_obj = await exports.generateJSONTables(db_json, users_json);

    const table_keys = Object.keys(tables_obj);
    
    let success = true;
    for (let i = 0; i < table_keys.length; i++) {
        const table_key = table_keys[i];
        if (!tables_obj[table_key] || tables_obj[table_key].length === 0) continue;
        success &= await exports.insertRecordsIntoTable(table_key, tables_obj[table_key], true);
    }

    return success;
}

const createFilesRecords = (files, subscriptions) => {
    for (let i = 0; i < subscriptions.length; i++) {
        const subscription = subscriptions[i];
        if (!subscription['videos']) continue;
        subscription['videos'] = subscription['videos'].map(file => ({ ...file, sub_id: subscription['id'], user_uid: subscription['user_uid'] ? subscription['user_uid'] : undefined}));
        files = files.concat(subscription['videos']);
    }

    return files;
}

const createPlaylistsRecords = async (playlists) => {

}

const createCategoriesRecords = async (categories) => {

}

const createSubscriptionsRecords = (subscriptions) => {
    for (let i = 0; i < subscriptions.length; i++) {
        delete subscriptions[i]['videos'];
    }

    return subscriptions;
}

const createUsersRecords = (users) => {
    users.forEach(user => {
        delete user['files'];
        delete user['playlists'];
        delete user['subscriptions'];
    });
    return users;
}

const createRolesRecords = (roles) => {
    const new_roles = [];
    Object.keys(roles).forEach(role_key => {
        new_roles.push({
            key: role_key,
            ...roles[role_key]
        });
    });
    return new_roles;
}

const createDownloadsRecords = (downloads) => {
    const new_downloads = [];
    Object.keys(downloads).forEach(session_key => {
        new_downloads.push({
            key: session_key,
            ...downloads[session_key]
        });
    });
    return new_downloads;
}

exports.backupDB = async () => {
    const backup_dir = path.join('appdata', 'db_backup');
    fs.ensureDirSync(backup_dir);
    const backup_file_name = `${using_local_db ? 'local' : 'remote'}_db.json.${Date.now()/1000}.bak`;
    const path_to_backups = path.join(backup_dir, backup_file_name);

    logger.info(`Backing up ${using_local_db ? 'local' : 'remote'} DB to ${path_to_backups}`);

    const table_to_records = {};
    for (let i = 0; i < tables_list.length; i++) {
        const table = tables_list[i];
        table_to_records[table] = await exports.getRecords(table);
    }

    fs.writeJsonSync(path_to_backups, table_to_records);

    return backup_file_name;
}

exports.restoreDB = async (file_name) => {
    const path_to_backup = path.join('appdata', 'db_backup', file_name);

    logger.debug('Reading database backup file.');
    const table_to_records = fs.readJSONSync(path_to_backup);

    if (!table_to_records) {
        logger.error(`Failed to restore DB! Backup file '${path_to_backup}' could not be read.`);
        return false;
    }

    logger.debug('Clearing database.');
    await exports.removeAllRecords();

    logger.debug('Database cleared! Beginning restore.');
    let success = true;
    for (let i = 0; i < tables_list.length; i++) {
        const table = tables_list[i];
        if (!table_to_records[table] || table_to_records[table].length === 0) continue;
        success &= await exports.bulkInsertRecordsIntoTable(table, table_to_records[table]);
    }

    logger.debug('Restore finished!');

    return success;
}

exports.transferDB = async (local_to_remote) => {
    const table_to_records = {};
    for (let i = 0; i < tables_list.length; i++) {
        const table = tables_list[i];
        table_to_records[table] = await exports.getRecords(table);
    }

    logger.info('Backup up DB...');
    await exports.backupDB(); // should backup always

    using_local_db = !local_to_remote;
    if (local_to_remote) {
        const db_connected = await exports.connectToDB(5, true);
        if (!db_connected) {
            logger.error('Failed to transfer database - could not connect to MongoDB. Verify that your connection URL is valid.');
            return false;
        }
    }
    success = true;

    logger.debug('Clearing new database before transfer...');

    await exports.removeAllRecords();

    logger.debug('Database cleared! Beginning transfer.');

    for (let i = 0; i < tables_list.length; i++) {
        const table = tables_list[i];
        if (!table_to_records[table] || table_to_records[table].length === 0) continue;
        success &= await exports.bulkInsertRecordsIntoTable(table, table_to_records[table]);
    }

    config_api.setConfigItem('ytdl_use_local_db', using_local_db);

    logger.debug('Transfer finished!');

    return success;
}

/*
    This function emulates MongoDB's ability to search for null or missing values,
    regex, comparison operators, and nested dot-path properties.
    Keep for backward compatibility with direct callers (tests use it).
*/
exports.applyFilterLocalDB = (db_path, filter_obj, operation) => {
    // Support being called with an array directly (test compatibility)
    if (Array.isArray(db_path)) {
        return applyFilterToCollection(db_path, filter_obj, operation);
    }
    // Otherwise treat as lowdb chain
    return applyFilterToCollection(db_path.value(), filter_obj, operation);
}

function applyFilterToCollection(collection, filter_obj, operation) {
    const filter_props = Object.keys(filter_obj);
    const filtered = collection.filter(record => {
        if (!filter_props) return true;
        let filtered = true;
        for (let i = 0; i < filter_props.length; i++) {
            const filter_prop = filter_props[i];
            const filter_prop_value = filter_obj[filter_prop];
            if (filter_prop_value === undefined || filter_prop_value === null) {
                filtered &= record[filter_prop] === undefined || record[filter_prop] === null;
            } else {
                if (typeof filter_prop_value === 'object') {
                    if ('$regex' in filter_prop_value) {
                        filtered &= (record[filter_prop] && record[filter_prop].search(new RegExp(filter_prop_value['$regex'], filter_prop_value['$options'])) !== -1);
                    } else if ('$ne' in filter_prop_value) {
                        filtered &= filter_prop in record && record[filter_prop] !== filter_prop_value['$ne'];
                    } else if ('$lt' in filter_prop_value) {
                        filtered &= filter_prop in record && record[filter_prop] < filter_prop_value['$lt'];
                    } else if ('$gt' in filter_prop_value) {
                        filtered &= filter_prop in record && record[filter_prop] > filter_prop_value['$gt'];
                    } else if ('$lte' in filter_prop_value) {
                        filtered &= filter_prop in record && record[filter_prop] <= filter_prop_value['$lt'];
                    } else if ('$gte' in filter_prop_value) {
                        filtered &= filter_prop in record && record[filter_prop] >= filter_prop_value['$gt'];
                    }
                } else {
                    // handle case of nested property check
                    if (filter_prop.includes('.'))
                        filtered &= utils.searchObjectByString(record, filter_prop) === filter_prop_value;
                    else
                        filtered &= record[filter_prop] === filter_prop_value;
                }
            }
        }
        return filtered;
    });

    if (operation === 'find') return filtered.length > 0 ? filtered[0] : null;
    if (operation === 'remove') return filtered.length;
    return filtered;
}

// should only be used for tests
exports.setLocalDBMode = (mode) => {
    using_local_db = mode;
}

// Settings KV store (for auth.js compatibility)

exports.getSetting = async (key) => {
    if (using_local_db && local_db) {
        const row = local_db.prepare('SELECT value FROM settings WHERE key = ?').get(key);
        return row ? JSON.parse(row.value) : null;
    }
    return null;
};

exports.setSetting = async (key, value) => {
    if (using_local_db && local_db) {
        local_db.prepare('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)').run(key, JSON.stringify(value));
    }
};
