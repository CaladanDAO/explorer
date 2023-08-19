// Copyright 2023 Caladan DAO
// This file is part of the CaladanDAO Block Explorer.
    
const ini = require('node-ini');

const {
    Bigtable
} = require("@google-cloud/bigtable");
const {
    BigQuery
} = require('@google-cloud/bigquery');
const {
    Storage
} = require('@google-cloud/storage');
const stream = require("stream");
const paraTool = require("./paraTool.js");
const mysql = require("mysql2");
const bunyan = require('bunyan');
const util = require('util');
const exec = util.promisify(require('child_process').exec);
const path = require('path');
const fs = require('fs');
const os = require("os");
const {
    BloomFilter
} = require('bloom-filters');

// Imports the Google Cloud client library for Bunyan
const {
    LoggingBunyan
} = require('@google-cloud/logging-bunyan');

module.exports = class CaladanDB {
    finished = util.promisify(stream.finished);
    exitOnDisconnect = true;
    isDisconneted = false;
    isConneted = false;
    disconnectedCnt = 0;
    isErrored = false;
    // general purpose sql batches
    // Creates a Bunyan Cloud Logging client
    logger = false;
    hostname = false;
    batchedSQL = [];
    initChainInfos = false;
    commitHash = 'NA';
    specVersions = {};
    chainInfos = {};
    chainNames = {};
    paraIDs = [];
    paras = {};
    pool = false;
    defaultBQLocation = "US";

    numIndexingWarns = 0;
    numIndexingErrors = 0;
    reloadChainInfo = false; // if set to true after system properties brings in a new asset, we get one chance to do so.
    lastBatchTS = 0;
    connection = false;
    APIWSEndpoint = null;

    GC_PROJECT = "";
    GC_BIGTABLE_INSTANCE = "";
    GC_BIGTABLE_CLUSTER = "";

    GC_STORAGE_BUCKET = "";

    POLKAHOLIC_EMAIL_USER = "";
    POLKAHOLIC_EMAIL_PASSWORD = "";

    EXTERNAL_WS_PROVIDER_URL = null;
    EXTERNAL_WS_PROVIDER_KEY = null;
    WSProviderQueue = {};

    BQ_SUBSTRATEETL_KEY = null;
    BQ_KEY = null;
    BNE_KEY = "AIzaSyBuFuqcrn81RjVNcFZfiHJ1HLnPZEFo9jQ" // TODO: bring this from the db.cnf
    bigQuery = null;
    googleStorage = null;
    bqTablesCallsEvents = null;

    showMemoryUsage(ctx = "") {
        const used = process.memoryUsage().heapUsed / 1024 / 1024;
        console.log(`${ctx} USED: [${used}MB]`)
    }

    constructor(serviceName = "polkaholic") {

        // Creates a Bunyan Cloud Logging client
        const loggingBunyan = new LoggingBunyan();
        this.logger = bunyan.createLogger({
            // The JSON payload of the log as it appears in Cloud Logging
            // will contain "name": "my-service"
            name: serviceName,
            streams: [
                // Log to the console at 'debug' and above
                {
                    stream: process.stdout,
                    level: 'debug'
                },
                // And log to Cloud Logging, logging at 'info' and above
                loggingBunyan.stream('debug'),
            ],
        });

        this.hostname = os.hostname();
        this.commitHash = paraTool.commitHash()
        this.version = `1.0.0` // we will update this manually
        this.indexerInfo = `${this.version}-${this.commitHash.slice(0,7)}`
        console.log(`****  Initiating Polkaholic ${this.indexerInfo} ****`)

        // 1. ready db config for WRITABLE mysql pool [always in US presently] using env variable POLKAHOLIC_DB
        let dbconfigFilename = (process.env.POLKAHOLIC_DB != undefined) ? process.env.POLKAHOLIC_DB : '/root/.mysql/.db00.cnf';
        try {
            let dbconfig = ini.parseSync(dbconfigFilename);
            if (dbconfig.email != undefined) {
                this.POLKAHOLIC_EMAIL_USER = dbconfig.email.email;
                this.POLKAHOLIC_EMAIL_PASSWORD = dbconfig.email.password;
            }
            if (dbconfig.gc != undefined) {
                this.GC_PROJECT = dbconfig.gc.projectName;
                this.GC_BIGTABLE_INSTANCE = dbconfig.gc.bigtableInstance;
                this.GC_BIGTABLE_CLUSTER = dbconfig.gc.bigtableCluster;
                this.GC_STORAGE_BUCKET = dbconfig.gc.storageBucket;
            }

            if (dbconfig.ws != undefined) {
                this.EXTERNAL_WS_PROVIDER_KEY = dbconfig.ws.key;
                this.EXTERNAL_WS_PROVIDER_URL = dbconfig.ws.url;
            }

            if (dbconfig.bq != undefined) {
                if (dbconfig.bq.substrateetlKey) {
                    this.BQ_KEY = dbconfig.bq.substrateetlKey;
                }
            }

            if (dbconfig.externalapis != undefined) {
                if (dbconfig.externalapis) {
                    this.EXTERNAL_APIKEYS = dbconfig.externalapis;
                }
            }

            this.pool = mysql.createPool(this.convert_dbconfig(dbconfig.client));
            // Ping WRITABLE database to check for common exception errors.
            this.pool.getConnection((err, connection) => {
                if (err) {
                    if (err.code === 'PROTOCOL_CONNECTION_LOST') {
                        console.error('Database connection was closed.')
                    }
                    if (err.code === 'ER_CON_COUNT_ERROR') {
                        console.error('Database has too many connections.')
                    }
                    if (err.code === 'ECONNREFUSED') {
                        console.error('Database connection was refused.')
                    }
                }

                if (connection) {
                    this.connection = connection;
                    connection.release()
                }
                return
            })
            // Promisify for Node.js async/await.
            this.pool.query = util.promisify(this.pool.query).bind(this.pool)
            this.pool.end = util.promisify(this.pool.end).bind(this.pool)
        } catch (err) {
            console.log(err);
            this.logger.fatal({
                "op": "dbconfig",
                err
            });
        }

        // 2. ready db config for READONLY mysql pool [default US replica but could be EU/AS replica] using env variable POLKAHOLIC_DB_REPLICA
        let dbconfigReplicaFilename = (process.env.POLKAHOLIC_DB_REPLICA != undefined) ? process.env.POLKAHOLIC_DB_REPLICA : '/root/.mysql/.db00-us-indexer.cnf';
        try {
            let dbconfigREADONLY = ini.parseSync(dbconfigReplicaFilename);
            this.poolREADONLY = mysql.createPool(this.convert_dbconfig(dbconfigREADONLY.client));
            // Ping READONLY database to check for common exception errors.
            this.poolREADONLY.getConnection((err, connection) => {
                if (err) {
                    if (err.code === 'PROTOCOL_CONNECTION_LOST') {
                        console.error('Database connection was closed.')
                    }
                    if (err.code === 'ER_CON_COUNT_ERROR') {
                        console.error('Database has too many connections.')
                    }
                    if (err.code === 'ECONNREFUSED') {
                        console.error('Database connection was refused.')
                    }
                }

                if (connection) {
                    this.connectionREADONLY = connection;
                    connection.release()
                }
                return
            })
            // Promisify for Node.js async/await.
            this.poolREADONLY.query = util.promisify(this.poolREADONLY.query).bind(this.poolREADONLY)
            this.poolREADONLY.end = util.promisify(this.poolREADONLY.end).bind(this.poolREADONLY)
        } catch (err) {
            console.log(err);
            this.logger.fatal({
                "op": "dbconfigReplica",
                err
            });
        }
        const bigtable = new Bigtable({
            projectId: this.GC_PROJECT
        });
        const instanceName = this.GC_BIGTABLE_INSTANCE;

        const tableAccountRealtime = "accountrealtime";
        const tableHashes = "hashes";
        const tableAddress = "address";

        this.instance = bigtable.instance(instanceName);
        this.btHashes = this.instance.table(tableHashes);
        this.btAccountRealtime = this.instance.table(tableAccountRealtime);
    }

    convert_dbconfig(c) {
        return {
            host: c.host,
            user: c.user,
            password: c.password,
            database: c.database,
            charset: c['default-character-set']
        };
    }

    get_btTableRealtime() {

        return ["accountrealtime", this.btAccountRealtime];
    }

    async release(msDelay = 1000) {
        await this.pool.end();
        if (this.connection) {
            await this.connection.destroy();
            this.connection = false;
        }
        await this.sleep(msDelay);
    }

    cacheInit() {
        const Memcached = require("memcached-promise");
        this.memcached = new Memcached("127.0.0.1:11211", {
            maxExpiration: 2592000,
            namespace: "polkaholic",
            debug: false
        });
    }

    async cacheWrite(key, val) {
        if (!this.memcached) this.cacheInit();
        await this.memcached.set(key, val);
    }

    async cacheRead(key) {
        if (!this.memcached) this.cacheInit();
        return this.memcached.get(key)
    }

    async sleep(ms) {
        return new Promise((resolve) => {
            setTimeout(resolve, ms);
        });
    }

    async getBlockRangebyTS(chainID, startTS, endTS) {
        let sql = `select UNIX_TIMESTAMP(min(blockDT)) startTS, UNIX_TIMESTAMP(max(blockDT)) endTS, min(blockNumber) startBN, max(blockNumber) endBN from block${chainID} where blockDT >= from_unixtime(${startTS}) and blockDT < from_unixtime(${endTS});`
        //if (this.debugLevel >= paraTool.debugTracing) console.log(`getBlockRangebyTS`, sql)
        var res = await this.poolREADONLY.query(sql);
        if (res.length > 0) {
            let r = res[0]
            r.rangeLen = 1 + r.endBN - r.startBN
            r.chainID = chainID
            return r
        } else {
            return false
        }
    }

    getChainDecimal(chainID) {
        if (chainID == undefined || chainID === null || chainID === false) {
            console.log("FAILED getChainDecimal", chainID);
            return false;
        }

        chainID = chainID.toString()
        if (this.chainInfos[chainID] != undefined) {
            return this.chainInfos[chainID].decimal
        } else {
            console.log("getChainDecimal FATAL ERROR: must call init", chainID)
        }
    }

    getChainSymbol(chainID) {
        if (typeof chainID !== "string") chainID = chainID.toString()
        if (chainID == undefined || chainID === null || chainID === false) {
            console.log("FAILED getChainSymbol", chainID);
            return false;
        }
        if (chainID == 1) {
            return "ETH";
        }

        if (this.chainInfos[chainID] != undefined) {
            return this.chainInfos[chainID].symbol
        } else {
            console.log("getChainSymbol FATAL ERROR: must call init", chainID)
        }
    }

    getChainAsset(chainID) {
        chainID = chainID.toString()
        let assetChain = null
        if (this.chainInfos[chainID] != undefined && this.chainInfos[chainID].symbol != undefined) {
            let asset = JSON.stringify({
                "Token": this.chainInfos[chainID].symbol
            })
            assetChain = paraTool.makeAssetChain(asset, chainID);
            if (this.assetInfo[assetChain] != undefined) {
                return (asset);
            }
        }
        console.log(`[${chainID}] getChainAsset FATAL ERROR: must call init for ${assetChain}`, this.chainInfos[chainID])
        return null
    }

    getChainName(chainID) {
        chainID = chainID.toString()
        if (this.chainInfos[chainID] != undefined) {
            return this.chainInfos[chainID].name
        } else {
            console.log("getChainName FATAL ERROR: must call init", chainID)
            let relay = paraTool.getRelayChainByChainID(chainID)
            let paraID = paraTool.getParaIDfromChainID(chainID)
            let name = `${relay}[paraID:${paraID}]`
            return name
        }
    }

    getChainEVMStatus(chainID) {
        chainID = chainID.toString()
        if (this.chainInfos[chainID] != undefined) {
            return this.chainInfos[chainID].isEVM
        } else {
            console.log("getChainEVMStatus FATAL ERROR: must call init", chainID)
        }
    }

    getChainFullInfo(chainID) {
        chainID = chainID.toString()
        let chainInfo = this.chainInfos[chainID]
        if (chainInfo != undefined) {
            let r = {
                chainID: paraTool.dechexToInt(chainID),
                chainName: chainInfo.name,
                asset: chainInfo.asset,
                symbol: chainInfo.symbol,
                ss58Format: chainInfo.ss58Format,
                evmChainID: chainInfo.evmChainID,
                decimals: chainInfo.decimal,
                priceUSD: chainInfo.priceUSD,
                priceUSDPercentChange: paraTool.round(chainInfo.priceUSDPercentChange, 2),
                relayChain: chainInfo.relayChain,
            }
            //console.log("getChainFullInfo", chainInfo, r)
            return r
        } else {
            return {
                chainID: paraTool.dechexToInt(chainID),
                chainName: `chain${chainID}`,
                symbol: "NA",
                decimals: 12,
                priceUSD: 0,
                priceUSDPercentChange: 0,
                relayChain: "NA",
            }
        }
    }

    getNameByChainID(chainID) {
        if (this.chainInfos[chainID] != undefined) {
            // [chainID, id]
            let cID = parseInt(chainID, 10)
            return [cID, this.chainInfos[chainID].id]
        }
        return [false, false]
    }

    getIDByChainID(chainID) {
        if (this.chainInfos[chainID] != undefined) {
            // [chainID, id]
            let cID = parseInt(chainID, 10)
            return this.chainInfos[chainID].id
        }
        return false
    }

    getChainIDByName(id) {
        if (this.chainNames[id] != undefined) {
            // [chainID, id]
            return [this.chainNames[id].chainID, id]
        }
        return [false, false]
    }

    currentTS() {
        return Math.floor(new Date().getTime() / 1000);
    }

    async numConnections() {
        var sql = 'SELECT COUNT(*) nconn FROM information_schema.PROCESSLIST';
        var q = await this.pool.query(sql);
        if (q.length > 0) {
            let numConn = q[0].nconn;
            if (numConn > 1000) console.log("WARNING: numConnections: ", numConn, "SIZE", this.batchedSQL.length);
            if (numConn > 3000) {
                console.log("TERMINATING Too many connections", numConn);
                process.exit(1);
            }
            return (numConn);
        }
    }

    async update_batchedSQL(sqlMax = 1.50) {
        if (this.batchedSQL.length == 0) return;
        var currentTS = this.currentTS();

        this.lastBatchTS = currentTS;
        let retrySQL = [];
        for (var i = 0; i < this.batchedSQL.length; i++) {
            let sql = this.batchedSQL[i];
            try {
                let sqlStartTS = new Date().getTime();
                await this.pool.query(sql);
                let sqlTS = (new Date().getTime() - sqlStartTS) / 1000;
                if (sqlTS > sqlMax) {
                    this.logger.info({
                        "op": "SLOWSQL",
                        "sql": (sql.length > 4096) ? sql.substring(0, 4096) : sql,
                        "len": sql.length,
                        "sqlTS": sqlTS
                    });
                }
            } catch (err) {
                if (err.toString().includes("Deadlock found")) {
                    retrySQL.push(sql);
                } else {
                    this.logger.error({
                        "op": "update_batchedSQL",
                        "sql": (sql.length > 4096) ? sql.substring(0, 4096) : sql,
                        "len": sql.length,
                        "try": 1,
                        err
                    });
                    this.numIndexingErrors++;
                    console.log("polkaholicDB numIndexingErrors ERROR", err);
                    let tsm = new Date().getTime();
                    let fn = "/var/log/" + tsm + "-" + i + ".sql";
                    await fs.writeFileSync(fn, sql);
                }
            }
        }
        this.batchedSQL = [];
        if (retrySQL.length > 0) {
            for (var i = 0; i < retrySQL.length; i++) {
                let sql = retrySQL[i];
                try {
                    await this.pool.query(sql);
                } catch (err) {
                    if (err.toString().includes("Deadlock found")) {
                        this.batchedSQL.push(sql);
                    } else {
                        this.logger.error({
                            "op": "update_batchedSQL RETRY",
                            "sql": (sql.length > 4096) ? sql.substring(0, 4096) : sql,
                            "len": sql.length,
                            "try": 2,
                            err
                        });
                        this.numIndexingErrors++;
                        console.log("*polkaholicDB numIndexingErrors ERROR", err);
                        let tsm = new Date().getTime()
                        let fn = "/var/log/" + tsm + "-" + i + ".sql";
                        await fs.writeFileSync(fn, sql);
                    }
                }
            }
        }
    }


    async upsertSQL(flds, debug = false, sqlMax = 1.50) {
        let tbl = flds.table;
        let keys = flds.keys;
        let vals = flds.vals;
        let data = flds.data;

        if (tbl == undefined || typeof tbl !== "string") return (false);
        if (keys == undefined || !Array.isArray(keys)) return (false);
        if (vals == undefined || !Array.isArray(vals)) return (false);
        if (data == undefined || !Array.isArray(data)) return (false);
        if (data.length == 0) return (false);

        let out = [];
        if (flds.replace !== undefined) {
            let farr = flds.replace;
            for (let i = 0; i < farr.length; i++) {
                let f = farr[i];
                out.push(`${f}=VALUES(${f})`);
            }
        }
        if (flds.replaceIfNull !== undefined) {
            let farr = flds.replaceIfNull;
            for (let i = 0; i < farr.length; i++) {
                let f = farr[i];
                out.push(`${f}=IF(${f} is null, VALUES(${f}), ${f})`);
            }
        }
        if (flds.lastUpdateBN !== undefined) {
            let farr = flds.lastUpdateBN;
            for (let i = 0; i < farr.length; i++) {
                let f = farr[i];
                out.push(`${f}=IF( lastUpdateBN <= values(lastUpdateBN), VALUES(${f}), ${f})`)
            }
        }
        let keysvals = keys.concat(vals);
        let fldstr = keysvals.join(",")
        let sql = `insert into ${tbl} (${fldstr}) VALUES ` + data.join(",");
        if (out.length > 0) {
            sql = sql + " on duplicate key update " + out.join(",")
        }
        this.batchedSQL.push(sql);
        if (debug) {
            console.log(sql);
        }
        await this.update_batchedSQL(sqlMax);
    }


    async getChains(crawling = 1, orderBy = "numAccountsActive DESC") {
        let chains = await this.poolREADONLY.query(`select id, chainID, chainName, blocksCovered, symbol, lastCrawlDT,  unix_timestamp(lastCrawlDT) as lastCrawlTS,
 iconUrl, numTransactionsEVM, numTransactionsEVM7d, numTransactionsEVM30d, numAccountsActive, numAccountsActive7d, numAccountsActive30d, relayChain, UNIX_TIMESTAMP(lastUpdateBlockLogDT) lastUpdateBlockLogTS, lastUpdateBlockLogDT, crawlingStatus from chain where crawling = ${crawling} order by ${orderBy}`);
        return (chains);
    }

    async get_chains_external(crawling = 1) {
        return await this.getChains();
    }

    async getChain(chainID_or_chainName) {
        try {
            let [chainID, id] = this.convertChainID(chainID_or_chainName)
            let sql = `select id, chainID, chainName, blocksCovered, crawlingStatus, etherscanAPIURL,
numTransactionsEVM, numTransactionsEVM7d, numTransactionsEVM30d,
floor(gasUsed / (numEVMBlocks+1)) as gasUsed,floor(gasUsed7d / (numEVMBlocks7d+1)) as gasUsed7d,floor(gasUsed30d / (numEVMBlocks30d+1)) as gasUsed30d,
floor(gasLimit / (numEVMBlocks+1)) as gasLimit,floor(gasLimit7d / (numEVMBlocks7d+1)) as gasLimit7d,floor(gasLimit30d / (numEVMBlocks30d+1)) as gasLimit30d
from chain where chainID = '${chainID}' limit 1`
            var chains = await this.poolREADONLY.query(sql);
            if (chains.length == 0) return (false);
            let chain = chains[0];
            return chain;
        } catch (err) {
            console.log(err);
        }
        return null;
    }

    countTopicLen(text_signature_full) {
        // The split method splits the string into an array of substrings around each instance of '*'
        // So the length of the array will be one more than the number of '*' characters in the string
        // We subtract 1 to get the count of '*' characters
        return text_signature_full.split('*').length;
    }
    async getContractABI() {
        let contractabis = await this.poolREADONLY.query(`select hex_signature, CONVERT(text_signature using utf8) text_signature, CONVERT(text_signature_full using utf8) text_signature_full, CONVERT(abiMaybe using utf8) abiMaybe, CONVERT(abiWeb3 using utf8) abiWeb3, CONVERT(addresses using utf8) addresses from signaturescurated`);
        let abis = {}
        let curatedABIs = {}
        try {
            for (const abi of contractabis) {
                let abiType = (abi.hex_signature.length == 10) ? "function" : "event";
                let hex_signature = abi.hex_signature;
                let signature = abi.text_signature
                let signature_full = abi.text_signature_full
                let jsonABI = JSON.parse(abi.abiMaybe);

                let jsonABIWeb3 = abi.abiWeb3 ? JSON.parse(abi.abiWeb3) : null;
                let topicLen = (abi.hex_signature.length == 10) ? "0" : this.countTopicLen(signature_full);
                let lookupID = (abiType == 'function') ? `${hex_signature}` : `${hex_signature}-${topicLen}`
                let r = {
                    abi: jsonABI,
                    abiWeb3: jsonABIWeb3,
                    abiType: abiType,
                    signature: signature,
                    signature_full: signature_full,
                    topic_len: topicLen
                }
                if (abis[lookupID] == undefined) {
                    abis[lookupID] = r
                }
                abis[lookupID].addresses = abi.addresses
                if (jsonABIWeb3 != undefined && signature_full != undefined) {
                    if (curatedABIs[signature_full] == undefined) {
                        curatedABIs[signature_full] = r
                    }
                    //curatedABIs[lookupID].addresses = abi.addresses
                    curatedABIs[signature_full].lookupID = lookupID
                }
            }
        } catch (err) {
            console.log(err)
        }
        return [abis, curatedABIs];
    }

    async getAddressProject(chainID = null, project = null) {
        let w = chainID ? `and chainID = '${chainID}'` : ""
        if (project) w += `and project = '${project}'`
        let recs = await this.poolREADONLY.query(`select address, chainID, contractType, project from abirepo where project is not null ${w}`);
        let address_project = {}
        try {
            for (const r of recs) {
                let p = {
                    project: r.project
                };
                if (r.contractType) p.contractType = r.contractType;
                address_project[r.address.toLowerCase()] = p;
            }
        } catch (err) {
            console.log(err)
        }
        return address_project;
    }


    async setupAPI(chain, backfill = false) {
        this.chainID = chain.chainID;
        this.chainName = chain.chainName;
        [this.contractABIs, this.curatedABIs] = await this.getContractABI();
    }

    getExitOnDisconnect() {
        let exitOnDisconnect = this.exitOnDisconnect
        return exitOnDisconnect
    }

    setExitOnDisconnect(exitOnDisconnect) {
        this.exitOnDisconnect = exitOnDisconnect
        //console.log(`*** setting ExitOnDisconnect=${this.exitOnDisconnect}`)
    }

    getDisconnectedCnt() {
        let disconnectedCnt = this.disconnectedCnt
        return disconnectedCnt
    }

    getDisconnected() {
        let disconnected = this.isDisconneted
        return disconnected
    }

    getConnected() {
        let connected = this.isConneted
        return connected
    }

    setConnected() {
        //successful connection will overwrite its previous error state
        this.isConneted = true
        this.isDisconneted = false
        this.isErrored = false
        this.disconnectedCnt = 0
    }

    setDisconnected() {
        this.isConneted = false
        this.isDisconneted = true
        this.disconnectedCnt += 1
    }

    getErrored() {
        let errored = this.isErrored
        return errored
    }

    setErrored() {
        this.isConneted = false
        this.isErrored = true
        this.disconnectedCnt += 1
    }

    get_google_storage() {
        if (this.googleStorage) return this.googleStorage;
        this.googleStorage = new Storage({
            projectId: 'awesome-web3',
            keyFilename: this.BQ_KEY
        })
        return this.googleStorage;
    }

    get_big_query() {
        if (this.bigQuery) return this.bigQuery;
        this.bigQuery = new BigQuery({
            projectId: 'awesome-web3',
            keyFilename: this.BQ_KEY
        })
        return this.bigQuery;
    }

    async execute_bqLoad(cmd) {
        try {
            //console.log(cmd);
            let res = await exec(cmd);
            console.log(res)
            return true
        } catch (err) {
            console.log(err);
            return false
        }
    }

    async execute_bqJob(sqlQuery, targetBQLocation = null) {
        // run bigquery job with suitable credentials
        const bigqueryClient = this.get_big_query();
        const options = {
            query: sqlQuery,
            location: (targetBQLocation) ? targetBQLocation : this.defaultBQLocation,
        };

        try {
            const response = await bigqueryClient.createQueryJob(options);
            const job = response[0];
            const [rows] = await job.getQueryResults();
            return rows;
        } catch (err) {
            console.log(err);
            throw new Error(`An error has occurred.`, sqlQuery);
        }
        return [];
    }

    async load_calls_events(datasetId = "substrate") {
        const bigquery = this.get_big_query()
        let sql = `select table_name, column_name, data_type, ordinal_position, is_nullable from awesome-web3.${datasetId}.INFORMATION_SCHEMA.COLUMNS where ( table_name like 'call_%' or table_name like 'evt_%' ) and column_name not in ("block_time", "relay_chain", "para_id", "extrinsic_id", "extrinsic_hash", "call_id", "signer_ss58", "signer_pub_key") limit 1000000`;
        let columns = await this.execute_bqJob(sql);
        this.bqTablesCallsEvents = {}
        for (const c of columns) {
            let sa = c.table_name.split("_");
            if (this.bqTablesCallsEvents[c.table_name] == undefined) {
                this.bqTablesCallsEvents[c.table_name] = {
                    columns: {},
                    description: null
                };
            }
            if (c.column_name == "chain_id") {} else {
                this.bqTablesCallsEvents[c.table_name].columns[c.column_name] = c;
            }
        }
    }

    async insertBTRows(tbl, rows, tableName = "") {
        if (rows.length == 0) return (true);
        try {
            await tbl.insert(rows);
            return (true);
        } catch (err) {
            let succ = true;
            console.log(err);
            for (let a = 0; a < rows.length; a++) {
                try {
                    let r = rows[a];
                    if (r.key !== undefined && r.key) {
                        await tbl.insert([r]);
                    }
                } catch (err) {
                    let tries = 0;
                    while (tries < 10) {
                        try {
                            tries++;
                            await tbl.insert([rows[a]]);
                            await this.sleep(100);
                        } catch (err) {
                            //console.log(err);
                        }
                    }
                    if (tries >= 10) {
                        succ = false;
                    }
                }
            }
            return (succ);
        }
    }

    sortArrayByField(array, field, order = 'asc') {
        return array.sort((a, b) => {
            if (order === 'asc') {
                return a[field] - b[field];
            } else if (order === 'desc') {
                return b[field] - a[field];
            } else {
                throw new Error("Invalid sorting order. Use 'asc' or 'desc'.");
            }
        });
    }

    // [blocks, contracts, logs, token_transfers, traces, transactions]
    build_evm_block_from_row(row) {
        let rowData = row.data;
        let r = {
            prefix: false,
            chain_id: false,
            block: false,
            blockHash: false,
            blockNumber: false,
            blockTS: false,
            contracts: false,
            logs: false,
            token_transfers: false,
            traces: false,
            transactions: false,
        }
        let evmColF = Object.keys(rowData)
        //console.log(`evmColF`, evmColF)
        r.blockNumber = parseInt(row.id.substr(2), 16);
        r.prefix = row.id
        //console.log(`${row.id} rowData`, rowData)
        if (rowData["blocks"]) {
            let columnFamily = rowData["blocks"];
            let blkhashes = Object.keys(columnFamily)
            //console.log(`blkhashes`, blkhashes)
            //TODO: remove the incorret finalizedhash here
            if (blkhashes.length == 1) {
                r.blockHash = blkhashes[0];
            } else {
                //use the correct finalized hash by checking and see if we can find the proper blockrow
                console.log(`ERROR: multiple evm blkhashes found ${blkhashes} @ blockNumber:`, r.blockNumber, `cbt read chain prefix=${row.id}`)
                this.logger.error({
                    "op": "build_evm_block_from_row",
                    "err": `multiple evm blkhashes found ${blkhashes}`
                });
                return false
            }
            //console.log(`rowData["blocks"]`, rowData["blocks"])
            let cell = (rowData["blocks"][r.blockHash]) ? rowData["blocks"][r.blockHash][0] : false;
            if (cell) {
                let blk = JSON.parse(cell.value)
                r.block = blk;
                r.blockTS = blk.timestamp
                r.chain_id = blk.chain_id
            }
        }
        if (rowData["logs"]) {
            let cell = (r.blockHash && rowData["logs"][r.blockHash]) ? rowData["logs"][r.blockHash][0] : false;
            if (cell) {
                r.logs = JSON.parse(cell.value);
            }
        }
        if (rowData["traces"]) {
            let cell = (r.blockHash && rowData["traces"][r.blockHash]) ? rowData["traces"][r.blockHash][0] : false;
            if (cell) {
                r.traces = JSON.parse(cell.value);
            }
        }
        if (rowData["transactions"]) {
            let cell = (r.blockHash && rowData["transactions"][r.blockHash]) ? rowData["transactions"][r.blockHash][0] : false;
            if (cell) {
                r.transactions = JSON.parse(cell.value);
            }
        }
        if (rowData["token_transfers"]) {
            let cell = (r.blockHash && rowData["token_transfers"][r.blockHash]) ? rowData["token_transfers"][r.blockHash][0] : false;
            if (cell) {
                r.token_transfers = JSON.parse(cell.value);
            }
        }
        if (rowData["contracts"]) {
            let cell = (r.blockHash && rowData["contracts"][r.blockHash]) ? rowData["contracts"][r.blockHash][0] : false;
            if (cell) {
                r.contracts = JSON.parse(cell.value);
            }
        }
        // want RPC style of blockWithTransaction, ReceiptsWithLogs and trace
        let btBlkCols = ["timestamp", "number", "hash", "parent_hash", "nonce", "sha3_uncles", "logs_bloom", "transactions_root", "state_root", "receipts_root", "miner", "difficulty", "total_difficulty", "size", 'gas_limit', "gas_used"]
        let rpcBlkCols = ["timestamp", "number", "hash", "parentHash", "nonce", "sha3Uncles", "logsBloom", "transactionsRoot", "stateRoot", "receipts_root", "miner", "difficulty", "totalDifficulty", "size", "gas_limit", "gas_used"]
        let rpcBlk = {}
        // convert header
        for (let i = 0; i < btBlkCols.length; i++) {
            let btBlkCol = btBlkCols[i]
            let rpcBlkCol = rpcBlkCols[i]
            let btBlk = r.block
            rpcBlk[rpcBlkCol] = btBlk[btBlkCol]
        }

        let btTxCols = ["block_hash", "block_number", "from_address", "gas", "gas_price", "max_fee_per_gas", "max_priority_fee_per_gas", "hash", "input", "nonce", "to_address", "transaction_index", "value", "transaction_type", "chain_id"]
        let rpcTxCols = ["blockHash", "blockNumber", "from", "gas", "gasPrice", "maxFeePerGas", "maxPriorityFeePerGas", "hash", "input", "nonce", "to", "transactionIndex", "value", "type", "chainId"] //no accessList, v, r, s

        // include transaction
        let rpcTxns = []
        let rpcReceipts = [] //receipt is a combinations of txn result + logs.. need to extract from both obj

        let btReceiptCols = ["block_hash", "block_number", "receipt_contract_address", "receipt_cumulative_gas_used", "receipt_effective_gas_price", "from_address", "receipt_gas_used", "logs", "logsBloom", "receipt_status", "to_address", "hash", "transaction_index", "transaction_type"] //no bloom
        let rpcReceiptCols = ["blockHash", "blockNumber", "contractAddress", "cumulativeGasUsed", "effectiveGasPrice", "from", "gasUsed", "logs", "logsBloom", "status", "to", "transactionHash", "transactionIndex", "type"]

        let btLogCols = ["address", "topics", "data", "block_number", "transaction_hash", "transaction_index", "block_hash", "log_index", "removed"] //no removed
        let rpcLogCols = ["address", "topics", "data", "blockNumber", "transactionHash", "transactionIndex", "blockHash", "logIndex", "removed"]

        this.sortArrayByField(r.transactions, 'transaction_index', 'asc');
        this.sortArrayByField(r.logs, 'log_index', 'asc');

        let logMap = {} //hash -> []
        for (let i = 0; i < r.logs.length; i++) {
            let rpcLog = {}
            let btLog = r.logs[i]
            let transactionHash = btLog.transaction_hash
            if (logMap[transactionHash] == undefined) logMap[transactionHash] = []
            //console.log(`btLog`, btLog)
            // convert receipt header
            for (let i = 0; i < btLogCols.length; i++) {
                let btLogCol = btLogCols[i]
                let rpcLogCol = rpcLogCols[i]
                if (rpcLogCol == "removed") {
                    rpcLog["removed"] = false
                    //It is true when the log was removed due to a chain reorganization, and false if it's a valid log. since we don't have this. will use false for all
                } else {
                    rpcLog[rpcLogCol] = (btLog[btLogCol] != undefined) ? btLog[btLogCol] : null
                }
            }
            logMap[transactionHash].push(rpcLog)
        }
        //console.log(`logMap`, logMap)

        for (let i = 0; i < r.transactions.length; i++) {
            let btTxn = r.transactions[i]
            let transactionHash = btTxn.hash
            //console.log(`btTxn`, btTxn)
            let rpcTxn = {}
            let rpcReceipt = {}
            // convert transaction
            for (let i = 0; i < btTxCols.length; i++) {
                let btTxCol = btTxCols[i]
                let rpcTxCol = rpcTxCols[i]
                rpcTxn[rpcTxCol] = (btTxn[btTxCol] != undefined) ? btTxn[btTxCol] : null
            }
            //btTxn is missing accessList, r, s, v
            rpcTxns.push(rpcTxn)

            // convert receipt header
            for (let i = 0; i < btReceiptCols.length; i++) {
                let btReceiptCol = btReceiptCols[i]
                let rpcReceiptCol = rpcReceiptCols[i]
                if (rpcReceiptCol == "logsBloom") {
                    rpcReceipt["logsBloom"] = null
                } else if (rpcReceiptCol == "logs") {
                    if (logMap[transactionHash] == undefined) {
                        rpcReceipt["logs"] = [] // TODO build logs using logs
                    } else {
                        //console.log(`${transactionHash} logs`, logMap[transactionHash])
                        rpcReceipt["logs"] = logMap[transactionHash]
                    }
                } else {
                    rpcReceipt[rpcReceiptCol] = (btTxn[btReceiptCol] != undefined) ? btTxn[btReceiptCol] : null
                }
            }
            // rpcReceipts is missing logsbloom + removed
            rpcReceipts.push(rpcReceipt)
        }
        //will have to sort for messy ordering
        //rpcBlk.transactions = this.sortArrayByField(rpcTxns, 'transactionIndex', 'asc');
        rpcBlk.transactions = rpcTxns
        //console.log(`rpcBlk`, rpcBlk)
        //console.log(`rpcReceipts`, rpcReceipts)
        //TODO: traces...
        let f = {
            prefix: r.prefix,
            chain_id: r.chain_id,
            blockHash: r.blockHash,
            blockNumber: r.blockNumber,
            blockTS: paraTool.dechexToInt(r.blockTS),
            block: rpcBlk,
            transactions: rpcTxns,
            evmReceipts: rpcReceipts,
            traces: false
        }
        //process.exit(0)
        return f
    }

    validate_evm_row(row) {
        let rRow = this.build_evm_block_from_row(row)
        if (!rRow.block) {
            return [false, false]
        }
        let knownTxType = [0, 2]
        let rpcTxns = rRow.transactions
        for (const rpcTxn of rpcTxns) {
            if (!knownTxType.includes(rpcTxn["type"])) {
                console.log(`Missing txType`)
                return [false, false]
            }
        }
        if (Array.isArray(rpcTxns) && rpcTxns.length > 0) {
            if (!rRow.evmReceipts) {
                console.log(`Missing evmReceipts`)
                return [false, false]
            }
        }
        //TODO: check trace
        return [true, rRow]
    }

    build_block_from_row(row) {
        let rowData = row.data;
        let r = {
            block: false,
            blockHash: false,
            blockNumber: false,
            events: false,
            trace: false,
            autotrace: false,
            evmBlock: false,
            finalized: false,
            traceType: false,
            blockStats: false,
            feed: false
        }
        r.blockNumber = parseInt(row.id.substr(2), 16);
        // 0. get the finalized blockHash; however, any "raw" column is also finalized
        //console.log(`returned families`, Object.keys(rowData))
        if (rowData["finalized"]) {
            let columnFamily = rowData["finalized"];
            let blkhashes = Object.keys(columnFamily)

            //TODO: remove the incorret finalizedhash here
            if (blkhashes.length == 1) {
                r.blockHash = blkhashes[0];
                r.finalized = true;
            } else {
                r.fork = r.blockNumber;
                //use the correct finalized hash by checking and see if we can find the proper blockrow
                console.log(`ERROR: multiple finalized blkhashes found ${blkhashes} @ blockNumber:`, r.blockNumber, `cbt read chain prefix=${row.id}`)
                this.logger.error({
                    "op": "build_block_from_row",
                    "err": `multiple finalized blkhashes found ${blkhashes}`
                });
            }
        }

        // 1. store extrinsics of the block in the address in feed
        if (rowData["blockraw"]) {
            let cell = (r.blockHash && rowData["blockraw"][r.blockHash]) ? rowData["blockraw"][r.blockHash][0] : false;
            let cellEvents = (r.blockHash && rowData["events"] && rowData["events"][r.blockHash]) ? rowData["events"][r.blockHash][0] : false;
            if (cell) {
                r.block = JSON.parse(cell.value);
                if (cellEvents) r.events = JSON.parse(cellEvents.value);
                r.blockHash = r.block.hash;
            } else {
                console.log("no finalized block", r.blockNumber);
            }
        }

        // 2. process deduped traceBlock
        if (rowData["trace"]) {
            let cell = (rowData["trace"]["raw"]) ? rowData["trace"]["raw"][0] : (r.blockHash && rowData["trace"][r.blockHash]) ? rowData["trace"][r.blockHash][0] : false;
            if (cell) {
                r.trace = JSON.parse(cell.value);
                let cellTS = cell.timestamp / 1000000;
                if (rowData["n"] && rowData["n"]["traceType"]) {
                    let traceTypeCell = rowData["n"]["traceType"][0];
                    switch (traceTypeCell.value) {
                        case "subscribeStorage":
                        case "state_traceBlock":
                            r.traceType = traceTypeCell.value;
                    }
                }
            }
        }
        if (rowData["autotrace"]) {
            let cell = (r.blockHash && rowData["autotrace"][r.blockHash]) ? rowData["autotrace"][r.blockHash][0] : false;
            if (cell) {
                r.autotrace = JSON.parse(cell.value);
            }
        }

        // 3. return feed
        if (rowData["feed"]) {
            let cell = (r.blockHash && rowData["feed"][r.blockHash]) ? rowData["feed"][r.blockHash][0] : false;
            if (cell) {
                r.feed = JSON.parse(cell.value);
            }
        }

        // 4. return blockrawevm
        if (rowData["blockrawevm"]) {
            let cell = (r.blockHash && rowData["blockrawevm"][r.blockHash]) ? rowData["blockrawevm"][r.blockHash][0] : false;
            if (cell) {
                r.evmBlock = JSON.parse(cell.value);
            }
        }

        // 5. return receiptsevm
        if (rowData["receiptsevm"]) {
            let cell = (r.blockHash && rowData["receiptsevm"][r.blockHash]) ? rowData["receiptsevm"][r.blockHash][0] : false;
            if (cell) {
                r.evmReceipts = JSON.parse(cell.value);
            }
        }

        // 5b. return traceevm
        if (rowData["traceevm"]) {
            let cell = (r.blockHash && rowData["traceevm"][r.blockHash]) ? rowData["traceevm"][r.blockHash][0] : false;
            if (cell) {
                r.evmTrace = JSON.parse(cell.value);
            }
        }

        // 6. return feedevm (evmFullBlock)
        if (rowData["feedevm"]) {
            let cell = (r.blockHash && rowData["feedevm"][r.blockHash]) ? rowData["feedevm"][r.blockHash][0] : false;
            if (cell) {
                r.evmFullBlock = JSON.parse(cell.value);
            }
        }

        return r;
    }

    build_feed_from_row(row, requestedBlockHash = false) {
        let rowData = row.data;
        let r = {
            blockHash: false,
            blockNumber: false,
            evmFullBlock: false,
            finalized: false,
            feed: false
        }
        r.blockNumber = parseInt(row.id.substr(2), 16);
        // 0. get the finalized blockHash; however, any "raw" column is also finalized
        //console.log(`returned families`, Object.keys(rowData))
        if (rowData["finalized"]) {
            let columnFamily = rowData["finalized"];
            for (const h of Object.keys(columnFamily)) {
                r.blockHash = h;
                r.finalized = true;
                // TODO: check that its unique
            }
        } else {
            let blockHashes = Object.keys(rowData["feed"])
            //console.log(`${r.blockNumber} hashes`, blockHashes)
            if (requestedBlockHash && rowData["feed"][requestedBlockHash]) {
                r.blockHash = requestedBlockHash;
            } else if (blockHashes.length > 0) {
                r.blockHash = blockHashes[0];
            }
        }

        // 1. return feed
        if (rowData["feed"]) {
            let cell = (r.blockHash && rowData["feed"][r.blockHash]) ? rowData["feed"][r.blockHash][0] : false;
            if (cell) {
                r.feed = JSON.parse(cell.value);
            }
        }

        // 2. return feedevm (evmFullBlock)
        if (rowData["feedevm"]) {
            let cell = (r.blockHash && rowData["feedevm"][r.blockHash]) ? rowData["feedevm"][r.blockHash][0] : false;
            if (cell) {
                r.evmFullBlock = JSON.parse(cell.value);
            }
        }
        return r;
    }

    gs_evm_file_name(chainID, logDT, blockNumber) {
        return `${chainID}/${logDT.replaceAll("-","/")}/${blockNumber}.json.gz`
    }

    async fetch_evm_block_gs(chainID, blockNumber) {
        let sql = `select UNIX_TIMESTAMP(blockDT) blockTS from block${chainID} where blockNumber = '${blockNumber}' limit 1`
        let blocks = await this.poolREADONLY.query(sql);
        if (blocks.length == 1) {
            let b = blocks[0];
            let [logDT0, hr] = paraTool.ts_to_logDT_hr(b.blockTS);
            const storage = new Storage();
            const bucketName = 'caladan_evm';
            const bucket = storage.bucket(bucketName);
            const fileName = this.gs_evm_file_name(chainID, logDT0, blockNumber);
            const file = bucket.file(fileName);
            const buffer = await file.download();
            const r = JSON.parse(buffer[0]); // block, receipts, evm
            return r
        }
        return null;
    }



    async fetch_block_gs(chainID, blockNumber) {
        try {
            return this.fetch_evm_block_gs(chainID, blockNumber);
        } catch (e) {
            console.log(e)
            return null;
        }
    }


    async fetch_block(chainID, blockNumber) {
        try {
            let r = await this.fetch_block_gs(chainID, blockNumber);
            console.log("fetch_block", r);
            return r;
        } catch (e) {
            console.log("ERR", e);
        }
        return null
    }

    async fetch_block_row(chain, blockNumber, families = ["blockraw", "trace", "events", "feed", "n", "finalized", "feed", "autotrace"], feedOnly = false, blockHash = false) {
        let chainID = chain.chainID;
        if (!feedOnly && chain.isEVM > 0) {
            // OPTIMIZATION: its wasteful to bring in all these columns if the user hasn't asked for them ... however finalized is generally required
            families.push("blockrawevm");
            families.push("receiptsevm");
            families.push("traceevm");
            families.push("feedevm");
            families.push("traceevm");
        }
        const filter = {
            filter: [{
                family: families,
                cellLimit: 100
            }]
        };


        const tableChain = this.getTableChain(chainID);
        const [row] = await tableChain.row(paraTool.blockNumberToHex(blockNumber)).get(filter);

        return this.build_block_from_row(row)
    }

    push_rows_related_keys(family, column, rows, key, c) {
        let ts = this.getCurrentTS();
        let colData = {}
        colData[`${column}`] = {
            value: JSON.stringify(c),
            timestamp: ts * 1000000
        }
        let data = {}
        data[`${family}`] = colData
        if (key) {
            let row = {
                key: key.toLowerCase(),
                data
            }
            //console.log("PUSH", row);
            rows.push(row);
        }
    }

    getCurrentTS() {
        return Math.round(new Date().getTime() / 1000);
    }

    getTraceParseTS(maxDaysAgo = 31) {
        let [logDT, _] = paraTool.ts_to_logDT_hr(this.getCurrentTS() - 86400 * maxDaysAgo);
        return (paraTool.logDT_hr_to_ts(logDT, 0));
    }

    add_index_metadata(c) {
        c.source = this.hostname;
        c.genTS = this.getCurrentTS();
        c.commit = this.indexerInfo;
    }

    capitalizeFirstLetter(string) {
        return string.charAt(0).toUpperCase() + string.slice(1);
    }



    async loadEventBloomFilter() {
        try {
            let dir = `./schema/bloom/`
            let fn = path.join(dir, `event_topic.json`)
            var exported = JSON.parse(fs.readFileSync(fn));
            const importedFilter = BloomFilter.fromJSON(exported)
            console.log(`setup event_topic bloom?`, importedFilter.has('0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'))
            return importedFilter
        } catch (e) {
            let dir = `/root/go/src/github.com/colorfulnotion/evm-etl/substrate/schema/bloom/`
            let fn = path.join(dir, `event_topic.json`)
            var exported = JSON.parse(fs.readFileSync(fn));
            const importedFilter = BloomFilter.fromJSON(exported)
            console.log(`setup event_topic bloom?`, importedFilter.has('0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'))
            return importedFilter
        }
    }

    getLogDTRange(startLogDT = null, endLogDT = null, isAscending = true) {
        let startLogTS = paraTool.logDT_hr_to_ts(startLogDT, 0)
        let [startDT, _] = paraTool.ts_to_logDT_hr(startLogTS);
        if (startLogDT == null) {
            //startLogDT = (relayChain == "kusama") ? "2021-07-01" : "2022-05-04";
            startLogDT = "2023-02-01"
        }
        let ts = this.getCurrentTS();
        if (endLogDT != undefined) {
            let endTS = paraTool.logDT_hr_to_ts(endLogDT, 0) + 86400
            if (ts > endTS) ts = endTS
        }
        let logDTRange = []
        while (true) {
            ts = ts - 86400;
            let [logDT, _] = paraTool.ts_to_logDT_hr(ts);
            logDTRange.push(logDT)
            if (logDT == startDT) {
                break;
            }
        }
        if (isAscending) {
            return logDTRange.reverse();
        } else {
            return logDTRange
        }
    }

}
