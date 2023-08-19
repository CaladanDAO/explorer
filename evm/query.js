// Copyright 2023 Caladan DAO
// This file is part of CaladanDAO Block Explorer.

const AssetManager = require("./assetManager");
const paraTool = require("./paraTool");
const ethTool = require("./ethTool");
const mysql = require("mysql2");
const uiTool = require('./uiTool')
const assetAndPriceFeedTTL = 300; // how long the data stays cached


module.exports = class Query extends AssetManager {
    debugLevel = paraTool.debugNoLog;

    constructor(debugLevel = paraTool.debugNoLog) {
        super()

        if (debugLevel) {
            this.debugLevel = debugLevel;
        }
    }

    async init() {
	await this.assetManagerInit()
	this.contractABIs = await this.getContractABI();
	return (true);
    }


    // this supports reloading of chains/assets/specVersions every 15m
    async autoUpdate(intervalSeconds = 900) {
        while (1) {
            await this.sleep(intervalSeconds * 1000);
            console.log("query autoUpdating...")
            await this.init();
        }
    }
    async checkAPIKey(apikey) {
        let minkey = this.currentMinuteKey();
        let ratekey = `rate:${minkey}`;
        let ratelimit = 300;
        let usage = 0;
        try {
            const [row] = await this.btAPIKeys.row(apikey).get([ratekey, "n"]);
            if (row["n"] && row["n"]["ratelimit"]) {
                let x = row["n"]["ratelimit"];
                ratelimit = parseInt(x[0].value, 10);
            }
            if (row["rate"] && row["rate"][minkey]) {
                let y = row["rate"][minkey];
                if (y.length > 0) {
                    usage = y[0].value;
                }
            }
            //console.log("checkAPIKey", apikey, "usage", usage, "ratelimit", ratelimit);
            if (usage < ratelimit) {
                return ({
                    "success": true
                });
            } else {
                return ({
                    "error": "rate limit exceeded",
                    "code": 429
                });
            }
        } catch (e) {
            if (e.code == 404) {
                //console.log("checkAPIKey 404", apikey)
                return ({
                    "success": true
                });
            } else {
                return ({
                    "error": "general error",
                    "code": 401
                });
            }
        }

    }

    currentMinuteKey() {
        let today = new Date();
        let dd = today.getUTCDate().toString().padStart(2, '0');
        let mm = String(today.getUTCMonth() + 1).padStart(2, '0'); //January is 0!
        let yyyy = today.getUTCFullYear();
        let hr = today.getUTCHours().toString().padStart(2, "0");
        let min = today.getUTCMinutes().toString().padStart(2, "0");
        return `${yyyy}${mm}${dd}-${hr}${min}`;
    }

    async tallyAPIKey(apikey, cnt = 1) {
	return
        // increment "rate" cell
        try {
            let minkey = this.currentMinuteKey();
            let ratekey = `rate:${minkey}`;
            const row = this.btAPIKeys.row(apikey);
            await row.increment(ratekey, cnt);
            return (true);
        } catch (e) {
            console.log(e);
        }
        return (true);
    }


    canonicalizeEmail(e) {
        if ( e) {
	    return e.trim().toLowerCase();
	}
	
    }

    getPasswordHash(h) {
        let SALT = (process.env.POLKAHOLIC_SALT != undefined) ? process.env.POLKAHOLIC_SALT : "";
        return uiTool.blake2(`${SALT}${h}`)
    }

    create_api_key(email) {
        let ts = Math.floor(Date.now() / 1000)
        let raw = uiTool.blake2(email + ts.toString());
        raw = raw.substring(2, 34);
        return (raw);
    }

    async userExists(email) {
        var sql = `select password from user where email = '${email}' limit 1`;
        let users = await this.poolREADONLY.query(sql);
        return (users.length > 0)
    }

    resetPasswordSig(toMail, ts) {
        let h = ts + toMail + this.POLKAHOLIC_EMAIL_PASSWORD;
        let sig = uiTool.blake2(h);
        return sig;
    }

    async sendResetPasswordLink(toMail) {
        // include nodemailer
        const nodemailer = require('nodemailer');
        // declare vars
        let fromMail = 'info@polkaholic.io';

        let subject = 'Polkaholic.io Password Reset';
        let ts = new Date().getTime().toString();
        let sig = this.resetPasswordSig(toMail, ts);
        let RESETURL = `http://polkaholic.io/resetpassword/${toMail}/${ts}/${sig}`
        let text = `To reset your password on Polkaholic click this link:\r\n${RESETURL}`;

        // auth
        var transporter = nodemailer.createTransport({
            service: 'Godaddy',
            secureConnection: false,
            auth: {
                user: this.POLKAHOLIC_EMAIL_USER,
                pass: this.POLKAHOLIC_EMAIL_PASSWORD
            }
        });

        // email options
        let mailOptions = {
            from: fromMail,
            to: toMail,
            subject: subject,
            text: text
        };

        // send email
        transporter.sendMail(mailOptions, (error, response) => {
            if (error) {
                console.log(error);
            }
        });
    }

    async resetPassword(email, password, ts, sig) {
        let expectedSig = this.resetPasswordSig(email, ts);
        if (sig != expectedSig) return ({
            error: "Could not reset password"
        });

        var passwordHash = this.getPasswordHash(password)
        let sql = `update user set password = ${mysql.escape(passwordHash)} where email = ${mysql.escape(email)}`;
        try {
            this.batchedSQL.push(sql);
            await this.update_batchedSQL();
            return ({
                success: true
            });
        } catch (e) {
            this.logger.error({
                "op": "query.resetPassword",
                sql,
                err
            });
            return ({
                error: "Could not reset password"
            });
        }
    }


    async registerUser(email, password) {
        email = this.canonicalizeEmail(email);
        if (!uiTool.validEmail(email)) {
            return ({
                error: `Invalid email: ${email}`
            });
        }
        if (!uiTool.validPassword(password)) {
            return ({
                error: "Invalid password (must be 6 chars or more)"
            });
        }
        let userAlreadyExists = await this.userExists(email);
        if (userAlreadyExists) {
            return ({
                error: "User already exists."
            });
        }
        var passwordHash = this.getPasswordHash(password)
        try {
            var sql = `insert into user ( email, password, createDT ) values (${mysql.escape(email)}, ${mysql.escape(passwordHash)}, Now() )`
            this.batchedSQL.push(sql);
            await this.update_batchedSQL();
            return ({
                success: true
            });
        } catch (e) {
            this.logger.error({
                "op": "query.registerUser",
                email,
                passwordHash,
                err
            });
            return ({
                error: "Could not register user"
            });
        }
    }

    async validateUser(email, password) {
        let passwordHash = this.getPasswordHash(password)
        try {
            var sql = `select password from user where email = '${email}' limit 1`;
            let users = await this.poolREADONLY.query(sql);
            if (users.length == 0) {
                return {
                    error: "Email not found"
                };
            }
            if (users.length == 1 && (users[0].password != passwordHash)) {
                return {
                    error: "Password incorrect"
                };
            }
            return ({
                success: true
            });
        } catch (err) {
            this.logger.error({
                "op": "query.validateUser",
                email,
                passwordHash,
                err
            });
            return ({
                error: "Could not validate your account"
            })
        }
    }

    async updateAPIKeyPlan(email, apikey, planID) {
        // TODO with stripe
        try {
            // update bigtable with new PlanID
            let ratelimit = this.getPlanRateLimit(planID);
            let nrec = {};
            nrec["ratelimit"] = {
                value: JSON.stringify(ratelimit),
                timestamp: new Date()
            };
            let rowsToInsert = [{
                key: apikey,
                data: {
                    n: nrec
                }
            }];
            await this.btAPIKeys.insert(rowsToInsert);

            var sql = `update apikey set planID = ${mysql.escape(planID)} where email = ${mysql.escape(email)} and apikey = ${mysql.escape(apikey)}`;
            this.batchedSQL.push(sql);
            await this.update_batchedSQL();
            return (true);
        } catch (err) {
            this.logger.error({
                "op": "query.updateAPIKeyPlan",
                email,
                apikey,
                err
            });
            return (false);
        }
    }

    getAPIKeyPlan(loggedInEmail, apikey) {}
    async getAPIKeys(email) {
        var sql = `select apikey, createDT, planID from apikey where email = '${email}' and deleted = 0 limit 100`;
        try {
            let apikeys = await this.poolREADONLY.query(sql);
            return (apikeys);
        } catch (e) {
            this.logger.error({
                "op": "query.getAPIKeys",
                email,
                apikey,
                err
            });
            return (false);
        }
    }

    // # of request allowed per minute
    getPlanRateLimit(planID) {
        switch (planID) {
            case 1:
                return (1200); // 20 QPS
            case 2:
                return (6000); // 100 QPS
            case 3:
                return (30000); // 500 QPS
            default:
                return (300); // 5 QPS
        }
    }

    getPlans() {
        return [{
            name: "Developer",
            monthlyUSD: 0,
            minuteLimit: this.getPlanRateLimit(0)
        }, {
            name: "Lite",
            monthlyUSD: 199,
            minuteLimit: this.getPlanRateLimit(1)
        }, {
            name: "Pro",
            monthlyUSD: 399,
            minuteLimit: this.getPlanRateLimit(2)
        }, {
            name: "Enterprise",
            monthlyUSD: 1999,
            minuteLimit: this.getPlanRateLimit(3)
        }];
    }

    async createAPIKey(email, planID = 0) {
        let apikey = this.create_api_key(email)
        var sql = `insert into apikey (email, apikey, createDT) values (${mysql.escape(email)}, ${mysql.escape(apikey)}, Now())`;
        try {
            // update bigtable
            let ratelimit = this.getPlanRateLimit(planID);
            console.log(apikey, planID, ratelimit);
            let nrec = {};
            nrec["ratelimit"] = {
                value: JSON.stringify(ratelimit),
                timestamp: new Date()
            };
            let rowsToInsert = [{
                key: apikey,
                data: {
                    n: nrec
                }
            }];
            this.batchedSQL.push(sql);
            await this.update_batchedSQL();
            await this.btAPIKeys.insert(rowsToInsert);
            return ({
                success: true,
                apikey: apikey
            })
        } catch (err) {
            this.logger.error({
                "op": "query.createAPIKey",
                email,
                sql,
                err
            });
            return ({
                error: "Could not create API Key"
            });
        }
    }

    async deleteAPIKey(email, apikey) {
        var sql = `update apikey set deleted = 1, deleteDT = Now() where email = ${mysql.escape(email)} and apikey = ${mysql.escape(apikey)}`;
        try {
            this.batchedSQL.push(sql);
            await this.update_batchedSQL();
            return ({
                success: true
            })
        } catch (e) {
            this.logger.error({
                "op": "query.deleteAPIKey",
                email,
                sql,
                err
            });
            return ({
                error: "Could not delete API Key"
            });
        }
    }

    async search_address(addr) {
        let res = [];
        try {
            let [tblName, tblRealtime] = this.get_btTableRealtime()
            // TODO: use getRow?
            let [rows] = await tblRealtime.getRows({
                keys: [addr]
            });
            rows.forEach((row) => {
                let rowData = row.data;
                if (rowData["wasmcontract"]) {
                    for (const chainID of Object.keys(rowData["wasmcontract"])) {
                        let cells = rowData["wasmcontract"][chainID]
                        let cell = cells[0];
                        let c = JSON.parse(cell.value);
                        if (c && c.address) {
                            // TODO: adjust description based on PSP22, ... contractType features
                            res.push({
                                link: `/wasmcontract/${c.address}/${c.chainID}`,
                                text: "WASM Contract: " + c.address,
                                description: `ChainID: ${c.chainID} Deployer: ${c.deployer}`
                            })
                        }
                    }
                } else if (rowData["evmcontract"]) {
                    for (const chainID of Object.keys(rowData["evmcontract"])) {
                        let cells = rowData["evmcontract"][chainID]
                        let cell = cells[0];
                        let c = JSON.parse(cell.value);
                        if (c && c.asset) {
                            // TODO: adjust text/description based on ERC20, ERC721, ERC1155 assetType data
                            console.log("SEARCH RES", c);
                            let [__, id] = this.convertChainID(c.chainID)
                            let chainName = this.getChainName(c.chainID);
                            let description = `Chain ID: ${c.chainID} ${chainName}`
                            let paraID = paraTool.getParaIDfromChainID(chainID)
                            if (paraID > 0) {
                                description += ` ParaID: ${paraID}`;
                            }
                            if (c.symbol) {
                                description += ` ${c.symbol}`
                            }
                            res.push({
                                link: `/address/${c.asset}/${c.chainID}`,
                                text: "EVM Contract: " + c.asset,
                                description: description,
                                numHolders: c.numHolders
                            })
                        }
                    }
                } else {
                    if (addr.length == 42) {
                        res.push({
                            link: "/address/" + addr,
                            text: addr,
                            description: "Address"
                        })
                    } else {
                        res.push({
                            link: "/account/" + addr,
                            text: addr,
                            description: "Address"
                        })
                    }
                }
            });
        } catch (err) {
            if (err.code == 404) {
                return res;
            } else {
                this.logger.error({
                    "op": "query.search_address",
                    addr,
                    err
                });
            }
        }
        return res;
    }

    getChainInfo(chainID = paraTool.chainIDPolkadot) {
        return this.getChainFullInfo(chainID)
    }

    redirect_search_block(hash, blockcells, res = []) {
        let cell = blockcells[0];
        let feed = JSON.parse(cell.value);
        if (feed) {
            let chainID = feed.chainID;
            let blockNumber = feed.blockNumber;
            if (blockNumber) {
                // send users to eg /block/0/9963670?blockhash=0xcf10b0c43f5c87de7cb9b3c0be6187097bd936bde19bd937516482ac01a8d46f
                res.push({
                    type: "block",
                    blockNumber: blockNumber,
                    chainID: chainID,
                    link: `/block/${chainID}/${blockNumber}?blockhash=${hash}`,
                    text: `chain: ${chainID} blockNumber: ${blockNumber} hash: ${hash}`,
                    description: this.getChainName(chainID) + " Block " + blockNumber + " : " + hash
                })
            }
        }
    }

    redirect_search_stateroot(hash, blockcells, res = []) {
        let cell = blockcells[0];
        let feed = JSON.parse(cell.value);
        if (feed) {
            let blockHash = feed.blockHash
            let chainID = feed.chainID;
            let blockNumber = feed.blockNumber;
            if (blockNumber) {
                // send users to eg /block/0/9963670?blockhash=0xcf10b0c43f5c87de7cb9b3c0be6187097bd936bde19bd937516482ac01a8d46f
                res.push({
                    type: "stateroot",
                    chainID: chainID,
                    blockNumber: blockNumber,
                    link: `/block/${chainID}/${blockNumber}?blockhash=${blockHash}`,
                    text: `chain: ${chainID} blockNumber: ${blockNumber} hash: ${blockHash}`,
                    description: this.getChainName(chainID) + " Block " + blockNumber + " : " + blockHash
                })
            }
        }
    }

    // send users to eg /chain/{id}
    redirect_search_chain(search, cells, res = []) {
        let cell = cells[0];
        let c = JSON.parse(cell.value);
        if (c && (c.chainID != undefined)) {
            let chainID = c.chainID;
            res.push({
                link: `/blocks/${c.id}`,
                text: `${c.chainName}`,
                description: `chainID: ${c.chainID} id: ${c.id} para ID: ${c.paraID} relay chain: ${c.relayChain}`,
                numHolders: c.numHolders
            })
        }
    }

    // send users to eg /chain/{id}
    redirect_search_symbol(search, cells, res = []) {
        let cell = cells[0];
        let c = JSON.parse(cell.value);
        if (c && c.symbol) {
            res.push({
                link: `/symbol/${c.symbol}`,
                text: `${c.symbol}`,
                description: `relay chain: ${c.relayChain}`,
                numHolders: c.numHolders
            })
        }
    }

    check_block_hash(hash, blockcells, res) {
        let cell = blockcells[0];
        let feed = JSON.parse(cell.value);
        if (feed) {
            let chainID = feed.chainID;
            let blockNumber = feed.blockNumber;
            let relayBN = (feed.relayBN != undefined) ? feed.relayBN : null
            let relayStateRoot = (feed.relayStateRoot != undefined) ? feed.relayStateRoot : null
            let blockType = 'substrate'
            if (feed.blockType != undefined) {
                blockType = feed.blockType
            }
            if (blockNumber) {
                res.hash = hash
                res.chainID = chainID
                res.blockNumber = blockNumber
                if (blockType == 'evm') {
                    res.hashType = 'evmBlockHash'
                } else if (blockType == 'substrate') {
                    if (relayBN) res.relayBN = relayBN
                    if (relayStateRoot) res.relayStateRoot = relayStateRoot
                    res.hashType = 'substrateBlockHash'
                }
            }
        }
    }

    check_tx_hash(hash, txcells, res) {
        let cell = txcells[0]; // TODO: how do you support edge case of multiple distinct txhashes - can we use versions https://github.com/paritytech/polkadot/issues/231
        let feed = JSON.parse(cell.value);
        if (feed) {
            let chainID = feed.chainID;
            let blockNumber = (feed.blockNumber != undefined) ? feed.blockNumber : null;
            let hashType = (feed.extrinsicHash != undefined) ? 'extrinsicHash' : 'transactionHash'
            res.hash = hash
            res.chainID = chainID
            res.blockNumber = blockNumber
            res.hashType = hashType
        }
    }

    redirect_search_tx(hash, txcells, res = []) {
        let cell = txcells[0]; // TODO: how do you support edge case of multiple distinct txhashes - can we use versions https://github.com/paritytech/polkadot/issues/231
        let feed = JSON.parse(cell.value);
        if (feed) {
            let chainID = feed.chainID;
            let blockNumber = feed.blockNumber;
            let addr = feed.addr;
            res.push({
                type: "tx",
                chainID: chainID,
                blockNumber: blockNumber,
                link: "/tx/" + hash,
                text: `chain: ${chainID} blockNumber: ${blockNumber} address: ${addr}`,
                description: "tx"
            })
        }
    }

    async search_hash(hash) {
        let res = [];
        let families = ['feed', 'feedunfinalized', 'feedevmunfinalized', 'feedpending'] // 3 columnfamily
        try {
            let [rows] = await this.btHashes.getRows({
                keys: [hash]
            });
            rows.forEach((row) => {
                let rowData = row.data;
                //priority: use feed then feedunfinalized/feedevmunfinalized
                let data = false
                if (rowData["feed"]) {
                    // finalized
                    data = rowData["feed"]
                } else if (rowData["feedunfinalized"]) {
                    data = rowData["feedunfinalized"]
                } else if (rowData["feedevmunfinalized"]) {
                    data = rowData["feedevmunfinalized"]
                } else if (rowData["feedpending"]) {
                    data = rowData["feedpending"]
                }
                if (data) {
                    if (data["block"]) {
                        this.redirect_search_block(hash, data["block"], res)
                    } else if (data["stateroot"]) {
                        this.redirect_search_stateroot(hash, data["stateroot"], res)
                    } else if (data["tx"]) {
                        this.redirect_search_tx(hash, data["tx"], res)
                    }
                } else {
                    if (rowData["wasmcode"]) {
                        for (const chainID of Object.keys(rowData["wasmcode"])) {
                            this.redirect_search_wasmcode(hash, rowData["wasmcode"][chainID], res)
                        }
                    }
                    if (rowData["wasmcontract"]) {
                        for (const chainID of Object.keys(rowData["wasmcontract"])) {
                            this.redirect_search_wasmcontract(hash, rowData["wasmcontract"][chainID], res)
                        }
                    } else if (rowData["symbol"]) {
                        for (const relayChain of Object.keys(rowData["symbol"])) {
                            this.redirect_search_symbol(hash, rowData["symbol"][relayChain], res)
                        }
                    } else if (rowData["chain"]) {
                        for (const chainID of Object.keys(rowData["chain"])) {
                            this.redirect_search_chain(hash, rowData["chain"][chainID], res)
                        }
                    }
                }
            });
        } catch (err) {
            if (err.code == 404) {
                console.log("NOT FOUND", hash);
                return res;
            } else {
                console.log(err);
                this.logger.error({
                    "op": "query.search_hash",
                    hash,
                    err
                });
            }
        }
        return res;
    }

    async lookupHash(hash) {
        let res = {
            hash: hash,
            hashType: 'NotFound', //substrateBlockHash/evmBlockHash, extrinsicHash, transactionHash
            status: 'NotFound',
            chainID: null,
            blockNumber: null,
        };
        let families = ['feed', 'feedunfinalized', 'feedevmunfinalized', 'feedpending'] // 3 columnfamily
        try {
            // TODO: use getRow
            let [rows] = await this.btHashes.getRows({
                keys: [hash]
            });
            for (let i = 0; i < rows.length; i++) {
                let row = rows[i];
                let rowData = row.data;
                //priority: use feed then feedunfinalized/feedevmunfinalized
                let blockcells = false;
                let txcells = false;
                let data = false

		if (rowData["evmtx"]) {
                    data = rowData["evmtx"]
                    res.status = 'finalized';
                } else if (rowData["evmtxunfinalized"]) {
                    data = rowData["evmtxunfinalized"]
                    res.status = 'unfinalized';
                } else if (rowData["feed"]) {
                    // finalized
                    data = rowData["feed"]
                    res.status = 'finalized'
                } else if (rowData["feedunfinalized"]) {
                    data = rowData["feedunfinalized"]
                    res.status = 'unfinalized'
                } else if (rowData["feedevmunfinalized"]) {
                    data = rowData["feedevmunfinalized"]
                    res.status = 'unfinalized'
                } else if (rowData["feedpending"]) {
                    data = rowData["feedpending"]
                    res.status = 'pending'
                }
                if (data) {
                    if (data["block"]) {
                        blockcells = data["block"]
                        this.check_block_hash(hash, blockcells, res)
                    } else if (data["stateroot"]) {
                        blockcells = data["stateroot"]
                        this.check_stateroot_hash(hash, blockcells, res)
                    } else if (data["tx"]) {
                        txcells = data["tx"]
                        this.check_tx_hash(hash, txcells, res)
                    }
                }
            }
        } catch (err) {
            if (err.code == 404) {
                return res;
            } else {
                this.logger.error({
                    "op": "query.lookupHashsearch_hash",
                    hash,
                    err
                });
            }
        }
        return res;
    }

    async search_blocks(bn) {
        let chains = await this.getChains();
        let res = [];
        for (var i = 0; i < chains.length; i++) {
            if (bn < chains[i].blocksCovered) {
                res.push({
                    link: "/block/" + chains[i].chainID + "/" + bn,
                    text: chains[i].chainName + " Block " + bn.toString(),
                    description: "Block",
                    numHolders: chains[i].numHolders
                });
            }
        }
        return res;
    }

    async getSearchResults(search) {
        if (search.length > 45 && search.length < 53) {
            search = paraTool.getPubKey(search);
        }
        var tasks = [this.search_address(search.toLowerCase()), this.search_hash(search.toLowerCase())];
        var results = await Promise.all(tasks);
        var out = results.flat(2);
        if (out.length == 0) {
            let bn = parseInt(search, 10);
            if (bn > 0) {
                return await this.search_blocks(bn);
            }
        }
        if (out.length > 1) { // better would be sort by TVL (numHolders*priceUSD) desc
            out.sort(function(a, b) {
                let b1 = (b.numHolders !== undefined) ? b.numHolders : 0;
                let a1 = (a.numHolders !== undefined) ? a.numHolders : 0;
                return (b1 - a1);
            })
        }
        return out;
    }

    async getChainSymbols(chainID_or_chainName) {
        let [chainID, id] = this.convertChainID(chainID_or_chainName)
        if (chainID === false) return [];
        try {
            let sql = `select distinct symbol from asset where chainID = ${chainID} and assetType = 'Token' order by symbol`;
            let symbols = await this.poolREADONLY.query(sql);
            return symbols;
        } catch (err) {
            this.logger.error({
                "op": "query.getChainSymbols",
                chainID_or_chainName,
                err
            });
        }
    }


    trimquote(s) {
        if (s.length >= 2 && (s.substring(0, 1) == '"') && (s.substring(s.length - 1, s.length))) {
            return s.substring(1, s.length - 1);
        }
        return s;
    }


    async getHashStatus(hash) {
        // 'notFound', 'pending', 'unfinalized', 'finalized'
        let res = {
            hashType: 'unknown',
            status: 'notFound'
        }
        let families = ['feed', 'feedunfinalized', 'feedevmunfinalized', 'feedpending'] // 3 columnfamily
        try {
            let [rows] = await this.btHashes.getRows({
                keys: [hash]
            });
            rows.forEach((row) => {
                let rowData = row.data;
                //priority: use feed then feedunfinalized/feedevmunfinalized
                let data = false;
                if (rowData["feed"]) {
                    // finalized
                    data = rowData["feed"]
                    res.status = 'finalized'
                } else if (rowData["feedunfinalized"]) {
                    data = rowData["feedunfinalized"]
                    res.status = 'unfinalized'
                } else if (rowData["feedevmunfinalized"]) {
                    data = rowData["feedevmunfinalized"]
                    res.status = 'unfinalized'
                } else if (rowData["feedpending"]) {
                    data = rowData["feedpending"]
                    res.status = 'pending'
                }
                if (data) {
                    if (data["tx"]) {
                        res.hashType = 'tx'
                    } else if (data["block"]) {
                        res.hashType = 'block'
                    }
                }
            });
        } catch (err) {
            this.logger.error({
                "op": "query.getHashStatus",
                hash,
                err
            });
        }
        return res;
    }

    getAssetSymbol(asset) {
        try {
            if (typeof asset == "string") {
                let a = JSON.parse(asset)
                if (a.Token) return (a.Token);
                return (false);
            }
            if (asset && asset.Token) {
                if (a.Token) return (a.Token);
                return (false);
            }
        } catch (err) {
            this.logger.error({
                "op": "query.getAssetSymbol",
                asset,
                err
            });

        }
        return (false);
    }

    async get_tx_link(f, txHash, finalized = false) {
        for (const hash of Object.keys(f)) {
            const cell = f[hash][0];
            let c = JSON.parse(cell.value);
            try {
                let chainID = c.chainID
                let bn = c.bn
                let r = await this.fetch_evm_block_gs(chainID, bn);
                let [bl, targetTrace] = await this.decorate_block_transaction_trace(r, chainID, txHash)
                //******* TODO: use targetTrace somehow!!! ******
                if (bl && Array.isArray(bl.transactions)) {
                    let tx = bl.transactions[0]
                    tx.finalized = finalized;
                    tx.status = finalized ? "finalized" : "unfinalized";
                    //tx.chainID = c.chainID;
                    tx.chainName = this.getChainName(c.chainID)
                    //TODO: update txn.transactionsInternal to use new format
                    tx.transactionsInternal = []
                    return tx;
                } else {
                    return false
                }
            } catch (err) {
                console.log(err);
            }
        }
        return null;
    }

    async getContractsCall(extrinsicHash) {
        let sql = `select identifier, CONVERT(decodedCall using utf8) as decodedCall from contractsCall where extrinsicHash = '${extrinsicHash}' order by blockTS desc limit 1`;
        console.log(sql);
        let recs = await this.poolREADONLY.query(sql);
        if (recs.length > 0) {
            return recs[0];
        }
        return null;
    }

    async getTransaction(txHash, decorate = true, decorateExtra = ["usd", "address", "related", "data"], isRecursive = true) {
        //console.log(`getTransaction txHash=${txHash}, decorate=${decorate}, decorateExtra=${decorateExtra}`)
        let [decorateData, decorateAddr, decorateUSD, decorateRelated] = this.getDecorateOption(decorateExtra)
        //console.log(`getTransaction txHash=${txHash} decorateData=${decorateData} decorateAddr=${decorateAddr} decorateUSD=${decorateUSD} decorateRelated=${decorateRelated}`)
        const filter = {
            column: {
                cellLimit: 1
            },
        };
        if (!this.validAddress(txHash)) {
            throw new paraTool.InvalidError(`Invalid Extrinsic Hash: ${txHash}`)
        }
        try {
            const [row] = await this.btHashes.row(txHash).get({
                filter
            });

            let rowData = row.data;
            let feedData = false
            let feedTX = false
            let status = ""
            let isPending = false
            let isEVMUnfinalized = false
            if (rowData["feed"]) {
                feedData = rowData["feed"]
                status = "finalized"
            } else if (rowData["feedunfinalized"]) {
                feedData = rowData["feedunfinalized"]
                status = "unfinalized"
            } else if (rowData["feedevmunfinalized"]) {
                feedData = rowData["feedevmunfinalized"]
                status = "unfinalized"
                isEVMUnfinalized = true
            } else if (rowData["feedpending"]) {
                feedData = rowData["feedpending"]
                status = "pending"
                isPending = true
            }
            if (rowData["evmtx"]) {
                return await this.get_tx_link(rowData["evmtx"], txHash, true);
            } else if (rowData["evmtxunfinalized"]) {
                return await this.get_tx_link(rowData["evmtxunfinalized"], txHash, false);
            } else if (feedData && feedData["tx"]) {
                feedTX = feedData["tx"]
            }
            if (feedTX) {
                const cell = feedTX[0];
                let c = JSON.parse(cell.value);
                //console.log(`getTransaction raw ${txHash}`, c)
                if (!paraTool.auditHashesTx(c)) {
                    console.log(`Audit Failed`, txHash)
                }

                if (c.transactionHash) {
                    // this is an EVM tx
                    let assetChain = c.to ? paraTool.makeAssetChain(c.to.toLowerCase(), c.chainID) : null;
                    if (this.assetInfo[assetChain]) {
                        c.assetInfo = this.assetInfo[assetChain];
                    }
                    c.chainName = this.getChainName(c.chainID)
                    let chainAsset = this.getChainAsset(c.chainID)
                    let cTimestamp = (isPending) ? Math.floor(Date.now() / 1000) : c.timestamp
                    if (isPending) {
                        c.timestamp = cTimestamp
                    }
                    let cFee = (isPending) ? 0 : c.fee
                    //await this.decorateUSD(c, "value", chainAsset, c.chainID, cTimestamp, decorateUSD)
                    if (decorateUSD) {
                        let p = await this.computePriceUSD({
                            val: c.value,
                            asset: chainAsset,
                            chainID: c.chainID,
                            ts: cTimestamp
                        });
                        if (p) {
                            c.valueUSD = p.valUSD;
                            c.priceUSD = p.priceUSD;
                            c.priceUSDCurrent = p.priceUSDCurrent;
                        }
                    }

                    c.symbol = this.getChainSymbol(c.chainID);
                    if (!isPending && decorateUSD) {
                        c.feeUSD = c.fee * c.priceUSD;
                    }

                    c.result = c.status // this is success/fail indicator of the evm tx
                    c.status = status // finalized/unfinalized

                    // decorate transactionsInternal
                    if (c.transactionsInternal !== undefined && c.transactionsInternal.length > 0) {
                        for (let i = 0; i < c.transactionsInternal.length; i++) {
                            let t = c.transactionsInternal[i];
                            t.valueRaw = t.value;
                            t.value = t.valueRaw / 10 ** this.getChainDecimal(c.chainID);
                            if (decorateUSD) {
                                t.valueUSD = c.priceUSD * t.value;
                                t.priceUSD = c.priceUSD;
                                t.priceUSDCurrent = c.priceUSDCurrent;
                            }
                        }
                    }
                    // decorate transfers
                    if (c.transfers !== undefined && c.transfers.length > 0) {
                        for (let i = 0; i < c.transfers.length; i++) {
                            let t = c.transfers[i];
                            //let tokenAsset = t.tokenAddress.toLowerCase();
                            //let tokenAssetChain = paraTool.makeAssetChain(tokenAsset, c.chainID);
                            let [isXcAsset, tokenAssetChain, rawAssetChain] = paraTool.getErcTokenAssetChain(t.tokenAddress, c.chainID) // REVIEW
                            let [tokenAsset, _] = paraTool.parseAssetChain(tokenAssetChain)
                            if (this.assetInfo[tokenAssetChain]) {
                                t.assetInfo = this.assetInfo[tokenAssetChain];
                                if (t.assetInfo.decimals !== false) {
                                    t.value = t.value / 10 ** t.assetInfo.decimals;
                                    if (decorateUSD) {
                                        let p = await this.computePriceUSD({
                                            val: t.value,
                                            asset: tokenAsset,
                                            chainID: c.chainID,
                                            ts: cTimestamp
                                        });
                                        if (p) {
                                            t.valueUSD = p.valUSD;
                                            t.priceUSD = p.priceUSD;
                                            t.priceUSDCurrent = p.priceUSDCurrent;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    return c;
                }

                if (!isPending) {
                    //pending does not have event, fee, specVersion, blockNumber
                    let dEvents = []
                    if (d.events) {
                        for (const evt of d.events) {
                            let [dEvent, isTransferType] = await this.decorateEvent(evt, d.chainID, d.ts, decorate, decorateExtra)
                            dEvents.push(dEvent)
                        }
                        d.events = dEvents
                        //await this.decorateFee(d, d.chainID, decorateUSD)
                        d.specVersion = this.getSpecVersionForBlockNumber(d.chainID, d.blockNumber);
                    }
                }
                return d;
            }

        } catch (err) {
            if (err.code == 404) {
                throw new paraTool.NotFoundError(`Transaction not found: ${txHash}`)
            } else {
                console.log(err);
                this.logger.error({
                    "op": "query.getTransaction",
                    txHash,
                    err
                });
            }
        }
        return (false);
    }

    async getAssetQuery(chainID, asset, queryType = "pricefeed", homeAddress = false, querylimit = 3000) {
        switch (queryType) {
            case "pricefeed":
                if (querylimit > 3000) querylimit = 3000
                return await this.getAssetPriceFeed(chainID, asset, querylimit)
            case "holders":
                if (querylimit > 3000) querylimit = 1000
                return await this.getAssetHolders(chainID, asset, querylimit)
            case "related":
                if (querylimit > 3000) querylimit = 100
                return await this.getAssetsRelated(chainID, asset, homeAddress = false, querylimit)
            default:
                return false;
                break;
        }
    }


    getHoldingsState(holdings, asset, chainID) {
        if (!holdings) return (false);
        for (const assetType of Object.keys(holdings)) {
            let a = holdings[assetType];
            for (let i = 0; i < a.length; i++) {
                let b = a[i];
                if ((b.assetInfo.asset == asset) && (b.assetInfo.chainID == chainID)) {
                    return b.state;
                }
            }
        }
        return (undefined);
    }

    async getChainRecentBlocks(chainID_or_chainName, startBN = false, limit = 50) {
        let chain = await this.getChain(chainID_or_chainName);
        let [chainID, id] = this.convertChainID(chainID_or_chainName)
        if (chainID === false) return ([]);
        if (!startBN) startBN = chain.blocksCovered - limit;
        try {
            let sql = `select blockNumber, blockHash, UNIX_TIMESTAMP(blockDT) as blockTS, numTransactions as numTransactionsEVM, gasUsed, fees, feesBurned from block${chainID} order by blockNumber desc limit 50`
            let blocks = await this.poolREADONLY.query(sql);
            return blocks;
        } catch (err) {
            this.logger.error({
                "op": "query.getChainRecentBlocks",
                chainID_or_chainName,
                err
            });

        }
        return ([]);
    }

    async decorateBlock(block, chainID, evmBlock = false, decorate = true, decorateExtra = ["data", "address", "usd", "related"]) {
        let [decorateData, decorateAddr, decorateUSD, decorateRelated] = this.getDecorateOption(decorateExtra)
        try {
            let exts = block.extrinsics
            let decoratedExts = []
            for (const d of exts) {
                //let de = await this.decorateBlockExtrinsic(d, chainID, block.blockTS, decorate, decorateExtra)
                let de = await this.decorateExtrinsic(d, chainID, "", decorate, decorateExtra)
                decoratedExts.push(de)
            }
            block.extrinsics = decoratedExts
            if (decorate && block.author != undefined) {
                block.authorAddress = paraTool.getPubKey(block.author)
                this.decorateAddress(block, "authorAddress", decorateAddr, false)
            } else if (evmBlock && evmBlock.author != undefined) {
                block.author = evmBlock.author
                block.authorAddress = paraTool.getPubKey(evmBlock.author)
                this.decorateAddress(block, "authorAddress", decorateAddr, false)
            }

            block.specVersion = this.getSpecVersionForBlockNumber(chainID, block.number);
            if (evmBlock) {
                if (evmBlock.transactionsInternal !== undefined && evmBlock.transactionsInternal.length > 0) {
                    for (let i = 0; i < evmBlock.transactionsInternal.length; i++) {
                        let t = evmBlock.transactionsInternal[i];
                        t.valueRaw = t.value;
                        t.value = t.valueRaw / 10 ** this.getChainDecimal(chainID);
                        if (decorateUSD) {
                            /*t.valueUSD = c.priceUSD * t.value;
                            t.priceUSD = c.priceUSD;
                            t.priceUSDCurrent = c.priceUSDCurrent;*/
                        }
                    }
                }
                if (evmBlock.transactionsConnected == undefined) evmBlock.transactionsConnected = []
                block.evmBlock = evmBlock
                // decorate transactionsInternal
            }
        } catch (err) {
            this.logger.error({
                "op": "decorateBlock",
                chainID,
                number: block.number,
                err
            });
        }
        return block
    }


    async decorate_block_transaction_trace(r, chainID, txHash) {
        //let tableIdDisabled = true
        //await this.initEvmSchemaMap(tableIdDisabled)
        //console.log(`decorate_block_transaction_trace txHash=${txHash}`)
        //console.log('decorate_evm_block', r)
        //let evmRPCInternalApi = this.evmRPCInternal
        let blkNum = false
        let blkHash = false
        let blockTS = false
        let blockAvailable = false
        let traceAvailable = false
        let receiptsAvailable = false
        let rpcBlock = r.block
        let rpcReceipts = r.receipts
        let rpcTraces = r.traces
        let evmReceipts = []
        let evmTrace = false
        let blk = false
        let stream_bq = false
        let write_bt = false
        let targetTrace = false;
        let transactionIndex;
        for (const txn of rpcBlock.transactions) {
            if (txn.hash == txHash) {
                transactionIndex = paraTool.dechexToInt(txn.transactionIndex)
                break
            }
        }
        if (transactionIndex == undefined) {
            console.log(`lookup failed!`)
            return [false, false]
        }
        if (rpcBlock) {
            blockAvailable = true
            let targetTxn = rpcBlock.transactions[transactionIndex]
            rpcBlock.transactions = [targetTxn]
            blk = ethTool.standardizeRPCBlock(rpcBlock)
            blkNum = blk.number;
            blkHash = blk.hash;
            blockTS = blk.timestamp;
        } else {
            console.log(`rpcBlock missing`, r)
        }
        if (rpcTraces) {
            targetTrace = rpcTraces[transactionIndex]
            traceAvailable = true
            evmTrace = [targetTrace]
        } else {
            console.log(`rpcTraces missing`, r)
        }
        if (rpcReceipts) {
            receiptsAvailable = true
            let targetReceipt = rpcReceipts[transactionIndex]
            evmReceipts = ethTool.standardizeRPCReceiptLogs([targetReceipt])
        } else {
            console.log(`rpcReceipts missing`, r)
        }
        /*
        console.log(`blk`, blk)
        console.log(`evmReceipts`, evmReceipts)
        console.log(`evmTrace`, evmTrace)
        */
        //console.log(`decorate_block_transaction [${blkNum}] [${blkHash}] Trace=${traceAvailable}, Receipts=${receiptsAvailable} , currTS=${this.getCurrentTS()}, blockTS=${blockTS}`)
        var statusesPromise = Promise.all([
            this.processTransactions(blk.transactions),
            this.processReceipts(evmReceipts)
        ])
        let [dTxns, dReceipts] = await statusesPromise

        //console.log(`++++ [#${blkNum} ${blkHash}] dTxns len=${dTxns.length}`, dTxns)
        //console.log(`[#${blkNum} ${blkHash}] dReceipts len=${dReceipts.length}`, dReceipts)
        let flatTraces = ethTool.debugTraceToFlatTraces(evmTrace, dTxns)
        //console.log(`flatTraces[${flatTraces.length}]`, flatTraces)
        //fuseBlockTransactionReceipt(evmBlk, dTxns, dReceipts, flatTraces, chainID)
        let evmBlockTransaction = await ethTool.fuseBlockTransactionReceipt(blk, blk.transactions, dReceipts, flatTraces, chainID)
        //console.log(`evmBlockTransaction`, evmBlockTransaction)
        //console.log(JSON.stringify(evmBlockTransaction))
        return [evmBlockTransaction, targetTrace]
    }

    async decorate_evm_block(chainID, r) {
        console.log('decorate_evm_block', r)
        //let evmRPCInternalApi = this.evmRPCInternal
	console.log(r);
        let blkNum = false
        let blkHash = false
        let blockTS = false
        let blockAvailable = false
        let traceAvailable = false
        let receiptsAvailable = false
        let rpcBlock = r.block
        let rpcReceipts = r.receipts
        //let chainID = r.chain_id
        let evmReceipts = []
        let evmTrace = false
        let blk = false
        let stream_bq = false
        let write_bt = false

        if (rpcBlock) {
            blockAvailable = true
            blk = ethTool.standardizeRPCBlock(rpcBlock)
            console.log(`evmBlk++`, blk)
            blkNum = blk.number;
            blkHash = blk.hash;
            blockTS = blk.timestamp;
        } else {
            console.log(`r.block missing`, r)
        }
        if (r.traces) {
            traceAvailable = true
            evmTrace = r.traces
        }
        if (rpcReceipts) {
            receiptsAvailable = true
            evmReceipts = ethTool.standardizeRPCReceiptLogs(rpcReceipts)
        } else {
            console.log(`r.rpcReceipts missing`, r)
        }
        //console.log(`blk.transactions`, blk.transactions)
        console.log(`decorate_evm_block [${blkNum}] [${blkHash}] Trace=${traceAvailable}, Receipts=${receiptsAvailable} , currTS=${this.getCurrentTS()}, blockTS=${blockTS}`)
        var statusesPromise = Promise.all([
            this.processTransactions(blk.transactions),
            this.processReceipts(evmReceipts)
        ])
        let [dTxns, dReceipts] = await statusesPromise

        //console.log(`++++ [#${blkNum} ${blkHash}] dTxns len=${dTxns.length}`, dTxns)
        //console.log(`[#${blkNum} ${blkHash}] dReceipts len=${dReceipts.length}`, dReceipts)
        let flatTraces = ethTool.debugTraceToFlatTraces(evmTrace, dTxns)
        //console.log(`flatTraces[${flatTraces.length}]`, flatTraces)
        //fuseBlockTransactionReceipt(evmBlk, dTxns, dReceipts, flatTraces, chainID)
        let evmFullBlock = await ethTool.fuseBlockTransactionReceipt(blk, blk.transactions, dReceipts, flatTraces, chainID)
        return evmFullBlock;
    }

    async getBlock(chainID_or_chainName, blockNumber, blockHash = false, decorate = true, decorateExtra = ["data", "address", "usd", "related"]) {
        let [chainID, id] = this.convertChainID(chainID_or_chainName)

        if (chainID === false) throw new paraTool.NotFoundError(`Invalid chain: ${chainID_or_chainName}`)
        let chain = await this.getChain(chainID);
        if (blockNumber > chain.blocksCovered) {
            throw new paraTool.InvalidError(`Invalid blockNumber: ${blockNumber} (tip: ${chain.blocksCovered})`)
        }
        try {
            let row = await this.fetch_block(chainID, blockNumber);
            let evmFullBlock = (row.evmFullBlock) ? row.evmFullBlock : false
            if (row && row.receipts != undefined) {
                try {
                    row.evmBlock = await this.decorate_evm_block(chainID, row)

                    return row
                } catch (err) {
                    console.log(err);
                }
            }
            if (row.feed) {
                let block = row.feed;
                block = await this.decorateBlock(row.feed, chainID, evmFullBlock, decorate, decorateExtra);
                return block;
            } else {
                throw new paraTool.NotFoundError(`Block not indexed yet: ${blockNumber}`)
            }
        } catch (err) {
            if (err.code == 404) {
                throw new paraTool.NotFoundError(`Block not found: ${blockNumber}`)
            }
            this.logger.error({
                "op": "query.getBlock",
                chainID,
                blockNumber,
                blockHash,
                err
            });
        }
        return false;
    }


    async getBlockByHash(blockHash = false, decorate = true, decorateExtra = ["data", "address", "usd", "related"]) {
        let res = await this.lookupHash(blockHash)
        if (res.hashType == "evmBlockHash") {
            if (res.status == "finalized" || res.status == "unfinalized") {
                let chainID = res.chainID
                let blockNumber = res.blockNumber
                let block = await this.getBlock(chainID, blockNumber, blockHash, decorate, decorateExtra)
                if (res.hashType == "evmBlockHash" && block.evmBlock) {
                    // return just the evmBlock is looked up by evmBlockHash
                    return block.evmBlock
                } else {
                    block.chainID = res.chainID
                    return block
                }
            }
        }
        return false
    }

    parse_date(d) {
        return d; // TODO
    }

    parse_asset(a) {
        let [assetUnparsed, chainID] = paraTool.parseAssetChain(a);
        return (assetUnparsed);
    }

    async getAccountBalances(rawAddress, lookback = 180, ts = null, maxRows = 1000) {
        let chainList = []
        let balances = await this.getAccount(rawAddress, "balances", chainList, maxRows, ts, lookback);
        // TODO: treat "false" case
        return (balances);
    }

    async getAccountUnfinalized(rawAddress, lookback = 180, ts = null, maxRows = 1000) {
        let chainList = []
        let unfinalized = await this.getAccount(rawAddress, "unfinalized", chainList, maxRows, ts, lookback);
        // TODO: treat "false" case
        return (unfinalized);
    }

    page_params(ts, limit, p, chainList, decorate, decorateExtra) {
        let out = `ts=${ts}&limit=${limit}`
        if (p > 0) out += `&p=${p}`
        if (chainList.length > 0) {
            out += `&chainfilters=` + chainList.join(",");
        }
        if (decorate) {
            out += `&decorateExtra=` + decorateExtra.join(",");
        }
        return out;
    }

    async get_account_evmtransfers(address, rows, maxRows = 1000, chainList = [], decorate = true, decorateExtra = ["data", "address", "usd", "related"], TSStart = null, pageIndex = 0) {
        let [decorateData, decorateAddr, decorateUSD, decorateRelated] = this.getDecorateOption(decorateExtra)
        let feedTransfer = [];
        let feedTransferItems = 0;
        let p = 0;
        let pTS = null;
        let prevKey = false;
        if (rows && rows.length > 0) {
            for (const row of rows) {
                let rowData = row.data
                if (rowData["feedevmtransfer"]) {
                    let [accKey, ts, transactionHash] = paraTool.parse_addressExtrinsic_rowKey(row.id)
                    let transactionsTransfer = rowData["feedevmtransfer"];
                    if (pTS != ts) {
                        p = 0;
                        pTS = ts;
                    }
                    //transfer:extrinsicHash [rows]
                    //tranfers:extrinsicHash#eventID
                    //tranfers:0x0804ea6287afaf070b7717505770da790785b0b36d34529d51b5c9670ea49cb5#5-324497-0-0 @ 2022/02/01-16:47:48.000000

                    //for each feedevmtransfer:extrinsicHash row, cell come in reverse order
                    for (const transactionHashEventID of Object.keys(transactionsTransfer).reverse()) {
                        //console.log(`extrinsicsTransfer[${extrinsicHashEventID}] length=${extrinsicsTransferCells.length}`)
                        for (const cell of transactionsTransfer[transactionHashEventID]) {
                            try {
                                let transactionHash = transactionHashEventID.split('#')[0]
                                let eventID = transactionHashEventID.split('#')[1]
                                var t = JSON.parse(cell.value);
                                if (!this.chainFilters(chainList, t.chainID)) {
                                    //filter non-specified records .. do not decorate
                                    continue
                                }
                                t['transactionHash'] = transactionHash;
                                t['blockNumber'] = parseInt(t.blockNumber, 10);
                                t['chainID'] = parseInt(t.chainID, 10);
                                t['chainName'] = this.getChainName(t["chainID"]);

                                let [__, id] = this.convertChainID(t.chainID);
                                t['id'] = id
                                if (t.ts) t['ts'] = parseInt(t.ts, 10);

                                let tt = await this.decorateQueryFeedEVMTransfer(t, t.chainID, decorate, decorateExtra)
                                let currKey = `${transactionHash}|${t.from}|${t.to}|${t.value}` // it's probably "safe" to check without asset type here.
                                if (currKey == prevKey) {
                                    console.log(`skip duplicate [${tt.eventID}] (${currKey})`)
                                } else if (TSStart && (t['ts'] == TSStart) && (p < pageIndex)) {
                                    console.log("SKIPPING (p<pageIndex)", "ts", ts, "ts0", t['ts'], "p", p, "pageIndex", pageIndex)
                                    // skip this until hitting pageIndex
                                    p++;
                                } else if (feedTransferItems < maxRows) {
                                    console.log("INCLUDING", "ts", ts, "ts0", t['ts'], "p", p, "pageIndex", pageIndex)
                                    p++;
                                    feedTransfer.push(tt);
                                    feedTransferItems++;
                                } else if (feedTransferItems == maxRows) {
                                    return {
                                        data: feedTransfer,
                                        nextPage: `/account/evmtransfers/${address}?` + this.page_params(ts, maxRows, p, chainList, decorate, decorateExtra)
                                    }
                                }
                                console.log("prevkey SET", currKey);
                                prevKey = currKey
                            } catch (err) {
                                // bad data
                                console.log(err);
                            }
                            break;
                        }
                    }
                }
            }
        }
        return {
            data: feedTransfer,
            nextPage: null
        }
    }


    // column: link
    // cell value: { title, description, metadata, linktype }
    async get_hashes_related(address, relatedData, hashesType = "address") {
        console.log("get_hashes_related", address);
        let related = [];
        if (relatedData) {
            for (const col of Object.keys(relatedData)) {
                let cell = relatedData[col];
                try {
                    let res = JSON.parse(cell[0].value)

                    related.push(res);
                    if (res.description == "Reversed H160 Address") {
                        let reverseAddr = res.title
                        related.push({
                            "datasource": "NativeH160",
                            "url": "/account/" + reverseAddr, //this redirect to NativeH160 (moonbeam/moonriver case)
                            "title": reverseAddr,
                            "description": `Native H160 Address (Moonbeam, Moonriver)`,
                            "linktype": "address",
                            "metadata": {}
                        });
                    }
                } catch (err) {
                    console.log("RELATED ERR", err);
                }
            }
        }
        if (hashesType == "address") {
            if (paraTool.isValidEVMAddress(address)) {
                // Note: Astar public key ss58 generation -> recovery original ss58 from evmAddress is not possible
                // TODO: add xrc20 generation .. if the address is a contract Address
                // Input is H160 for this address
                let evmH160 = address
                let h160SS58Pubkey = paraTool.h160ToPubkey(evmH160)
                let h160SubstrateAddr = paraTool.getAddress(h160SS58Pubkey)
                // h160SS58Pubkey is derived as first 20bytes of blake2("evm:20bytes(ss58Pubkey)), which is different from the original ss58Pubkey
                related.push({
                    "datasource": "H160",
                    "url": "/account/" + h160SS58Pubkey, // evmH160 is not directly searchable in our system. will have to redirect user to h160SS58Pubkey
                    "title": evmH160,
                    "description": `H160 Address`,
                    "linktype": "address",
                    "metadata": {}
                });
                related.push({
                    "datasource": "NativeH160",
                    "url": "/account/" + address, //this redirect to NativeH160 (moonbeam/moonriver case)
                    "title": address,
                    "description": `Native H160 Address (Moonbeam, Moonriver)`,
                    "linktype": "address",
                    "metadata": {}
                });
            }
        }
        return (related)
    }

    async getMultiAccount(rawAddresses, requestedChainID = 0, chainList = []) {
        let addresses = []
        for (let i = 0; i < rawAddresses.length; i++) {
            let x = paraTool.getPubKey(rawAddresses[i]);
            if (x) addresses.push(x);
        }
        let accounts = [];
        if (addresses.length == 0) return (accounts);
        try {
            let [tblName, tblRealtime] = this.get_btTableRealtime()
            const filter = [{
                column: {
                    cellLimit: 1
                },
                families: ["realtime", "evmcontract", "wasmcontract", "labels"],
                limit: 10,
            }];

            let [rows] = await tblRealtime.getRows({
                keys: addresses,
                filter
            });


            for (const row of rows) {
                let address = row.id;
                let rowData = row.data;
                let isEVMAddr = paraTool.isValidEVMAddress(address)
                let [assets, contract, labels] = await this.get_account_realtime(address, rowData["realtime"], rowData["evmcontract"], rowData["wasmcontract"], rowData["label"], chainList)
                let chainsMap = {};
                for (let i = 0; i < assets.length; i++) {
                    let a = assets[i];
                    let chainID = a.assetInfo.chainID;
                    if (chainsMap[chainID] == undefined) {
                        let chainInfo = this.chainInfos[chainID];
                        var id, chainName, ss58Format, ss58Address, iconUrl, subscanURL, dappURL, WSEndpoint;
                        if (chainInfo !== undefined) {
                            chainName = this.getChainName(chainID);
                            id = chainInfo.id;
                            ss58Format = this.chainInfos[chainID].ss58Format;
                            ss58Address = isEVMAddr ? false : paraTool.getAddress(address, ss58Format);
                            iconUrl = this.chainInfos[chainID].iconUrl;
                            subscanURL = this.chainInfos[chainID].subscanURL;
                            dappURL = this.chainInfos[chainID].dappURL;
                            WSEndpoint = this.chainInfos[chainID].WSEndpoint;
                        }
                        chainsMap[chainID] = {
                            chainID,
                            chainName,
                            id,
                            ss58Format,
                            ss58Address,
                            iconUrl,
                            subscanURL,
                            dappURL,
                            WSEndpoint,
                            assets: [],
                            balanceUSD: 0
                        };
                    }
                    let o = a.assetInfo;
                    o.state = a.state;
                    chainsMap[a.assetInfo.chainID].assets.push(o);
                }

                // if we didn't get any assets at all for the requestedChainID, synthesize a 0 asset record so that the user can see it
                if (chainsMap[requestedChainID] == undefined && this.chainInfos[requestedChainID] != undefined) {
                    let chainInfo = this.chainInfos[requestedChainID];
                    let chainName = this.getChainName(requestedChainID);
                    let id = chainInfo.id;
                    let ss58Format = chainInfo.ss58Format;
                    let ss58Address = isEVMAddr ? false : paraTool.getAddress(address, ss58Format);
                    let iconUrl = chainInfo.iconUrl;
                    let zeroAssets = [];
                    chainsMap[requestedChainID] = {
                        chainID: requestedChainID,
                        chainName,
                        id,
                        ss58Format,
                        ss58Address,
                        iconUrl,
                        assets: zeroAssets,
                        balanceUSD: 0
                    }
                }
                // turn chainsMap into chains array, and compute balanceUSD
                let chains = [];
                let balanceUSD = 0;
                for (const chainID of Object.keys(chainsMap)) {
                    let chain = chainsMap[chainID];
                    let chainBalanceUSD = 0;
                    for (let j = 0; j < chain.assets.length; j++) {
                        let a = chain.assets[j];
                        if (a.state.balanceUSD !== undefined && !isNaN(a.state.balanceUSD)) {
                            chainBalanceUSD += a.state.balanceUSD;
                        }
                    }
                    chain.assets.sort(function(a, b) {
                        let bBalance = (b.state.balanceUSD !== undefined) ? b.state.balanceUSD : 0;
                        let aBalance = (a.state.balanceUSD !== undefined) ? a.state.balanceUSD : 0;
                        if (aBalance != bBalance) {
                            return (bBalance - aBalance);
                        }
                        let bFree = b.state.free !== undefined ? b.state.free : 0;
                        let aFree = a.state.free !== undefined ? a.state.free : 0;
                        if (aFree != bFree) {
                            return (bFree - aFree);
                        }
                        return 0;
                    })
                    chain.balanceUSD = chainBalanceUSD;
                    balanceUSD += chain.balanceUSD;
                    chains.push(chain);
                }

                // check asc vs desc
                chains.sort(function(a, b) {
                    if (requestedChainID == a.chainID) {
                        return -1;
                    }
                    if (requestedChainID == b.chainID) {
                        return 1;
                    }
                    let bBalance = b.balanceUSD;
                    let aBalance = a.balanceUSD;
                    if (aBalance != bBalance) {
                        return (bBalance - aBalance);
                    }
                    let bAssets = b.assets.length;
                    let aAssets = a.assets.length;
                    if (aAssets != bAssets) {
                        return (bAssets - aAssets);
                    }
                    return (a.chainID - b.chainID);
                })


                let requestedChainPrefix = null;
                if (requestedChainID) {
                    requestedChainPrefix = this.getChainPrefix(requestedChainID);
                } else if (chains.length > 0) {
                    requestedChainPrefix = chains[0].ss58Format;
                } else {
                    requestedChainPrefix = 0;
                }
                let requestedChainAddress = isEVMAddr ? address : paraTool.getAddress(address, requestedChainPrefix)
                let account = {
                    address,
                    requestedChainAddress,
                    requestedChainPrefix,
                    balanceUSD,
                    chains,
                    labels,
                    numFollowing: 0,
                    numFollowers: 0,
                    isFollowing: false,
                    nickname: null,
                    info: null,
                    judgements: null,
                    infoKSM: null,
                    judgementsKSM: null,
                    related: null
                }

                let a = this.lookup_account(address);
                if (a) {
                    account.nickname = a.nickname;
                    try {
                        if (a.parentDisplay != null && a.subName) {
                            account.subName = `${a.parentDisplay}/${a.subName}`
                            account.parent = a.parent
                        } else if (a.parentDisplayKSM != null && a.subNameKSM) {
                            account.subName = `${a.parentDisplayKSM}/${a.subNameKSM}`
                            account.parent = a.parentKSM
                        } else {
                            account.subName = null;
                        }
                        account.info = (a.info != null) ? a.info : null;
                        account.judgements = (a.judgements != null) ? a.judgements : null;
                        account.infoKSM = (a.infoKSM != null) ? a.infoKSM : null;
                        account.judgementsKSM = (a.judgementsKSM != null) ? a.judgementsKSM : null;
                        account.related = (a.related != null) ? a.related : null;
                    } catch (e) {
                        console.log(e)
                    }
                    account.numFollowers = a.numFollowers;
                    account.numFollowing = a.numFollowing;
                }

                accounts.push(account);
            }
        } catch (err) {
            console.log("getMultiAccount", err);
            throw err;
        }

        return (accounts);
    }

    async get_account_realtime(address, realtimeData, evmcontractData, wasmcontractData, labelData, chainList = []) {
        let realtime = {};
        let contract = null;
        let labels = {};
        if (realtimeData) {
            let lastCellTS = {};
            for (const assetChainEncoded of Object.keys(realtimeData)) {
                let cell = realtimeData[assetChainEncoded];
                let assetChain = paraTool.decodeAssetChain(assetChainEncoded);
                let [asset, chainID] = paraTool.parseAssetChain(assetChain);
                if (!this.chainFilters(chainList, chainID)) {
                    //filter non-specified records .. do not decorate
                    continue
                }
                if (chainID !== undefined) {
                    try {
                        let assetInfo = this.assetInfo[assetChain];
                        if (assetInfo == undefined) {
                            try {
                                let assetType = "Token"; // /ERC20/ERC20LP/...
                                if (realtime[assetType] == undefined) {
                                    realtime[assetType] = [];
                                }
                                let c = JSON.parse(cell[0].value);
                                if (c.symbol && c.balance) {
                                    let chainName = this.getChainName(chainID);
                                    realtime[assetType].push({
                                        assetChain,
                                        assetInfo: {
                                            asset,
                                            chainName,
                                            symbol: c.symbol,
                                            chainID,
                                            assetName: c.symbol,
                                            assetType
                                        },
                                        state: {
                                            free: c.balance,
                                            balanceUSD: c.valueUSD
                                        }
                                    })
                                }
                            } catch (err) {
                                console.log(err)
                            }
                        } else {
                            let assetType = assetInfo.assetType;
                            if (realtime[assetType] == undefined) {
                                realtime[assetType] = [];
                            }
                            let cellTS = cell[0].timestamp / 1000000;
                            if ((lastCellTS[assetChain] == undefined) || (lastCellTS[assetChain] > cellTS)) {
                                realtime[assetType].push({
                                    assetChain,
                                    assetInfo,
                                    state: JSON.parse(cell[0].value)
                                });
                                lastCellTS[assetChain] = cellTS;
                            }
                        }
                    } catch (err) {
                        console.log("REALTIME ERR", err);
                    }
                }
            }
        }
        let totalUSDVal = await this.compute_holdings_USD(realtime);

        let current = [];
        let covered = {};
        for (const k of Object.keys(realtime)) {
            let kassets = realtime[k];
            for (let j = 0; j < kassets.length; j++) {
                if (!covered[kassets[j].assetChain]) {
                    covered[kassets[j].assetChain] = true;
                    let a = kassets[j]
                    if (a != undefined && a.state != undefined) {
                        let aState = a.state
                        if (aState.free || aState.reserved || aState.miscFrozen || aState.feeFrozen || aState.frozen || aState.supplied || aState.borrowed) {
                            current.push(kassets[j]);
                        }
                    }
                }
            }
        }
        console.log("REALTIME", realtime, "totalUSDVAL", totalUSDVal, "current", current);

        if (evmcontractData) {
            let lastCellTS = {};
            for (const chainID of Object.keys(evmcontractData)) {
                let cells = evmcontractData[chainID];
                if (!this.chainFilters(chainList, chainID)) {
                    //filter non-specified records .. do not decorate
                    continue
                }
                contract = JSON.parse(cells[0].value)
                break;
            }

        } else if (wasmcontractData) {
            let lastCellTS = {};
            for (const chainID of Object.keys(wasmcontractData)) {
                let cell = wasmcontractData[chainID];
                if (!this.chainFilters(chainList, chainID)) {
                    //filter non-specified records .. do not decorate
                    continue
                }
                contract = JSON.parse(cell[0].value)
                break;
            }
            // TODO
        } else if (labelData) {
            for (const labelType of Object.keys(labelData)) {
                let cell = labelData[labelType];
                let label = JSON.parse(cell[0].value)
                labels[labelType] = label;
                break;
            }
        }
        return [current, contract, labels];
    }


    async getBlockNumberByTS(chainID, ts, rangebackward = -1, rangeforward = 60) {
        let startTS = ts + rangebackward;
        let endTS = ts + rangeforward;
        let b0 = null;
        let b1 = null;
        let sql = `select blockNumber, blockDT, unix_timestamp(blockDT) as blockTS from block${chainID} where blockDT >= from_unixtime(${startTS}) and blockDT <= from_unixtime(${endTS}) order by blockNumber`;
        let blocks = await this.poolREADONLY.query(sql);
        for (let i = 0; i < blocks.length; i++) {
            let block = blocks[i];
            if (i == 0) {
                b0 = block.blockNumber;
            }
            if (i == blocks.length - 1) {
                b1 = block.blockNumber;
            }
        }
        return [b0, b1];
    }


    getAddressTopNFilters() {
        return [{
                filter: 'balanceUSD',
                display: "Balance USD",
                type: "currency"
            }, {
                filter: 'numChains',
                display: "# Chains",
                type: "number"
            }, {
                filter: 'numAssets',
                display: "# Assets",
                type: "number"
            }, {
                filter: 'numTransfersIn',
                display: "# Transfers In",
                type: "number"
            }, {
                filter: 'avgTransferInUSD',
                display: "Avg Transfer In (USD)",
                type: "currency"
            }, {
                filter: 'sumTransferInUSD',
                display: "Total Transfers In (USD)",
                type: "currency"
            }, {
                filter: 'numTransfersOut',
                display: "# Transfers Out",
                type: "number"
            }, {
                filter: 'avgTransferOutUSD',
                display: "Avg Transfer Out (USD)",
                type: "currency"
            }, {
                filter: 'sumTransferOutUSD',
                display: "Total Transfers Out (USD)",
                type: "currency"
            }, {
                filter: 'numExtrinsics',
                display: "# Extrinsics",
                type: "number"
            }, {
                filter: 'numExtrinsicsDefi',
                display: "# Extrinsics (Defi)",
                type: "number"
            }, {
                filter: 'numCrowdloans',
                display: "# Crowdloans",
                type: "number"
            },
            //{filter:'numSubAccounts', display: "# Subaccounts", type: "number"},
            {
                filter: 'numRewards',
                display: "# Rewards",
                type: "number"
            }, {
                filter: 'rewardsUSD',
                display: "Rewards (USD)",
                type: "currency"
            }
        ];
    }

    async getAddressTopN(topN = "balanceUSD", decorate = true, decorateExtra = ["data", "address", "usd", "related"]) {
        let topNfilters = this.getAddressTopNFilters().map((f) => (f.filter));
        if (!topNfilters.includes(topN)) {
            throw new InvalidError(`Invalid filter ${topN}`)
        }
        let [decorateData, decorateAddr, decorateUSD, decorateRelated] = this.getDecorateOption(decorateExtra)

        let sql = `select N, address, balanceUSD, val from addressTopN where topN = '${topN}' order by N asc`
        let addressTopN = await this.poolREADONLY.query(sql);
        for (let i = 0; i < addressTopN.length; i++) {
            let a = addressTopN[i];
            if (decorate) this.decorateAddress(a, "address", decorateAddr, decorateRelated)
        }
        return addressTopN;
    }

    async getAddressContract(rawAddress, chainID) {
        let address = paraTool.getPubKey(rawAddress)
        if (!this.validAddress(address)) {
            throw new paraTool.InvalidError(`Invalid address ${address}`)
        }
        let labels = [];
        let realtime = {};
        let w = (chainID) ? `and asset.chainID = '${chainID}'` : ""
        let contract = null;
        let assetChain = chainID ? paraTool.makeAssetChain(address, chainID) : "";
	{
            let families = ["realtime", "evmcontract", "wasmcontract", "label"];
            let row = false;
            contract = null;
            try {
                let [tblName, tblRealtime] = this.get_btTableRealtime()
                const filter = [{
                    column: {
                        cellLimit: 1
                    },
                    families: families,
                }];
                [row] = await tblRealtime.row(address).get({
                    filter
                });
                let rowData = row.data;
                [realtime, contract, labels] = await this.get_account_realtime(address, rowData["realtime"], rowData["evmcontract"], rowData["wasmcontract"], rowData["label"], [])
            } catch (err) {
                console.log(err);
            }
            return [realtime, contract, labels];
        }
    }


    async getAccount(rawAddress, accountGroup = "realtime", chainList = [], maxRows = 1000, TSStart = null, lookback = 180, decorate = true, decorateExtra = ["data", "address", "usd", "related"], pageIndex = 0) {
        let [decorateData, decorateAddr, decorateUSD, decorateRelated] = this.getDecorateOption(decorateExtra)
        let address = paraTool.getPubKey(rawAddress)
        if (!this.validAddress(address)) {
            throw new paraTool.InvalidError(`Invalid address ${address}`)
        }
        var hrstart = process.hrtime()
        if (accountGroup == "feed") {
            return await this.getAccountFeed(address, chainList, maxRows, decorate, decorateExtra);
        }
        // everything else comes from the "address" table, but could come from other "addressextrinsic"
        // based on the account group, figure out the source tableName and families needed from the table

        //addressextrinsic: feed, feedtransfer, crowdloans, rewards
        //addressrealtime: realtime (default)
        //addresshistory: history
        //hash related

        let tableName = "addressrealtime"
        let families = []
        let TSpagination = false;
        switch (accountGroup) {
            case "extrinsics":
                tableName = "addressextrinsic"
                families.push("feed");
                TSpagination = true;
                break;
            case "evmtxs":
                tableName = "addressextrinsic"
                families.push("feed");
                families.push("feedto"); // tmp column used to easily drop and readd
                TSpagination = true;
                break;
            case "unfinalized":
                tableName = "addressextrinsic"
                families.push("feed");
                families.push("feedunfinalized");
                TSpagination = false;
                break;
            case "transfers":
                tableName = "addressextrinsic"
                families.push("feedtransfer");
                TSpagination = true;
                break;
            case "evmtransfers":
                tableName = "addressextrinsic"
                families.push("feedevmtransfer");
                TSpagination = true;
                break;
            case "crowdloans":
                tableName = "addressextrinsic"
                families.push("feedcrowdloan");
                TSpagination = true;
                break;
            case "rewards":
                tableName = "addressextrinsic"
                families.push("feedreward");
                TSpagination = true;
                break;
            case "realtime":
                tableName = "addressrealtime"
                families.push("realtime");
                families.push("evmcontract");
                families.push("wasmcontract");
                break;
            case "ss58h160":
                tableName = "hashes"
                families.push("related");
                break;
            case "balances":
            case "history":
                tableName = "addresshistory"
                families.push("history");
                TSpagination = true;
                break;
            default:
                return false;
        }
        if (accountGroup == "balances") {
            maxRows = 1000;
        }
        //console.log(accountGroup, tableName);
        let startRow = address;
        if (TSpagination && (TSStart != null)) {
            startRow = address + "#" + paraTool.inverted_ts_key(TSStart)
        }
        try {
            let row = false,
                rows = false
            if (tableName == "addressrealtime") {
                try {
                    let [tblName, tblRealtime] = this.get_btTableRealtime()
                    const filter = [{
                        column: {
                            cellLimit: 1
                        },
                        families: families,
                        limit: maxRows,
                    }];
                    [row] = await tblRealtime.row(address).get({
                        filter
                    });

                } catch (err) {
                    if (err.code == 404) {
                        throw new paraTool.NotFoundError(`Account not found ${address}`);
                    }
                    this.logger.error({
                        "op": "query.getAccount",
                        address,
                        accountGroup,
                        err
                    });
                    return false;
                }
            } else if (tableName == "addressextrinsic") {
                let endRow = address + "#ZZZ"
                if (accountGroup == "unfinalized") {
                    endRow = address + "#" + paraTool.inverted_ts_key(this.currentTS() - 3600 * 2);
                }
                try {
                    let x = await this.btAddressExtrinsic.getRows({
                        start: startRow,
                        end: endRow,
                        limit: maxRows + 1,
                        filter: [{
                            family: families,
                            cellLimit: 1
                        }]
                    });
                    if (x.length > 0) {
                        [rows] = x;
                    }
                } catch (err) {
                    if (err.code == 404) {
                        throw Error(`Account not found ${address}`);
                    }
                    console.log(err);
                    this.logger.error({
                        "op": "query.getAccount",
                        address,
                        accountGroup,
                        err
                    });
                }
            } else if (tableName == "hashes") {

                const filter = [{
                    column: {
                        cellLimit: 1
                    },
                    limit: maxRows + 1,
                    families: families
                }];

                try {
                    [row] = await this.btHashes.row(address).get({
                        filter
                    });
                } catch (err) {
                    if (err.code == 404) {
                        //NO REASON TO throw an error, there are no "special" hashes
                    }
                    this.logger.error({
                        "op": "query.getAccount",
                        address,
                        accountGroup,
                        err
                    });
                }
            }
            switch (accountGroup) {
                case "realtime":
                    if (row) {
                        let rowData = row.data;
                        let [realtime, contract, labels] = await this.get_account_realtime(address, rowData["realtime"], rowData["evmcontract"], rowData["wasmcontract"], rowData["label"], chainList)
                        return realtime;
                    } else {
                        let [realtime, contract, labels] = await this.get_account_realtime(address, null, null, null, null, chainList)
                        return realtime;
                    }
                case "ss58h160":
                    let relatedData = false
                    if (row && row.data["related"] != undefined) {
                        relatedData = row.data["related"]
                    }
                    return await this.get_hashes_related(address, relatedData, "address")
                case "unfinalized":
                    //feed:extrinsicHash#chainID-extrinsicID
                    return await this.get_account_extrinsics_unfinalized(address, rows, maxRows, chainList, decorate, decorateExtra);
                case "evmtxs":
                case "extrinsics":
                    //feed:extrinsicHash#chainID-extrinsicID
                    return await this.get_account_extrinsics(address, rows, maxRows, chainList, decorate, decorateExtra, TSStart, pageIndex);
                case "transfers":
                    // need to also bring in "feedtransferunfinalized" from the same table
                    //feedtransfer:extrinsicHash#eventID
                    return await this.get_account_transfers(address, rows, maxRows, chainList, decorate, decorateExtra, TSStart, pageIndex);
                case "evmtransfers":
                    //feedtransfer:transactionHash#eventID
                    return await this.get_account_evmtransfers(address, rows, maxRows, chainList, decorate, decorateExtra, TSStart, pageIndex);
                case "crowdloans":
                    //feedcrowdloan:extrinsicHash#eventID
                    return await this.get_account_crowdloans(address, rows, maxRows, chainList, decorate, decorateExtra, TSStart, pageIndex);
                case "rewards":
                    //feedreward:extrinsicHash#eventID
                    return await this.get_account_rewards(address, rows, maxRows, chainList, decorate, decorateExtra, TSStart, pageIndex);
                case "history":
                    let relatedExtrinsicsMap = {}
                    let hist = await this.get_account_history(address, rows, maxRows, chainList, false)
                    try {
                        //MK: history has family "history" but we are calling "feed" family here??
                        let [extrinsics] = await this.btAddressExtrinsic.getRows({
                            start: startRow,
                            end: address + "#" + paraTool.inverted_ts_key(hist.minTS) + "#ZZZ",
                            filter: [{
                                //family: families,
                                family: ["feed"],
                                cellLimit: 1
                            }],
                            limit: maxRows
                        });
                        if (extrinsics && extrinsics.length > 0) {
                            for (const ext of extrinsics) {
                                let [addressPiece, ts, extrinsicHashPiece] = paraTool.parse_addressExtrinsic_rowKey(ext.id)
                                let out = {};
                                let rowData = ext.data
                                if (rowData["feed"]) {
                                    let extrinsics = rowData["feed"];
                                    for (const extrinsicHashEventID of Object.keys(extrinsics)) {
                                        for (const cell of extrinsics[extrinsicHashEventID]) {
                                            let t = JSON.parse(cell.value);
                                            // here we copy just a FEW of the flds over for recognitions sake
                                            let flds = ["chainID", "blockNumber", "extrinsicHash", "extrinsicID", "section", "method"];
                                            for (const fld of flds) {
                                                if (t[fld] !== undefined) {
                                                    if (fld == "chainID" || fld == "blockNumber") {
                                                        out[fld] = parseInt(t[fld], 10); // can we avoid this step?
                                                    } else {
                                                        out[fld] = t[fld];
                                                    }

                                                }
                                            }
                                        }
                                    }
                                }
                                relatedExtrinsicsMap[ts] = out;
                            }
                        }
                    } catch (err) {
                        this.logger.error({
                            "op": "query.getAccount",
                            address,
                            accountGroup,
                            err
                        });
                    }
                    // console.log("*** relatedExtrinsicMap ** ", relatedExtrinsicsMap);
                    let out = [];
                    for (const assetChain of Object.keys(hist.data)) {
                        let h = hist.data[assetChain];
                        let states = hist.data[assetChain].states;
                        for (let i = 0; i < states.length; i++) {
                            // each of these is a pair [indexTS, state] ... but we can push a POTENTIAL extrinsicHash IF they happen to be the SAME ts
                            if (states[i].length == 2) {
                                let [indexTS, _] = states[i];
                                if (relatedExtrinsicsMap[indexTS] !== undefined) {
                                    states[i].push(this.clean_extrinsic_object(relatedExtrinsicsMap[indexTS]));
                                }
                            }
                        }
                        out.push(h);
                    }
                    return {
                        data: out,
                            nextPage: hist.nextPage
                    };
                case "balances":
                    let historyObj = await this.get_account_history(address, rows, maxRows, chainList, true);
                    let h = historyObj.data
                    let balances = [];
                    let currentTS = this.currentTS();
                    let startTS = currentTS - 86400 * lookback;
                    for (let ts = startTS; ts <= currentTS; ts += 86400) {

                        let totalUSDVal = 0;
                        for (const assetChain of Object.keys(h)) {
                            let assetInfo = this.assetInfo[assetChain];
                            if (assetInfo !== undefined) {
                                let assetType = assetInfo.assetType;
                                let chainID = assetInfo.chainID;
                                let flds = this.get_assetType_flds(assetType);
                                let states = h[assetChain].states; // each of these is a pair [indexTS, state]
                                let state = false;
                                for (let i = 0; i < states.length; i++) {
                                    if (states[i].length == 2) {
                                        let [indexTS, stateAt] = states[i];
                                        if (ts >= indexTS && (state == false)) {
                                            state = stateAt;
                                            // can we break here?
                                        }
                                    }
                                }
                                if (state) {
                                    let USDval = await this.decorate_assetState(assetInfo, state, flds, ts);
                                    totalUSDVal += USDval
                                }
                            } else {
                                console.log("failed to find: ", assetChain);
                            }
                        }

                        balances.push([ts * 1000, totalUSDVal]);
                    }
                    return (balances);
                default:
                    return false;
                    break;
            }
        } catch (err) {
            if (err instanceof paraTool.InvalidError || err instanceof paraTool.NotFoundError) {
                throw err
            }
            console.log(err);
            this.logger.error({
                "op": "query.getAccount",
                address,
                accountGroup,
                err
            });
            return {};
        }
    }

    validAddress(address) {
        if (!address) return false;
        if (address.length == 66) return true;
        if (address.length == 42) return true;
        return false;
    }

    async getRealtimeAsset(rawAddress) {
        const maxRows = 1000;
        let address = paraTool.getPubKey(rawAddress)
        if (!this.validAddress(address)) {
            throw new paraTool.InvalidError(`Invalid address ${address}`)
        }
        var hrstart = process.hrtime()
        try {
            console.log("reading: ", rawAddress);
            let [tblName, tblRealtime] = this.get_btTableRealtime()
            const filter = [{
                column: {
                    cellLimit: 1
                },
                families: [
                    "realtime"
                ],
                limit: maxRows
            }];

            const [row] = await tblRealtime.row(address).get({
                filter
            });

            let rowData = row.data;
            let account = {};
            const realtimeData = rowData["realtime"];
            let realtime = {};
            if (realtimeData) {
                for (const assetChainEncoded of Object.keys(realtimeData)) {
                    let cell = realtimeData[assetChainEncoded];
                    let assetChain = paraTool.decodeAssetChain(assetChainEncoded);
                    let [asset, chainID] = paraTool.parseAssetChain(assetChain);
                    if (chainID !== undefined) {
                        try {
                            let assetInfo = this.assetInfo[assetChain];
                            if (assetInfo == undefined) {
                                // console.log("NO ASSETINFO", assetChain, "asset", asset, "chainID", chainID, cell[0].value);
                            } else {
                                let assetType = assetInfo.assetType;
                                if (realtime[assetType] == undefined) {
                                    realtime[assetType] = [];
                                }
                                realtime[assetType].push({
                                    assetInfo,
                                    state: JSON.parse(cell[0].value)
                                });
                            }
                        } catch (err) {
                            console.log("REALTIME ERR", err);
                        }
                    }
                }
            }
            await this.compute_holdings_USD(realtime);
            return (realtime);
        } catch (err) {
            if (err.code == 404) {
                throw Error("Account not found");
            }
            console.log(err);
            this.logger.error({
                "op": "query.getRealtimeAsset",
                address,
                err
            });
        }
        return (false);
    }

    decorateEventModule(evt, decorate = true, decorateData = true) {
        try {
            let [section, method] = paraTool.parseSectionMethod(evt)
            let nEvent = {}
            nEvent.eventID = evt.eventID
            if (decorate && decorateData) nEvent.docs = evt.docs
            nEvent.section = evt.section
            nEvent.method = evt.method
            nEvent.data = evt.data
            if (decorate && decorateData) nEvent.dataType = evt.dataType // returning dataType for now?
            return nEvent
        } catch (err) {
            this.logger.error({
                "op": "query.decorateEventModule",
                evt,
                err
            });
        }
    }

    async decorateParams(section, method, args, chainID, ts, depth = 0, decorate = true, decorateExtra = ["data", "address", "usd"]) {
        this.chainParserInit(chainID, this.debugLevel);
        let [decorateData, decorateAddr, decorateUSD, decorateRelated] = this.getDecorateOption(decorateExtra)
        try {
            if (section == 'ethereum' && method == 'transact') {
                if (args.transaction != undefined) {
                    let evmTx = false;
                    if (args.transaction.eip1559 != undefined) {
                        evmTx = args.transaction.eip1559
                    } else if (args.transaction.legacy != undefined) {
                        evmTx = args.transaction.legacy
                    }
                    if (args.transaction.v1 != undefined) {
                        evmTx = args.transaction.v1
                    }
                    if (args.transaction.v2 != undefined) {
                        evmTx = args.transaction.v2
                    }
                    if (decorate && evmTx) {
                        let output = this.decodeTransactionInput(evmTx)
                        if (output != undefined) {
                            args.decodedEvmInput = output
                        }
                    }
                }
            }
            if (args.other_signatories != undefined) {
                if (decorate) this.decorateAddresses(args, "other_signatories", decorateAddr, false) // ignore here?
            }
            if (args.real != undefined) {
                let address = paraTool.getPubKey(args.real)
                if (address) {
                    args.realAddress = address
                    if (decorate) this.decorateAddress(args, "realAddress", decorateAddr, false)
                }
            }
            if (args.calls != undefined) { // this is an array
                //console.log(depth, "descend into calls", args.calls.length)
                let i = 0;
                for (const c of args.calls) {
                    let call_section = c.section;
                    let call_method = c.method;
                    //console.log(depth, "call ", i , call_section, call_method, c);
                    i++;
                    await this.decorateParams(call_section, call_method, c.args, chainID, ts, depth + 1, decorate, decorateExtra)
                }
            } else if (args.call != undefined) { // this is an object
                let call = args.call
                let call_section = call.section;
                let call_method = call.method;
                //console.log(depth, "descend into call", call)
                await this.decorateParams(call_section, call_method, call.args, chainID, ts, depth + 1, decorate, decorateExtra)
            } else {
                let pallet_method = `${section}:${method}`
                //console.log(depth, "leaf", pallet_method, args)
                await this.chainParser.decorate_query_params(this, pallet_method, args, chainID, ts, 0, decorate, decorateExtra)
            }
        } catch (err) {
            console.log(err);
            this.logger.error({
                "op": "query.decorateParams",
                section,
                method,
                args,
                chainID,
                err
            });
        }
    }

    async decorateFee(extrinsic, chainID, decorateUSD = true) {
        try {
            var chainSymbol = this.getChainSymbol(chainID)
            var chainDecimals = this.getChainDecimal(chainID)
            var fee = (extrinsic.fee != undefined) ? (extrinsic.fee) : 0
            var tip = (extrinsic.tip != undefined) ? (extrinsic.tip) : 0
            var targetAsset = `{"Token":"${chainSymbol}"}`
            extrinsic.chainSymbol = chainSymbol
            if (decorateUSD) {
                let p = await this.computePriceUSD({
                    asset: targetAsset,
                    chainID,
                    ts: extrinsic.ts
                });
                if (p) {
                    extrinsic.priceUSD = p.priceUSD
                    extrinsic.feeUSD = fee * p.priceUSD
                    extrinsic.tipUSD = tip * p.priceUSD
                    extrinsic.priceUSDCurrent = p.priceUSDCurrent
                }
            }
        } catch (err) {
            this.logger.error({
                "op": "query.decorateFee",
                extrinsic,
                chainID,
                err
            });

        }
    }


    async decorateQueryFeedEVMTransfer(feedtransfer, chainID, decorate = true, decorateExtra = ["data", "address", "usd", "related"]) {
        let [decorateData, decorateAddr, decorateUSD, decorateRelated] = this.getDecorateOption(decorateExtra)
        var value = feedtransfer.value
        var assetSymbol = null;
        var decimals = null;
        let dFeedtransfer = {
            chainID: feedtransfer.chainID,
            chainName: feedtransfer.chainName,
            id: feedtransfer.id,
            blockNumber: feedtransfer.blockNumber,
            ts: feedtransfer.ts,
            transactionHash: feedtransfer.transactionHash,
            from: feedtransfer.from,
            to: feedtransfer.to,
            tokenAddress: feedtransfer.tokenAddress,
            valueRaw: value
        }
        let asset = feedtransfer.tokenAddress.toLowerCase();
        let assetChain = paraTool.makeAssetChain(asset, chainID);
        let assetInfo = this.assetInfo[assetChain];
        if (assetInfo != undefined) {
            dFeedtransfer.decimals = assetInfo.decimals;
            dFeedtransfer.value = dFeedtransfer.valueRaw / 10 ** assetInfo.decimals;
            dFeedtransfer.symbol = assetInfo.symbol;
            if (decorateUSD) {
                let p = await this.computePriceUSD({
                    val: dFeedtransfer.value,
                    asset,
                    chainID,
                    ts: feedtransfer.ts
                })
                if (p) {
                    dFeedtransfer.valueUSD = p.valUSD
                    dFeedtransfer.priceUSD = p.priceUSD
                    dFeedtransfer.priceUSDCurrent = p.priceUSDCurrent
                }
            }
            console.log("DECORATE", decorateUSD, asset, dFeedtransfer);
        } else {
            console.log("MISSING", assetChain, feedtransfer.tokenAddress.toLowerCase(), chainID);
        }
        if (decorate) {
            if (dFeedtransfer.from != undefined) {
                //if (decorate) this.decorateAddress(dFeedtransfer, "fromAddress", decorateAddr, decorateRelated)
            }
            if (dFeedtransfer.to != undefined) {
                // if (decorate) this.decorateAddress(dFeedtransfer, "toAddress", decorateAddr, decorateRelated)
            }
        }
        return dFeedtransfer
    }

    // input: 1642608001
    // output: 1642608000
    hourly_key_from_ts(ts) {
        let out = Math.round(ts / 3600) * 3600;
        return (out.toString());
    }


    async getChainLog(chainID_or_chainName, lookback = 90) {
        let [chainID, id] = this.convertChainID(chainID_or_chainName)
        if (chainID === false) {
            throw new paraTool.InvalidError(`Invalid chain: ${chainID_or_chainName}`)
        }
        try {
            var sql = `select logDT, UNIX_TIMESTAMP(logDT) as logTS, startBN, endBN,  numTransactionsEVM, numTransactionsEVM1559, numTransactionsEVMLegacy, numEVMContractsCreated, numActiveAccounts, numNewAccounts, gasUsed, gasLimit, gasPrice, maxFeePerGas, maxPriorityFeePerGas from blocklog where chainID = '${chainID}' and logDT >= date_sub(Now(), interval ${lookback} DAY) and logDT >= '2023-01-01' order by logDT desc`;
            let recs = await this.poolREADONLY.query(sql);
            for (let i = 0; i < recs.length; i++) {
                let [logDT, _] = paraTool.ts_to_logDT_hr(recs[i].logTS);
                recs[i].logDT = logDT;
                recs[i].fees = parseFloat(recs[i].fees);
            }
            return (recs);
        } catch (err) {
            this.logger.error({
                "op": "query.getChainLog",
                chainID,
                err
            });
        }
        return false;
    }


    chainFilters(chainList = [], targetChainID) {
        if (targetChainID == undefined) return false
        if (isNaN(targetChainID)) return false
        let chainID = paraTool.dechexToInt(targetChainID, 10)
        if (chainList.length == 0) {
            return true
        } else if (chainList.includes(chainID)) {
            return true
        }
        return false
    }

    getSS58ByChainID(destAddress, chainID = 0) {
        let ss58Address = false
        if (!destAddress) return false
        if (destAddress.length == 42) {
            ss58Address = destAddress
        } else if (destAddress.length == 66) {
            let chainIDDestInfo = this.chainInfos[chainID]
            if (chainIDDestInfo.ss58Format != undefined) {
                ss58Address = paraTool.getAddress(destAddress, chainIDDestInfo.ss58Format)
            } else {
                ss58Address = paraTool.getAddress(destAddress, 42) // default
            }
        }
        return ss58Address
    }

    async get_sourcify_evmcontract(asset) {
        const axios = require("axios");
        // check https://sourcify.dev/server/check-by-addresses?addresses=0x3a7798ca28cfe64c974f8196450e1464f43a0d1e&chainIds=1284,1285,1287
        try {
            let url = `https://sourcify.dev/server/check-by-addresses?addresses=${asset}&chainIds=1284,1285,1287`
            let response = await axios.get(url)
            response = response.data;
            for (let i = 0; i < response.length; i++) {
                let r = response[i];
                // if status is false, give up
                if (!r.status || (r.status === "false") || r.status === false) {
                    // TODO: store miss in abiRaw and only call sourcify again if addDT is more than 1-5 mins ago
                    return null;
                }
                // for any chainIDs in the response, get the files {"address": "0x3A7798CA28CFE64C974f8196450e1464f43A0d1e","status": "perfect","chainIds": ["1287"]}
                let abiraw = [];
                let contractCode = [];
                let contract = {
                    code: {}
                };
                for (const evmchainID of r.chainIds) {
                    let chainID = ethTool.evmChainIDToChainID(evmchainID) // e.g moonbeam 1284 => 2004
                    if (chainID) {
                        let files = 0;
                        let codeResult = {}
                        let filesUrl = `https://sourcify.dev/server/files/tree/${evmchainID}/${asset}`
                        let filesResponse = await axios.get(filesUrl)
                        for (const fileurl of filesResponse.data) {
                            let f = await axios.get(fileurl)
                            console.log(fileurl, f.data);
                            if (f.data && f.data.output != undefined && f.data.output.abi != undefined) {
                                let flds = ["compiler", "language", "settings", "sources"]
                                for (const fld of flds) {
                                    if (f.data[fld] != undefined) {
                                        contract[fld] = f.data[fld];
                                    }
                                }
                                contract.ABI = f.data.output.abi
                                abiraw.push(`('${asset}', '${chainID}', ${mysql.escape(JSON.stringify(contract.ABI))})`)
                            } else {
                                codeResult[fileurl] = f.data;
                                files++;
                            }
                        }
                        if (files > 0) {
                            let y = {
                                result: [codeResult]
                            }
                            contractCode.push(`('${asset}', '${chainID}', ${mysql.escape(JSON.stringify(y))}, Now())`)
                        }
                        contract.chainID = chainID;
                    } else {

                        // TODO: store miss in abiRaw and only call sourcify again if addDT is more than 1-5 mins ago
                    }
                }
                let vals = ["abiraw"];
                await this.upsertSQL({
                    "table": "asset",
                    "keys": ["asset", "chainID"],
                    "vals": vals,
                    "data": abiraw,
                    "replace": vals
                });
                let vals2 = ["code", "addDT"];
                await this.upsertSQL({
                    "table": "contractCode",
                    "keys": ["asset", "chainID"],
                    "vals": vals2,
                    "data": contractCode,
                    "replace": vals2
                });
                return contract;
            }
        } catch (err) {
            console.log(err);
            return null
        }
    }

    async getEVMContract(asset, chainID = null) {
        try {
            let assetChain = (chainID) ? paraTool.makeAssetChain(asset, chainID) : "";
            let where_asset = (this.xcContractAddress[asset] != undefined) ? `asset.xcContractAddress = '${asset}'` : `asset.asset = '${asset}'`
            let w = (chainID) ? ` and asset.chainID = '${chainID}'` : "";
            let sql = `select asset.asset, asset.assetType, asset.chainID, convert(asset.abiRaw using utf8) as ABI, convert(contractCode.code using utf8) code, asset.xcContractAddress, addDT from asset left join contractCode on contractCode.asset = asset.asset and contractCode.chainID = asset.chainID where ( ${where_asset} ) ${w} order by addDT desc limit 1`
            let evmContracts = await this.poolREADONLY.query(sql);
            if (evmContracts.length == 0) {
                // return not found error
                let evmContract = await this.get_sourcify_evmcontract(asset, chainID);

                throw new paraTool.NotFoundError(`EVM Contract not found: ${asset}`)
                return (false);
            }
            let evmContract = evmContracts[0];
            if (this.xcContractAddress[asset] != undefined) {
                evmContract.asset = evmContract.xcContractAddress;
            }
            let [_, id] = this.convertChainID(evmContract.chainID)
            let chainName = this.getChainName(evmContract.chainID);
            evmContract.id = id;
            evmContract.chainName = chainName;
            try {
                let code = JSON.parse(evmContract.code);
                if (code && code.result && Array.isArray(code.result) && code.result.length > 0) {
                    let result = code.result[0];
                    evmContract.code = {};
                    for (const key of Object.keys(result)) {
                        evmContract.code[key] = result[key];
                    }
                    //evmContract.ABI = JSON.parse(evmContract.ABI);
                }
            } catch (err) {
                console.log(err);
                delete evmContract.ABI
            }
            try {
                if (evmContract.ABI && evmContract.ABI.length > 0) {
                    let abiRaw = JSON.parse(evmContract.ABI);
                    if (abiRaw.result != undefined) {
                        abiRaw = abiRaw.result;
                    }
                    if (typeof abiRaw == "string") {
                        abiRaw = JSON.parse(abiRaw);
                    }
                    if (abiRaw.length > 0) {
                        evmContract.ABI = abiRaw;
                    }
                }
                if (Array.isArray(evmContract.ABI) && (evmContract.ABI.length > 0)) {

                } else {
                    evmContract.ABI = ethTool.getABIByAssetType(evmContract.assetType)
                }
            } catch (err) {
                console.log(err);
                delete evmContract.ABI
            }
            return evmContract;
        } catch (err) {
            console.log(err)
        }
    }


    canonicalize_chainfilters(chainfilters) {
        if (typeof chainfilters == "string") {
            if (chainfilters == "all") return [];
            chainfilters = chainfilters.split(",");
        }
        let out = [];
        for (let i = 0; i < chainfilters.length; i++) {
            let [chainID, id] = this.convertChainID(chainfilters[i]);

            if (id) {
                out.push(chainID);
            } else {
                return null
            }
        }
        return out;
    }
}
