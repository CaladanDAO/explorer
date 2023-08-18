// Copyright 2023 Colorful Notion, Inc.
// This file is part of CaladanDAO Block Explorer.

// Polkaholic is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// The CaladanDAO Block Explorer code is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with the CaladanDAO Block Explorer.  If not, see <http://www.gnu.org/licenses/>.

const paraTool = require("./paraTool");
const uiTool = require("./uiTool");
const mysql = require("mysql2");
const {
    ApiPromise
} = require('@polkadot/api');
const MAX_PRICEUSD = 100000.00;
const EvmManager = require("./evmManager");

module.exports = class AssetManager extends EvmManager {
    assetInfo = {};
    alternativeAssetInfo = {};
    symbolRelayChainAsset = {}; // symbolRelayChain -> { ${chainID}: assetInfo }
    assetlog = {};
    ratelog = {};
    routers = {};
    assetlogTTL = 0;
    ercTokenList = {};
    currencyIDInfo = {};
    storageKeys = {};
    metadata = {};

    skipStorageKeys = {};
    accounts = {};
    chainParser = null; // initiated by setup_chainParser (=> chainParserInit)
    chainParserChainID = null;
    apiParser = null;


    lastEventReceivedTS = 0;
    constructor(debugLevel = false) {
        super()
    }

    getChainPrefix(chainID) {
        if (this.chainInfos[chainID] != undefined) {
            let ss58Format = this.chainInfos[chainID].ss58Format;
            return ss58Format
        }
        return 42 //default substrate
    }

    CDPStringUnify(assetString) {
        let s = assetString.replace('CDP_Borrow', 'CDP');
        return s.replace('CDP_Supply', 'CDP');
    }

    convertChainID(chainID_or_chainName) {
        let chainID = false
        let id = false
        try {
            chainID = parseInt(chainID_or_chainName, 10);
            if (isNaN(chainID)) {
                [chainID, id] = this.getChainIDByName(chainID_or_chainName)
            } else {
                [chainID, id] = this.getNameByChainID(chainID_or_chainName)
            }
        } catch (e) {
            [chainID, id] = this.getChainIDByName(chainID_or_chainName)
        }
        console.log(`chainID=${chainID}, id=${id}, chainID_or_chainName=${chainID_or_chainName}`)
        return [chainID, id]
    }

    async getBlockHashFinalized(chainID, blockNumber) {
        let sql = `select blockHash, if(blockDT is Null, 0, 1) as finalized from block${chainID} where blockNumber = '${blockNumber}' and blockDT is not Null`
        let blocks = await this.poolREADONLY.query(sql);
        if (blocks.length == 1) {
            return blocks[0].blockHash;
        }
    }

    async autoRefreshAssetManager(crawler) {
        await crawler.assetManagerInit();
        if (crawler.web3Api) {
            console.log("autoRefresh contractABI...")
            crawler.contractABIs = await crawler.getContractABI();
        }
    }

    async selfTerminate(crawler) {
        if (crawler.getCurrentTS() - crawler.lastEventReceivedTS > 300) {
            console.log("No event received in 5mins, terminating")
            process.exit(1);
        }
    }

    async chainParserInit(chainID, debugLevel = 0) {
        if (debugLevel >= paraTool.debugVerbose) console.log(`chainParserInit chainID=${chainID}, this.chainParserChainID=${this.chainParserChainID}`)
        if (this.chainParser && (this.chainParserChainID == chainID)) {
            return;
        }
        if ([paraTool.chainIDKarura, paraTool.chainIDAcala].includes(chainID)) {
            this.chainParser = new AcalaParser();
        } else if ([paraTool.chainIDBifrostDOT, paraTool.chainIDBifrostKSM].includes(chainID)) {
            this.chainParser = new BifrostParser();
        } else if ([paraTool.chainIDAstar, paraTool.chainIDShiden, paraTool.chainIDShibuya].includes(chainID)) {
            this.chainParser = new AstarParser();
        } else if (chainID == paraTool.chainIDParallel || chainID == paraTool.chainIDHeiko) {
            this.chainParser = new ParallelParser();
            //await this.chainParser.addCustomAsset(this); // This line add psuedo asset HKO/PARA used by dex volume / LP pair / etc ..
        } else if ([paraTool.chainIDMoonbeam, paraTool.chainIDMoonriver, paraTool.chainIDMoonbaseAlpha, paraTool.chainIDMoonbaseBeta].includes(chainID)) {
            this.chainParser = new MoonbeamParser();
        } else if ([paraTool.chainIDInterlay, paraTool.chainIDKintsugi].includes(chainID)) {
            this.chainParser = new InterlayParser();
        } else {
            this.chainParser = new ChainParser();
        }
        if (this.chainParser) {
            this.chainParserChainID = chainID;
            if (debugLevel >= paraTool.debugVerbose) console.log(`this.chainParserChainID chainID=${chainID}, chainParserName=${this.chainParser.chainParserName}`)
        }
        if (debugLevel > 0) {
            this.chainParser.setDebugLevel(debugLevel)
        }
    }

    async get_chainSymbols() {
        let chainSQL = `select chainID, symbol from chain where symbol is not null`
        var chains = await this.poolREADONLY.query(chainSQL);
        let nativeSymbolMap = {}
        for (const chain of chains) {
            nativeSymbolMap[chain.chainID] = chain.symbol
        }
        return nativeSymbolMap
    }

    // reads all the decimals from the chain table and then the asset mysql table
    async init_chainInfos() {
        let chainSQL = `select id, chain.chainID, chain.chainName, chain.relayChain, chain.paraID, ss58Format, isEVM, chain.iconUrl,
 githubURL, subscanURL, parachainsURL, dappURL, WSEndpoint
 from chain where ( (crawling = 1 or chain.paraID > 0) and id is not null )`
        var chains = await this.poolREADONLY.query(chainSQL);
        let chainInfoMap = {}
        let chainNameMap = {}
        for (const chain of chains) {
            let decimals = parseInt(chain.decimals, 10)
            let paraID = parseInt(chain.paraID, 10)
            let chainName = (chain.chainName != undefined) ? this.capitalizeFirstLetter(chain.chainName) : null
            let info = {
                id: chain.id,
                iconUrl: chain.iconUrl,
                chainID: chain.chainID,
                name: chainName,
                decimal: decimals,
                asset: chain.asset,
                symbol: chain.symbol,
                ss58Format: chain.ss58Format,
                priceUSD: (chain.priceUSD != undefined) ? chain.priceUSD : 0,
                priceUSDPercentChange: (chain.priceUSDPercentChange != undefined) ? chain.priceUSDPercentChange : 0,
                relayChain: chain.relayChain,
                paraID: paraID,
                isEVM: (chain.isEVM == 1) ? true : false,
                githubURL: chain.githubURL,
                //subscanURL: chain.subscanURL,
                subscanURL: null,
                parachainsURL: chain.parachainsURL,
                dappURL: chain.dappURL,
                WSEndpoint: chain.WSEndpoint
            } //use the first decimal until we see an "exception"
            chainInfoMap[chain.chainID] = info
            chainNameMap[chain.id] = info
        }

        this.chainInfos = chainInfoMap
        this.chainNames = chainNameMap
    }

    async assetManagerInit() {
        await this.init_chainInfos()
        await this.init_chain_asset_and_nativeAsset()
        return (true);
    }

    async init_storage_keys() {
        this.storageKeys = {}
    }

    async init_accounts() {
        this.accounts = {};
    }

    lookup_account(address) {
        if (this.accounts[address] != undefined) {
            return (this.accounts[address]);
        }
        let asciiName = paraTool.pubKeyHex2ASCII(address);
        if (asciiName != null) {
            return ({
                nickname: asciiName,
                verified: true
            });
        }
        return (null)
    }

    lookup_trace_sectionStorage(inpk, inpv) {
        let o = {}
        if (!inpk) return ([null, null]);
        let key = inpk.slice()
        if (key.substr(0, 2) == "0x") key = key.substr(2)
        let k = key.slice();
        if (k.length > 64) k = k.substr(0, 64);
        let sk = this.storageKeys[k];
        if (sk != undefined) {
            return [sk.palletName, sk.storageName];
        }
        return ([null, null]);

    }

    async get_skipStorageKeys() {
        if (Object.keys(this.skipStorageKeys).length > 0) return;
        this.skipStorageKeys = {};
        var storageKeysList = await this.poolREADONLY.query(`select palletName, storageName, storageKey from chainPalletStorage where skip = 1`);
        if (storageKeysList.length > 0) {
            for (const sk of storageKeysList) {
                this.skipStorageKeys[`${sk.storageKey}`] = sk;
            }
        }
    }

    async init_paras() {
        let paras = await this.poolREADONLY.query(`select id, chainID, chainName, relayChain, paraID, concat(relayChain,'-',paraID) as fullparaID, symbol from chain order by relayChain desc, chainID;`);
        let paraMap = {}
        for (const p of paras) {
            paraMap[p.fullparaID] = p
            //this.paras[p.fullparaID] = p
        }
        this.paras = paraMap
    }

    async init_chain_asset_and_nativeAsset() {
        // return cached version
        let currTS = this.getCurrentTS();

        // reload assetInfo
        await this.init_asset_info()

        return true
    }
    async init_xcm_asset() {
    }


    getNativeChainAsset(chainID) {
        // TODO check
        let asset = this.getChainAsset(chainID)
        if (asset == null) return null
        let nativeAssetChain = paraTool.makeAssetChain(asset, chainID);
        //console.log(`Convert to nativeAssetChain ${chainID} -> ${nativeAssetChain}`)
        return nativeAssetChain
    }


    async init_asset_info() {
    }


    getNativeAsset(chainID) {
        let symbol = this.getChainSymbol(chainID);
        if (symbol) {
            return JSON.stringify({
                Token: symbol
            })
        } else {
            return (false);
        }
    }

    getNativeSymbol(chainID) {
        let symbol = this.getChainSymbol(chainID);
        if (symbol) {
            return symbol
        } else {
            return (false);
        }
    }

    getRelayChainSymbol(chainID) {
        let relayChain = paraTool.getRelayChainByChainID(chainID)
        let relayChainID = paraTool.getRelayChainID(relayChain)
        let symbol = this.getChainSymbol(relayChainID);
        if (symbol) {
            return symbol
        } else {
            return (false);
        }
    }


    //todo: should this function be async. If so, how to handle it in txparams.ejs?
    getAssetDecimal(asset, chainID, ctx = "false") {
        let assetChain = paraTool.makeAssetChain(asset, chainID);
        if (this.assetInfo[assetChain] != undefined) {
            return this.assetInfo[assetChain].decimals
        } else {
            //console.log("getAssetDecimal MISS", "CONTEXT", ctx, "assetString", assetString);
            return (false);
        }
    }

    getAssetSymbol(asset, chainID, ctx = "false") {
        let assetChain = paraTool.makeAssetChain(asset, chainID);
        if (this.assetInfo[assetChain] != undefined) {
            return this.assetInfo[assetChain].symbol
        } else {
            //console.log("getAssetDecimal MISS", "CONTEXT", ctx, "assetString", assetString);
            return (false);
        }
    }

    getAssetByCurrencyID(currencyID, chainID) {
        let currencyChain = paraTool.makeAssetChain(currencyID, chainID);
        if (this.currencyIDInfo[currencyChain] !== undefined) {
            let assetInfo = this.currencyIDInfo[currencyChain];
            return assetInfo
        }
        return false
    }

    getCurrencyIDDecimal(currencyID, chainID) {
        let currencyChain = paraTool.makeAssetChain(currencyID, chainID);
        if (this.currencyIDInfo[currencyChain] !== undefined) {
            let assetInfo = this.currencyIDInfo[currencyChain];
            if (assetInfo.decimals != undefined) {
                return assetInfo.decimals
            }
        }
        return (false);
    }

    getCurrencyIDSymbol(currencyID, chainID) {
        let currencyChain = paraTool.makeAssetChain(currencyID, chainID);
        if (this.currencyIDInfo[currencyChain] !== undefined) {
            let assetInfo = this.currencyIDInfo[currencyChain];
            if (assetInfo.symbol != undefined && assetInfo.symbol) {
                return assetInfo.symbol
            }
        }
        return (false);
    }


    getDecorateOption(decorateExtra) {
        if (Array.isArray(decorateExtra)) {
            let decorateData = decorateExtra.includes("data")
            let decorateAddr = decorateExtra.includes("address")
            let decorateUSD = decorateExtra.includes("usd")
            let decorateRelated = decorateExtra.includes("related")
            // TODO: deep events/logs let decorateDeep = decorateExtra.includes("deep")                                                                                       
	    return [decorateData, decorateAddr, decorateUSD, decorateRelated]
        } else if (decorateExtra == true) {
            // remove this once ready                                                                                                                                         
            return [true, true, true, true]
        } else if (decorateExtra == false) {
            return [true, true, true, false]
        } else {
            //return nothing if user purposefully pass in non-matched filter                                                                                                  
            return [false, false, false, false]
        }
    }


    async decorateEvent(event, chainID, ts, decorate = true, decorateExtra = ["data", "address", "usd", "related"], isUI = true) {
    }

    async decorate_assetState(assetInfo, state, flds, ts) {
        return 0; // TODO totalUSDVal;
    }

    filterRelated(related) {
        return related.filter((r) => {
            if (r.accountType == undefined) return false;
            if (r.accountType == "proxyDelegateOf") return true;
            if (r.accountType == "multisig") return true;
            return false;
        })
    }

    decorateAddresses(c, fld, decorateAddr = true, decorateRelated = true) {
        let res = [];
        let nhits = 0;
        try {
            if (!decorateAddr && !decorateRelated) return (false)
            if (c[fld] == undefined) return (false);
            if (!Array.isArray(c[fld])) return (false);

            for (const id of c[fld]) {
                // nickname, judgements, info, judgementsKSM, infoKSM, verified, verifyDT, numFollowers, numFollowing
                let address = paraTool.getPubKey(id);
                let hit = false;
                let o = {};
                if (address) {
                    let a = this.lookup_account(address);
                    if (a == null) {

                    } else {
                        if (decorateAddr) {
                            if (a.nickname != null && a.verified > 0) {
                                o['nickname'] = a.nickname;
                                hit = true;
                            }
                            if (a.info != null) {
                                o['info'] = a.info;
                                o['judgements'] = a.judgements;
                                hit = true;
                            } else if (a.infoKSM != null) {
                                o['info'] = a.infoKSM;
                                o['judgements'] = a.judgementsKSM;
                                hit = true;
                            }
                        }
                        if (a.related != undefined && decorateRelated) {
                            o['related'] = this.filterRelated(a.related);
                        }
                    }
                }
                res.push(o);
                if (hit) {
                    nhits++;
                }
            }
        } catch (e) {
            console.log(`decorateAddress err`, e.toString())
            return
        }
        if (nhits > 0) {
            c[fld + "_decorated"] = res;
        }
    }

    decorateAddress(c, fld, decorateAddr = true, decorateRelated = true) {
        let hit = false;
        try {
            if (!decorateAddr && !decorateRelated) return (false)
            if (c[fld] == undefined) return (false)
            let a = this.lookup_account(c[fld]);
            if (a == null) return (false);
            // nickname, judgements, info, judgementsKSM, infoKSM, verified, verifyDT, numFollowers, numFollowing
            if (a.related != undefined && decorateRelated) {
                c[fld + '_related'] = this.filterRelated(a.related);
            }
            if (decorateAddr) {
                if (a.nickname != null && a.verified > 0) {
                    c[fld + '_nickname'] = a.nickname;
                    hit = true;
                }
                if (a.parentDisplay != null && a.subName) {
                    c[fld + '_subIdentityName'] = `${a.parentDisplay}/${a.subName}`;
                    c[fld + '_parent'] = a.parent;
                    hit = true;
                } else if (a.parentDisplayKSM != null && a.subNameKSM) {
                    c[fld + '_subIdentityName'] = `${a.parentDisplayKSM}/${a.subNameKSM}`;
                    c[fld + '_parent'] = a.parentKSM;
                    hit = true;
                }
                if (a.info != null) {
                    if (a.display != null) c[fld + '_display'] = a.display;
                    c[fld + '_info'] = a.info;
                    c[fld + '_judgements'] = a.judgements;
                    hit = true;
                } else if (a.infoKSM != null) {
                    if (a.displayKSM != null) c[fld + '_display'] = a.displayKSM;
                    c[fld + '_info'] = a.infoKSM;
                    c[fld + '_judgements'] = a.judgementsKSM;
                    hit = true;
                }
            }
        } catch (e) {
            console.log(`decorateAddress err`, e.toString())
        }
        return (hit);
    }


        async compute_holdings_USD(holdings, ts = false) {
        if (!ts) {
            ts = this.getCurrentTS();
        }
        let totalUSDVal = 0;
        for (const assetType of Object.keys(holdings)) {
            let assets = holdings[assetType];
            if (assets == undefined) {
		//console.log("holdings assetType:", assetType, holdings[assetType]);                                                                                         
                continue;
            }
	    let flds = this.get_assetType_flds(assetType);
	    for (let i = 0; i < assets.length; i++) {
                let holding = holdings[assetType][i];
                let usdVal = await this.decorate_assetState(holding.assetInfo, holding.state, flds, ts);
                totalUSDVal += usdVal;
            }
	}
        return totalUSDVal;
	}

    get_assetType_flds(assetType) {
        let assetTypesFields = {
            'ERC20LP': [],
            'ERC20': [],
            'ERC721': [],
            'ERC1155': []
        };
        let res = assetTypesFields[assetType];
        if (res !== undefined) {
            return (res);
        }
        console.log("get_assetType_flds empty", assetType);
        return ([]);
    }


    
    async decorateArgsAsset(args, fld, fldasset, chainID, ts, decorateUSD = true) {
        if (args[fldasset] == undefined) return;
        if (args[fld] == undefined) return;
        try {
            let val = args[fld];
            let rawAsset = args[fldasset]
            if (rawAsset.token == undefined) return;
            let symbol = rawAsset.token
            let targetAsset = JSON.stringify({
                "Token": symbol
            })
            let decimals = this.getAssetDecimal(targetAsset, chainID)
            args[fld + "_symbol"] = symbol
            if (decorateUSD) {
                let p = await this.computePriceUSD({
                    val,
                    asset: targetAsset,
                    chainID: chainID,
                    ts
                });
                if (p) {
                    args[fld + "_USD"] = p.valUSD
                    args[fld + "_priceUSD"] = p.priceUSD
                    args[fld + "_priceUSDCurrent"] = p.priceUSDCurrent
                }
            }
        } catch (err) {
            console.log(err)
        }
    }

    async decorateArgsChainAsset(args, fld, chainID, ts, decorateUSD = true) {
        var decimals = this.getChainDecimal(chainID)
        if (args[fld] == undefined) return;
        let val = args[fld] / 10 ** decimals;
        let targetAsset = this.getChainAsset(chainID)
        if (targetAsset == null) return;
        let symbol = this.getChainSymbol(chainID)

        args[fld + "_symbol"] = symbol
        if (decorateUSD) {
            let p = await this.computePriceUSD({
                val,
                asset: targetAsset,
                chainID,
                ts
            });
            if (p) {
                args[fld + "_USD"] = p.valUSD
                args[fld + "_priceUSD"] = p.priceUSD
                args[fld + "_priceUSDCurrent"] = p.priceUSDCurrent
            }
        }
    }

    /* async decorateArgsPubKey(args, fld) {
        if (args[fld] == undefined) return;
        if (Array.isArray(args[fld])) {
            let ids = uiTool.presentDests(args[fld])
            let pubkeys = ids.map((id) => {
                return paraTool.getPubKey(id);
            })
            args[fld + "_pubkey"] = pubkeys;
        } else {
            let id = uiTool.presentDest(args[fld])
            if (!id) return;
            let pubkey = paraTool.getPubKey(id);
            if (pubkey) {
                args[fld + "_pubkey"] = pubkey;
            }
        }
    } */

    async decorateArgsCurrency(args, fld) {
        if (args[fld] == undefined) return;
        args[fld + "_symbol"] = uiTool.presentCurrency(args[fld])
    }

    async decorateArgsPath(args, fld) {
        if (args[fld] == undefined) return;
        args[fld + "_decorated"] = uiTool.presentPath(args[fld]);
    }

    async decorateArgsAssets(args, fld) {
        if (args[fld] == undefined) return;
        args[fld + "_symbol"] = uiTool.presentAssets(args[fld]);
    }

    async decorateArgsAssets(args, fld) {
        if (args[fld] == undefined) return;
        args[fld + "_decorated"] = uiTool.presentInfo(args[fld])
    }

    async decorateArgsTS(args, fld) {
        if (args[fld] == undefined) return;
        args[fld + "_decorated"] = uiTool.presentMS(args[fld])
    }

    async decorateArgsRemark(args, fld) {
        if (args[fld] == undefined) return;
        args[fld + "_decorated"] = uiTool.presentRemark(args[fld])
    }

    async decorateArgsParainfo(args, fld, chainID) {
        if (args[fld] == undefined) return;
        args[fld + "_parainfo"] = this.getParaInfo(args[fld], chainID)
    }
    async decorateArgsInfo(args, fld) {
        if (args[fld] == undefined) return;
        args[fld + "_info"] = uiTool.presentInfo(args[fld])
    }

    async decorateParams(pallet, method, args, chainID, ts) {
        let pallet_method = `${pallet}:${method}`
        this.chainParser.decorate_query_params(this, pallet_method, args, chainID, ts)

        return args
    }


    suppress_trace(id, section, storage) {
        switch (section) {
            case "Dmp":
            case "Hrmp":
            case "ParachainSystem":
                return (true);
        }
        return (false);
    }

    suppress_call(id, section, method) {
        //console.log(`id=${id}, section=${section} method=${method}`)
        if (id == "nodle" && section == "allocations") {
            return (true);
        }
        if (id == "khala") {
            if (method == "forceBatch" || section == "phalaMq" || section == "utility" || section == "proxy") {
                return (true);
            }
        }
        switch (section) {
            case "Dmp":
            case "Hrmp":
            case "ParachainSystem":
                return (true);
        }

        return (false);
    }


}
