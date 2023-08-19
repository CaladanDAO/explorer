var initrecentblocks = false;
var initchainlog = false;
var initspecversions = false;
var initwasmcontracts = false;
var initwasmcode = false;
var initchannels = false;
var refreshIntervalMS = 5000;
var recentBlocksIntervalId = false;

function build_filter_string(filter) {
    console.log("filter", filter);
    if (!filter) return "";
    let out = [];
    if (filter.chainID != undefined && filter.chainIDDest != undefined) {
        out.push(`chainID=${filter.chainID}`);
        out.push(`chainIDDest=${filter.chainIDDest}`);
    } else if (filter.chainList != undefined && Array.isArray(filter.chainList) && filter.chainList.length > 0) {
        out.push(`chainfilters=${filter.chainList.join(',')}`);
    } else if (filter.chainID != undefined) {
        out.push(`chainfilters=${filter.chainID}`);
    }
    if (filter.symbol != undefined) {
        out.push(`symbol=${filter.symbol}`);
    }
    console.log("filter out", out);
    if (out.length > 0) {
        let filterStr = "?" + out.join("&");
        return filterStr;
    }
    return "";
}


var inittokens = false;
let tokensTable = null;

function showtokens(chainID) {
    if (inittokens) return;
    else inittokens = true;
    let chainIDstr = (chainID == undefined) ? "all" : chainID.toString();
    let pathParams = `chain/token/${chainIDstr}`
    console.log(pathParams);
    let tableName = '#tabletokens'
    tokensTable = $(tableName).DataTable({
        order: [
            [1, "desc"],
            [6, "desc"],
        ],
        columnDefs: [{
            "className": "dt-right",
            "targets": [1, 3, 4, 5, 6]
        }, {
            "className": "dt-left",
            "targets": [2]
        }],
        columns: [{
            data: 'assetName',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    if (row.assetType == "ERC20LP") {
                        return "x" // presentAssetPair(row.assetChain, row.symbol, row.token0, row.token1, row.token0Symbol, row.token1Symbol, chainID);
                    } else if (row.assetType == "Loan") {
                        return presentLoan(row.assetChain, row.asset);
                    } else {

                        return `<A href="/asset/${row.chainID}/${row.asset}">${row.assetName}</A>`;
                    }
                }
                return data;
            }
        }, {
            data: 'asset',
            render: function(data, type, row, meta) {
                if (row.asset != undefined) {
                    try {
                        let str = (row.localSymbol != undefined && row.localSymbol) ? row.localSymbol : "";
                        let [accountState, balanceUSD] = get_accountState(row.asset, row.chainID, row.assetChain);
                        if (!accountState) {
                            if (type == 'display') {
                                if (balanceUSD == null) {
                                    return `-Connect Wallet [${str}]-`
                                } else {
                                    return "-";
                                }
                            } else {
                                return 0;
                            }
                        } else if (accountState && accountState.free !== undefined) {
                            if (type == 'display') {

                                return presentTokenCount(accountState.free) + " " + str + " (" + currencyFormat(balanceUSD) + ")";
                            } else {
                                return balanceUSD + .000000001 * accountState.free;
                            }
                            return 0;
                        } else {
                            if (type == 'display') {
                                return str;
                            }
                        }
                        return 0;
                    } catch (err) {
                        console.log(err);
                        return "-"
                    }
                }
            }
        }, {
            data: 'chainID',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    if (row.chainID != undefined && row.currencyID != undefined) {

                        let str = `${row.chainName} ${row.localSymbol}`;
                        if (row.currencyID != row.localSymbol && row.currencyID != row.symbol) {
                            str += ` (${row.currencyID})`
                        }
                        return `<a href='/asset/${row.chainID}/${row.currencyID}'>${str}</a>`
                    } else {
                        return "-";
                    }
                } else {
                    return row.assetName;
                }
                return data;
            }
        }, {
            data: 'numHolders',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return presentNumber(data);
                }
                return data;
            }
        }, {
            data: 'priceUSD',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return currencyFormat(data);
                }
                return data;
            }
        }, {
            data: 'totalFree',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    if (row.totalFree !== undefined) {
                        return presentTokenCount(data);
                    }
                }
                if (row.totalFree !== undefined) {
                    return data;
                } else {
                    return 0;
                }
            }
        }, {
            data: 'tvlFree',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    if (row.tvlFree != undefined) {
                        return currencyFormat(data);
                    }
                } else {
                    if (row.tvlFree != undefined) {
                        return data;
                    }
                }
                return 0;
            }
        }]
    });
    loadData2(pathParams, tableName, true)
}


function stoprecentblocks(chainID) {
    if (recentBlocksIntervalId) {
        clearInterval(recentBlocksIntervalId);
        recentBlocksIntervalId = false
    }
}

function showrecentblocks(chainID) {
    if (!recentBlocksIntervalId) {
        show_recentblocks(chainID)
    }
    recentBlocksIntervalId = setInterval(function() {
        show_recentblocks(chainID)
    }, refreshIntervalMS);
}

function show_recentblocks(chainID) {

    let pathParams = `chain/${chainID}`

    let tableName = '#tablerecentblocks'
    if (initrecentblocks) {

    } else {
        initrecentblocks = true;
        var table = $(tableName).DataTable({
            order: [
                [0, "desc"]
            ],
            columnDefs: [{
                "className": "dt-center",
                "targets": "_all"
            }],
            columns: [{
                data: 'blockNumber',
                render: function(data, type, row, meta) {
                    if (type == 'display') {
                        return presentBlockNumber(chainID, false, data)
                    }
                    return data;
                }
            }, {
                data: 'blockHash',
                render: function(data, type, row, meta) {
                    if (type == 'display') {
                        let f = (row.finalized == 0) ? presentFinalized(false) : "";
                        return f + " " + presentBlockHash(chainID, false, row.blockNumber, data);
                    }
                    return data;
                }
            }, {
                data: 'blockTS',
                render: function(data, type, row, meta) {
                    if (type == 'display') {
                        return presentTS(data);
                    }
                    return data;
                }
            }, {
                data: 'numTransactionsEVM',
                render: function(data, type, row, meta) {
                    if (type == 'display') {
			if (row.numTransactionsEVM != undefined) {
			    return `<a href='/txs/${chainID}/${row.blockNumber}'>` + presentNumber(row.numTransactionsEVM) + "</a>";
                        }
                    } else {
			if (row.numTransactionsEVM != undefined) {
			    return row.numTransactionsEVM;
			}
                    }
                    return "";
                }
            }, {
		data: 'gasUsed',
                render: function(data, type, row, meta) {
                    if (type == 'display') {
                        if (row.gasUsed != undefined) {
                            return presentNumber(row.gasUsed);
			}
		    } else {
                        if (row.gasUsed != undefined) {
			    return row.gasUsed;
			}
		    }
		    return "";
		}
	    }, {
                data: 'fees',
                render: function(data, type, row, meta) {
                    if (type == 'display') {
                        return  presentNumber(data);
                    }
                    return data;
                }
            }, {
                data: 'feesBurned',
                render: function(data, type, row, meta) {
                    if (type == 'display') {
                        return presentNumber(data);
                    }
                    return data;
                }
            } ]
        });
    }

    $(tableName).on('page.dt', function() {
        stoprecentblocks();
    });
    loadData2(pathParams, tableName, false, 'blocks')
}

function showspecversions(chainID) {
    if (initspecversions) return;
    else initspecversions = true;
    let pathParams = `specversions/${chainID}`
    let tableName = '#tablespecversions'
    var table = $(tableName).DataTable({
        order: [
            [0, "desc"]
        ],
        columnDefs: [{
            "className": "dt-center",
            "targets": "_all"
        }],
        columns: [{
            data: 'specVersion',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return presentSpecVersion(chainID, data)
                }
                return data;
            }
        }, {
            data: 'blockNumber',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return presentBlockNumber(chainID, false, data)
                }
                return data;
            }
        }, {
            data: 'blockHash',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return presentBlockHash(chainID, false, row.blockNumber, data);
                }
                return data;
            }
        }, {
            data: 'firstSeenTS',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return presentTS(data);
                }
                return data;
            }
        }]
    });
    loadData2(pathParams, tableName, false)
}

function showchannels(chainID) {
    if (initchannels) return;
    else initchannels = true;
    let pathParams = `chain/channels/${chainID}`
    let tableName = '#tablechannels'
    var table = $(tableName).DataTable({
        order: [
            [6, "desc"],
            [7, "desc"],
            [4, "desc"],
        ],
        columnDefs: [{
            "className": "dt-center",
            "targets": "_all"
        }],
        columns: [{
            data: 'id',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    if (row.chainID == chainID || row.id == chainID) {
                        return "<B>" + row.chainName + "</B>";
                    } else {
                        return presentChain(row.id, row.chainName, false, "", "#channels");
                    }
                }
                return data;
            }
        }, {
            data: 'idDest',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    if (row.chainIDDest == chainID || row.idDest == chainID) {
                        return "<B>" + row.chainNameDest + "</B>";
                    } else {
                        return presentChain(row.idDest, row.chainNameDest, false, "", "#channels");
                    }
                }
                return data;
            }
        }, {
            data: 'status',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return data;
                }
                return data;
            }
        }, {
            data: 'openRequestTS',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    let s = presentXCMMessageHash(row.msgHashOpenRequest, row.sentAtOpenRequest);
                    return presentTS(data) + s;
                }
                return data;
            }
        }, {
            data: 'acceptTS',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    let s = presentXCMMessageHash(row.msgHashAccepted, row.sentAtAccepted);
                    return presentTS(data) + s;
                }
                return data;
            }
        }, {
            data: 'symbols',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    let out = [];
                    if (data) {
                        for (const symbolChain of data) {
                            let [symbol, relayChain] = parseAssetChain(symbolChain);
                            out.push(`<a href='/channel/${row.chainID}/${row.chainIDDest}/${symbol}'>${symbol}</a>`);
                            console.log(symbolChain, row.chainID, row.chainIDDest);
                        }
                        return out.join(" | ");
                    }
                }
                return data;
            }
        }, {
            data: 'numXCMMessagesOutgoing7d',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    console.log(row);
                    // todo: 1d/7d/30d
                    return presentNumber(data);
                }
                return data;
            }
        }, {
            data: 'valXCMMessagesOutgoingUSD7d',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    // todo: 1d/7d/30d
                    return currencyFormat(data)
                }
                return data;
            }
        }]
    });
    loadData2(pathParams, tableName, false)
}


function showwasmcontracts(chainID) {
    if (initwasmcontracts) return;
    else initwasmcontracts = true;
    let pathParams = `wasmcontracts/${chainID}`
    let tableName = '#tablewasmcontracts'
    var table = $(tableName).DataTable({
        order: [
            [5, "desc"]
        ],
        columnDefs: [{
            "className": "dt-center",
            "targets": "_all"
        }],
        columns: [{
            data: 'address',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return presentWASMContract(data);
                }
                return data;
            }
        }, {
            data: 'status',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return data;
                }
                return data;
            }
        }, {
            data: 'deployer',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return presentID(data)
                }
                return data;
            }
        }, {
            data: 'codeHash',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return presentWASMCodeHash(data);
                }
                return data;
            }
        }, {
            data: 'instantiateBN',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return presentExtrinsicIDHash(row.extrinsicID, row.extrinsicHash);
                }
                return data;
            }
        }, {
            data: 'blockTS',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return presentTS(data);
                }
                return data;
            }
        }]
    });
    loadData2(pathParams, tableName, false)
}

function showwasmcode(chainID) {
    if (initwasmcode) return;
    else initwasmcode = true;
    let pathParams = `wasmcode/${chainID}`
    let tableName = '#tablewasmcode'
    var table = $(tableName).DataTable({
        order: [
            [6, "desc"]
        ],
        columnDefs: [{
            "className": "dt-center",
            "targets": "_all"
        }],
        columns: [{
            data: 'codeHash',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return presentWASMCodeHash(data);
                }
                return data;
            }
        }, {
            data: 'status',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return data;
                }
                return data;
            }
        }, {
            data: 'storer',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return presentID(data)
                }
                return data;
            }
        }, {
            data: 'codeStoredBN',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return presentExtrinsicIDHash(row.extrinsicID, row.extrinsicHash);
                }
                return data;
            }
        }, {
            data: 'language',
            render: function(data, type, row, meta) {
                return data;
            }
        }, {
            data: 'compiler',
            render: function(data, type, row, meta) {
                return data;
            }
        }, {
            data: 'codeStoredTS',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return presentTS(data);
                }
                return data;
            }
        }]
    });
    loadData2(pathParams, tableName, false)
}

function presentLoan(assetChain, assetString) {
    let asset = JSON.parse(assetString)
    let symbol = "UNK";

    try {
        if (asset.Loan != undefined && asset.Loan.Token != undefined) {
            symbol = asset.Loan.Token;
        }
        return '<a href="/asset/' + encodeURIComponent2(assetChain) + '"> Loan: ' + symbol + '</a>';
    } catch (e) {
        return "Loan: UNK"
    }
}

function showchaininfo(chainID) {
    // no datatable
}

function showchainlog(chainID, address) {
    if (initchainlog) return;
    else initchainlog = true;
    let pathParams = `chainlog/${chainID}`

    let tableName = '#tablechainlog'
    var table = $(tableName).DataTable({
        order: [
            [0, "desc"]
        ],
        pageLength: 50,
        lengthMenu: [
            [10, 25, 50, 100],
            [10, 25, 50, 100]
        ],
        columnDefs: [{
            "className": "dt-right",
            "targets": [1, 2, 3, 4]
        }, {
            "className": "dt-center",
            "targets": [1, 2, 3, 4]
        }],
        columns: [{
            data: 'logDT',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return data
                }
                return data;
            }
        },{
            data: 'startBN',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return data
                }
                return data;
            }
        },{
            data: 'endBN',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return data
                }
                return data;
            }
        }, {
            data: 'numActiveAccounts',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return data;
                }
                return data;
            }
        }, {
            data: 'numNewAccounts',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return data;
                }
                return data;
            }
        }, {
            data: 'numTransactionsEVM',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return presentNumber(data);
                }
                return data;
            }
        }, {
            data: 'numTransactionsEVM1559',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return presentNumber(data);
                }
                return data;
            }
        }, {
            data: 'numTransactionsEVMLegacy',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return presentNumber(data);
                }
                return data;
            }
        }, {
            data: 'fees',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return presentNumber(data);
                } else {
                    return data;
                }
            }
        }, {
            data: 'gasUsed',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return presentNumber(data);
                } else {
                    return data;
                }
            }
        }, {
            data: 'gasPrice',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return presentNumber(data);
                } else {
                    return data;
                }
            }
        }, {
            data: 'maxFeePerGas',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return presentNumber(data);
                } else {
                    return data;
                }
            }
        }, {
            data: 'maxPriorityFeePerGas',
            render: function(data, type, row, meta) {
                if (type == 'display') {
                    return presentNumber(data);
                } else {
                    return data;
                }
            }
        }]
    });
    loadData2(pathParams, tableName, true)
}
