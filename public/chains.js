var initchains = false;
var chainsTable = null;
var refreshIntervalMS = 6100;
var chainsUpdateIntervalId = false;

function stopchains() {
    if (chainsUpdateIntervalId) {
        clearInterval(chainsUpdateIntervalId);
        chainsUpdateIntervalId = false
    }
}

function showchains() {
    if (!chainsUpdateIntervalId) {
        show_chains();
    }
    chainsUpdateIntervalId = setInterval(function() {
        show_chains()
    }, refreshIntervalMS);
}

function get_accountBalanceOnChain(chainID, assetChain) {
    try {
        let balanceUSD = 0;
        for (let a = 0; a < accounts.length; a++) {
            let account = accounts[a];
            if (account.chains) {
                for (let i = 0; i < account.chains.length; i++) {
                    let c = account.chains[i];
                    if (c.chainID == chainID) {
                        for (let j = 0; j < c.assets.length; j++) {
                            let a = c.assets[j];
                            if (a.state.balanceUSD > 0) {
                                //console.log(a);
                                balanceUSD += a.state.balanceUSD;
                            }
                        }
                    }
                }
            }
        }
        return balanceUSD;
    } catch (err) {
        console.log(err);
    }
    return 0;
}

async function show_chains() {
    let pathParams = 'chains'
    let tableName = '#tablechains'
    if (initchains) {
        // if table is already initiated, update the rows
        //loadData2(pathParams, tableName, true)
    } else {
        initchains = true;
        chainsTable = $(tableName).DataTable({
            pageLength: -1,
            lengthMenu: [
                [10, 25, 50, -1],
                [10, 25, 50, "All"]
            ],
            columnDefs: [{
                "className": "dt-right",
                "targets": [2, 3, 4]
            }],
            order: [
                [2, "desc"],
                [4, "desc"],
            ],
            columns: [{
                data: 'id',
                render: function(data, type, row, meta) {
                    if (type == 'display') {
                        let links = [];
                        return presentChain(row.id, row.chainName, row.iconUrl, "") + `<div class="explorer">` + links.join(" | ") + `</div>`
                    }
                    return row.chainName;
                }
            }, {
                data: 'balanceUSD',
                render: function(data, type, row, meta) {
                    try {
                        // show account holdings summary on chain (linking to /xcmassets/${chainID})
                        let balanceUSD = get_accountBalanceOnChain(row.chainID);
                        if (type == 'display') {
                            if (balanceUSD == null) {
                                return "-Connect Wallet-";
                            }
                            let url = `/xcmassets/${row.chainID}`;
                            return `<a href="${url}">` + currencyFormat(balanceUSD) + "</a>";
                        } else {
                            if (balanceUSD == null) return 0;
                            return balanceUSD;
                        }
                    } catch {
                        return "-"
                    }
                    return 0;
                }
            }, {
                data: 'blocksCovered',
                render: function(data, type, row, meta) {
                    if (type == 'display') {
                        try {
                            let s = "<BR>" + presentTS(row.lastCrawlTS);
                            return presentBlockNumber(row.id, "", row.blocksCovered) + s;
                        } catch {
                            return "-"
                        }
                    }
                    return data;
                }
            }, {
                data: 'blocksFinalized',
                render: function(data, type, row, meta) {
                    if (type == 'display') {
                        try {
                            let s = "<BR>" + presentTS(row.lastFinalizedTS);
                            return presentBlockNumber(row.id, "", row.blocksFinalized) + s;
                        } catch {
                            return "-"
                        }
                    }
                    return data;
                }
            }, {
                data: 'numAccountsActive7d',
                render: function(data, type, row, meta) {
                    try {
                        if (type == 'display') {
                            let url = `/chainlog/${row.id}`
                            return `<a href="${url}">` + presentNumber(data) + "</a>";
                        }
                        return data;
                    } catch {
                        return "-"
                    }
                }
            }]
        });
    }

    $(tableName).on('page.dt', function() {
        stopchains();
    });

    //load data here: warning this function is technically async
    //load data here: warning this function is technically async
    await loadData2(pathParams, tableName, false)
}
