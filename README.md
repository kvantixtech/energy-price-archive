# Energy price archive

A daily, hash-chained archive of Denmark's published electricity price list: every grid tariff, Energinet's tariffs and fees, and the electricity tax. It also saves the day-ahead electricity prices for DK1 and DK2.

The official price list shows only its current state. Corrections, renamings and removals overwrite what was there before. This archive keeps every version, so the published history can be checked later.

**Status: collecting since 30 September 2026.** The first snapshot holds 120,026 price list rows. From the second snapshot on, every change is logged. The rules are in [`METHOD.md`](METHOD.md). The collector is [`kvx_prices.py`](kvx_prices.py), and its SHA-256 is in [`METHOD.lock`](METHOD.lock). The hash chain is anchored daily in [`anchors/chain-heads.csv`](anchors/chain-heads.csv).

Part of the Kvantix [Data Playground](https://kvantix.tech/playground/), alongside the [energy test](https://github.com/kvantixtech/energinet-forecasts) and the [weather forecast test](https://github.com/kvantixtech/weather-forecast-test).

## Check it yourself

`tools/anchor.py check --db <copy of prices.sqlite3>` recomputes the whole chain and confirms that every published anchor lies on it. `kvx_prices.py verify --full` also checks every saved response against its hash.

## Data and licence

Source: Energinet (www.energidataservice.dk), CC BY 4.0. Kvantix is not affiliated with Energinet, and Energinet does not endorse this archive.

Code: MIT.
