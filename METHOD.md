# Method: what the archive saves, and how

Fixed before the collector saved its first snapshot. Changes are listed in `CHANGELOG.md`, with the reason, before they take effect.

## Why this archive exists

Energinet's DataHub price list (`DatahubPricelist` on Energi Data Service) holds every Danish grid tariff, Energinet's own tariffs and fees, and the electricity tax, each with the date it applies from. It shows the list as it is now. Rows can be corrected, renamed or removed, and earlier versions are not kept. For example, when DataHub 3.0 went live on 21 September 2026, the contents of the `ChargeOwner` column changed. A query on Energinet's former owner name now returns no rows.

Day-ahead electricity prices (`DayAheadPrices`) are published once a day for the next day. If a price is ever corrected, the new value replaces the old one.

This archive saves both, as published, every day. After a while it can answer questions nobody else can answer afterwards. When was a tariff change first announced? How often are published tariffs corrected? What did the published list say on a given day?

## What is saved

| What | Dataset | When (UTC) |
|---|---|---|
| The full price list: all owners, all charges, all dates | `DatahubPricelist` | every day at 05:17 |
| Day-ahead prices for DK1 and DK2, from yesterday to tomorrow | `DayAheadPrices` | every day at 13:47, after the day-ahead results are published in both summer and winter time |

The collector (`kvx_prices.py`) runs on Kvantix' server and uses the Python standard library only. It works like the [weather test](https://github.com/kvantixtech/weather-forecast-test) and the [energy test](https://github.com/kvantixtech/energinet-forecasts):

- Every download is saved compressed (xz) and locked in a SHA-256 hash chain before it is read.
- Identical responses share one file, and the chain still records every download.
- A failed download stays in the chain as a gap.
- The head of the chain is published in this repository once a day at 14:27 UTC (`anchors/chain-heads.csv`).
- The collector's own SHA-256 is recorded in `METHOD.lock`. The daily anchor records which version was running.

Source: Energinet (www.energidataservice.dk), CC BY 4.0. Kvantix is not affiliated with Energinet, and Energinet does not endorse this archive.

## How changes are recorded

1. **A price list row is identified by** the owner's GLN number, the charge type, the charge type code and the date it applies from (`ValidFrom`). A new price, a new end date or a renamed owner for the same row is a *change*.
2. **The first snapshot is the baseline.** From the second snapshot on, every row that is added, changed or removed is logged, with its content before and after.
3. **A response is refused** if its number of records differs from the total it states, or if it contains no records. It stays in the chain, marked as unreadable, and the stored list is left as it was. Nothing is marked as removed because of a bad download.
4. **Day-ahead prices:** the first published value is kept. A later, different value is logged as a revision, with both values.

## What this can't show

- **Nothing before the first snapshot.** The first snapshot shows the list as published on that day, including any changes Energinet had already made.
- **What customers actually paid.** Electricity suppliers' own prices and subscriptions are not in the DataHub price list.
- **Why something changed.** The archive shows what was published and when.

## Analyses

This repository is an archive, not a test. Nothing is scored here. Any analysis built on it, for example of how often tariffs change or how early changes are announced, gets its own written method, committed before the analysis is run.
