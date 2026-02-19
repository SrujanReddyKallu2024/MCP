# Marketplace automations (from Confluence)

Pulled on **2026-02-19** from Confluence (space: **EMS Activate / ACTIVATE**).

## Source pages (authoritative)

- STS Automations (hub): https://pages.experian.local/spaces/ACTIVATE/pages/1524103782/STS+Automations
- Marketplaces (automation table): https://pages.experian.local/spaces/ACTIVATE/pages/1524103794/Marketplaces
- Marketplaces Overview (process docs): https://pages.experian.local/spaces/ACTIVATE/pages/1446433378/Marketplaces+Overview
- Marketplaces Schedule (triggers): https://pages.experian.local/spaces/ACTIVATE/pages/1852603752/Marketplaces+Schedule

## Marketplaces — automations list

This is the complete set of marketplace-related STS automations listed on the **Marketplaces** page.

- **Marketplace Input Files** — route: `imat8456-internal-marketplace_files` — polling: As soon as upload
- **Pubmatic** — route: `gftuk_team-p2match-marketplaces-pubmatic-outbound` — polling: Every hour
- **Custom/Standard Audience Files** — routes: `imig_mas-activate-marketplace-files`, `idbp8100-activate-marketplace-files`, `ires3653-activate-marketplace-files` — polling: As soon as uploaded
- **Eyeota** — route: `ip2match : profile-uploader-eu.s3.eyeota.net-RITM3283733` — polling: Every two hours / Every hour
- **Evorra** — route: `ip2match-UKBLDAPMTC02-evorra-prod-rawdata-RITM3541226` — polling: Every 30 mins / Every hour
- **Audigent** — route: `ip2match-tmp-RITM3402012 : audigent-experian-RITM3402012` — polling: Every hour
- **Peer39** — route: `ip2match-tmp-RITM3804215` — polling: Every 30 mins / Every hour
- **Vevo** — route: *(not specified on page)* — polling: Every hour
- **EU Geo Data to Tradedesk API** — route: *(not specified on page)* — polling: Every hour
- **EU Geo Response from Tradedesk API** — route: *(not specified on page)* — polling: As soon as upload
- **Beeswax (taxonomy + segments outbound)** — routes: `ip2match-taxonomy-RITM3941460`, `ip2match-segments-RITM3941460` — polling: Every hour
- **Beeswax (to_xpn inbound)** — route: `ip2match-beeswax-to_xpn` — polling: Every 30 minutes / Every hour
- **Freewheel (outbound to AIM buckets)** — route: `aim-incoming-data-prd-eu-west-2-freewheel` — polling: Every 30 minutes / Every hour
- **Freewheel (sent-extension return files)** — routes: `uat-taxonomy`, `prod-taxonomy` — polling: As soon as
- **Infosum (clean room)** — route: `Match -p2match` — polling: Infosum pull interval (TBC)
- **Infosum (sts publish/from_xpn)** — route: `Match -p2match` — polling: Every 30 minutes / Every hour
- **Stackadapt (test)** — route: `ip2match-tmp-stackadapt_RITM4027976` — polling: Every 30 minutes
- **Stackadapt (graph outbound)** — route: `ip2match-tmp-RITM4327761_stackadapt_graph_outbound` — polling: Every 30 minutes / Every hour
- **Snowflake** — route: `ip2match-tmp-RITM4191314` — polling: Every hour

For the full details (file patterns, sources/destinations, RITMs, notifications), use the table on:
https://pages.experian.local/spaces/ACTIVATE/pages/1524103794/Marketplaces

## Marketplaces Schedule — triggers

(From the **Marketplaces Schedule** page.)

| process | schedule |
|---|---|
| `ttdid_trigger.sh` | `SUN_DATE` midnight |
| `aud_trigger.sh` | `SUN_DATE` at 8am (**currently off**) |
| `p39_trigger.sh` | `SUN_DATE` 3pm (checks for `/user/unity/id_graph/flags/p39_taxonomy_${RUNDATE}_inprogress`) |
| `snf_trigger.sh` | `SUN_DATE` 4pm |
| `bw_trigger.sh` | hourly (if data is more than 14 days) |
| `fwgeo_trigger.sh` | (**currently off**) |
| `fw_trigger.sh` | `WED_DATE` midnight |
| `inf_trigger.sh` | Lookup: 1st of Month at 3pm |
| `lookup_trigger.sh` | called by other scripts |
| `stck_trigger.sh` | `MON_DATE` 9am |
| `ttdgeo_trigger.sh` | hourly |
| `ttdid_custom_trigger.sh` | hourly |

### Marketplaces Schedule — ID types by marketplace

(Also on the **Marketplaces Schedule** page.)

| Marketplace | IDs |
|---|---|
| Evrrora | ID5, MAIDS, TTDID, APPNID, PC, HEMS, MOBILE |
| TTD UK / DA | MAIDS, ID5, TTDID |
| Eyeota | ID5, PC |
| Pubmatic | MAIDS, IP, ID5 |
| Audigent | ID5, MAIDs, TTDID, AppNexus |
| Facebook | HEMS, MOBILE, MAIDS |
| Infosum | IP, HEMS, PC, ID5, MAIDs |
| Stackadapt | HEMS, MOBILE, IP, ID5, MAIDS |
| Zeotap (Amazon) (in progress) | MAIDS |

## Marketplaces Overview — child pages

Child pages under **Marketplaces Overview** (ID: 1446433378):

- https://pages.experian.local/spaces/ACTIVATE/pages/1550973598/Beeswax+Marketplace
- https://pages.experian.local/spaces/ACTIVATE/pages/1542927028/Freewheel+Marketplace
- https://pages.experian.local/spaces/ACTIVATE/pages/1446433383/The+Trade+Desk
- https://pages.experian.local/spaces/ACTIVATE/pages/1535019418/Trade+Desk+EU+Geo
- https://pages.experian.local/spaces/ACTIVATE/pages/1577972168/Freewheel+Geo+EU
- https://pages.experian.local/spaces/ACTIVATE/pages/1675172227/Audigent+Marketplace
- https://pages.experian.local/spaces/ACTIVATE/pages/1676284542/Replace+Match+ID+with+CB+Key+for+Audigent
- https://pages.experian.local/spaces/ACTIVATE/pages/1718752814/Tradedesk+Custom
- https://pages.experian.local/spaces/ACTIVATE/pages/1719407410/TTD+IDs+Marketplace+Process
- https://pages.experian.local/spaces/ACTIVATE/pages/1852603752/Marketplaces+Schedule
