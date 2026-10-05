# Job Collector

Collects job postings for Riadh's Job Radar three times a day (07:00, 12:00, 17:00 Berlin time), for free, using GitHub Actions.

**Sources**
- Company job boards via their public feeds: Ashby, Greenhouse, Lever, Personio, Recruitee, Workable, SmartRecruiters (`data/companies.json`, grows automatically from `candidates.txt`)
- Bundesagentur für Arbeit Jobsuche (Essen + 50 km, and home office Germany-wide)
- Arbeitnow, Himalayas, Jobicy, RemoteOK, Remotive

**Filters** (`config.json`): React / React Native / Frontend / Full-stack / TypeScript titles (or generic developer titles whose description mentions React/TypeScript), remote in Germany/EU or NRW office, posted within 60 days, no junior/intern/freelance/Angular-only roles.

**Output**
- `out/latest.json` — all current matches
- `out/new.json` — only jobs not seen before
- `out/status.json` — which sources worked

Run by hand: **Actions → Collect jobs → Run workflow**.
To add a company: add a line `Company name | industry` to `candidates.txt`.
