#!/usr/bin/env python3
"""Job collector for Riadh's Job Radar.

Pulls job postings from company job-board feeds (Ashby, Greenhouse, Lever, Personio,
Recruitee, Workable, SmartRecruiters), the Bundesagentur für Arbeit job search, and a few
remote job boards with public APIs. Filters them by title, stack, location and freshness,
removes duplicates and writes:

  out/latest.json   all matching jobs found in this run (with firstSeen dates)
  out/new.json      only the jobs never seen before
  out/status.json   which sources worked / failed, counts
  data/seen.json    memory of every job id already seen (id -> firstSeen)
  data/companies.json  company feeds in use (grows automatically via probing)

Standard library only. Run:  python collector.py            (collect)
                             python collector.py --probe    (also probe candidates.txt)
"""
import concurrent.futures as cf
import datetime as dt
import html
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

ROOT = os.path.dirname(os.path.abspath(__file__))
CFG = json.load(open(os.path.join(ROOT, "config.json"), encoding="utf-8"))
NOW = dt.datetime.now(dt.timezone.utc)
TODAY = NOW.strftime("%Y-%m-%d")
UA = "Mozilla/5.0 (compatible; job-radar-collector/1.0; +https://github.com/Riadh180/job-collector)"
STATUS = {"sources": {}, "errors": {}}


# ---------------------------------------------------------------- helpers
def http(url, headers=None, timeout=25, retries=1):
    h = {"User-Agent": UA, "Accept": "application/json, text/xml, */*"}
    if headers:
        h.update(headers)
    last = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=h)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            return e.code, b""
        except Exception as e:  # timeouts, DNS, resets
            last = e
            time.sleep(1.5)
    raise last


def get_json(url, headers=None):
    code, body = http(url, headers)
    if code != 200:
        return code, None
    try:
        return code, json.loads(body.decode("utf-8", "replace"))
    except Exception:
        return code, None


def strip_html(s):
    if not s:
        return ""
    s = re.sub(r"<[^>]+>", " ", s)
    return re.sub(r"\s+", " ", html.unescape(s)).strip()


def term_re(terms):
    parts = [r"(?<![a-z0-9])" + re.escape(t.lower().strip()) + r"(?![a-z0-9])" for t in terms if t.strip()]
    return re.compile("|".join(parts)) if parts else None


RE_TITLE_INC = term_re(CFG["title_include"])
RE_TITLE_GEN = term_re(CFG["title_generic"])
RE_TITLE_HARD = term_re(CFG["title_hard_exclude"])
RE_TITLE_SOFT = term_re(CFG["title_soft_exclude"])
RE_COMPANY_EXC = term_re(CFG["company_exclude"])
RE_REMOTE_DE = re.compile("|".join(CFG["remote_germany_phrases"]))
RE_STACK = term_re(CFG["description_stack"])
RE_ANGULAR = term_re(CFG["description_exclude_if_no_react"])
RE_REMOTE = term_re(CFG["remote_words"])
RE_OK_REGION = term_re(CFG["remote_ok_regions"])
RE_BAD_REGION = term_re(CFG["remote_bad_regions"])
RE_NRW = term_re(CFG["nrw_places"])
RE_DE = term_re(CFG["germany_words"])
RE_HYBRID = term_re(["hybrid"])


def parse_date(v):
    if v in (None, ""):
        return None
    try:
        if isinstance(v, (int, float)):
            v = v / 1000 if v > 1e11 else v
            return dt.datetime.fromtimestamp(v, dt.timezone.utc)
        s = str(v).strip().replace("Z", "+00:00")
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
            return dt.datetime.fromisoformat(s).replace(tzinfo=dt.timezone.utc)
        d = dt.datetime.fromisoformat(s)
        return d if d.tzinfo else d.replace(tzinfo=dt.timezone.utc)
    except Exception:
        for fmt in ("%a, %d %b %Y %H:%M:%S %z", "%Y-%m-%d %H:%M:%S", "%d.%m.%Y"):
            try:
                d = dt.datetime.strptime(str(v), fmt)
                return d if d.tzinfo else d.replace(tzinfo=dt.timezone.utc)
            except Exception:
                pass
    return None


# ---------------------------------------------------------------- filtering
def title_ok(title, desc):
    t = (title or "").lower()
    if RE_TITLE_HARD.search(t):
        return False, ""
    if RE_TITLE_SOFT.search(t) and not re.search(r"full[\s-]?stack|frontend|front-end|react", t):
        return False, ""
    d = (desc or "").lower()
    if RE_TITLE_INC.search(t):
        return True, "title"
    if RE_TITLE_GEN.search(t) and d and RE_STACK.search(d):
        if RE_ANGULAR.search(d) and not re.search(r"(?<![a-z])react(?![a-z])", d):
            return False, ""
        return True, "generic+stack"
    return False, ""


def location_ok(loc_text, remote_flag=None, desc="", source=""):
    """Return (ok, label). Generous pre-filter; Claude makes the final call."""
    l = (loc_text or "").lower()
    d = (desc or "").lower()[:4000]
    if RE_NRW.search(l):
        return True, "NRW"
    remoteish = bool(remote_flag) or bool(RE_REMOTE.search(l))
    if remoteish:
        if RE_BAD_REGION.search(l) and not (RE_OK_REGION.search(l) or RE_DE.search(l)):
            return False, ""
        if RE_OK_REGION.search(l) or RE_DE.search(l):
            return True, "remote"
        if l.strip() in ("", "remote") and source not in ("remoteok", "remotive", "jobicy", "himalayas"):
            return True, "remote"
        return False, ""
    # German office city, but the description says remote is possible
    if RE_DE.search(l) and d and RE_REMOTE_DE.search(d):
        return True, "DE+remote-in-text"
    return False, ""


def fresh(date):
    if date is None:
        return True
    return (NOW - date).days <= CFG["maxAgeDays"]


def salary_floor_fail(max_eur):
    return max_eur is not None and max_eur < CFG["salary_floor_eur"]


# ---------------------------------------------------------------- record
def rec(source, key, company, title, url, location, posted=None, remote=None, salary="", salary_max=None,
        desc="", employment="", industry="", ats=""):
    return {
        "id": f"{source}:{key}",
        "company": (company or "").strip(),
        "title": (title or "").strip(),
        "url": url,
        "location": (location or "").strip()[:200],
        "remote": remote,
        "postedAt": posted.strftime("%Y-%m-%d") if posted else "",
        "salary": salary or "",
        "salaryMax": salary_max,
        "employment": employment or "",
        "industry": industry or "",
        "source": source,
        "ats": ats or source,
        "_desc": desc or "",
    }


def accept(r, loc_extra=""):
    ok_t, why_t = title_ok(r["title"], r["_desc"])
    if not ok_t:
        return None
    if RE_COMPANY_EXC.search((r["company"] or "").lower()):
        return None
    ok_l, why_l = location_ok(r["location"] + " " + loc_extra, r["remote"], r["_desc"], r["source"])
    if not ok_l:
        return None
    if not fresh(parse_date(r["postedAt"])):
        return None
    if salary_floor_fail(r.get("salaryMax")):
        return None
    emp = (r.get("employment") or "").lower()
    if re.search(r"intern|contract|freelanc|temporary|part[- ]?time|werkstudent|befristet(?<!unbefristet)", emp):
        return None
    d = r["_desc"].lower()
    r["stackHits"] = sorted(set(m.group(0) for m in RE_STACK.finditer(d)))[:6] if d else []
    r["match"] = f"{why_t}; {why_l}"
    r["facts"] = key_facts(r["_desc"])
    return r


RE_FACT = re.compile(r"remote|home ?office|hybrid|office|büro|vor ort|on-?site|days? (a|per) week|tage|€|eur|salary|gehalt|"
                     r"compensation|vergütung|unbefristet|permanent|english|deutsch|german|react|typescript|node|expo|years", re.I)


def key_facts(desc, limit=700):
    """Short excerpt of the sentences that matter for the decision (remote rule, office days, salary, language, stack)."""
    if not desc:
        return ""
    sents = re.split(r"(?<=[.!?•·])\s+|\s{2,}", desc)
    picked, total = [], 0
    for s in sents:
        s = s.strip()
        if 15 < len(s) < 260 and RE_FACT.search(s) and s not in picked:
            picked.append(s)
            total += len(s)
            if total > limit:
                break
    return " | ".join(picked)[:limit]


# ---------------------------------------------------------------- company ATS fetchers
def ashby(c):
    code, j = get_json(f"https://api.ashbyhq.com/posting-api/job-board/{c['slug']}?includeCompensation=true")
    if j is None:
        raise RuntimeError(f"HTTP {code}")
    out = []
    for x in j.get("jobs", []):
        if x.get("isListed") is False:
            continue
        locs = [x.get("location") or ""] + [s.get("location", "") for s in (x.get("secondaryLocations") or [])]
        addr = ((x.get("address") or {}).get("postalAddress") or {})
        locs.append(addr.get("addressCountry") or "")
        remote = x.get("isRemote") or (x.get("workplaceType") or "").lower() == "remote"
        comp = x.get("compensation") or {}
        smax = None
        for t in comp.get("compensationTiers") or []:
            for cpt in t.get("components") or []:
                if cpt.get("compensationType") == "Salary" and (cpt.get("currencyCode") in ("EUR", None)):
                    smax = cpt.get("maxValue") or smax
        out.append(rec("ashby", f"{c['slug']}:{x.get('id')}", c["name"], x.get("title"), x.get("jobUrl"),
                       " / ".join(l for l in locs if l), parse_date(x.get("publishedAt")), remote,
                       comp.get("compensationTierSummary") or comp.get("scrapeableCompensationSalarySummary") or "",
                       smax, (x.get("descriptionPlain") or "")[:6000], x.get("employmentType") or "",
                       c.get("industry", ""), "ashby"))
    return out


def greenhouse(c):
    host = "boards-api.eu.greenhouse.io" if c.get("eu") else "boards-api.greenhouse.io"
    code, j = get_json(f"https://{host}/v1/boards/{c['slug']}/jobs?content=true")
    if j is None:
        raise RuntimeError(f"HTTP {code}")
    out = []
    for x in j.get("jobs", []):
        offices = " / ".join(o.get("name", "") for o in (x.get("offices") or []))
        loc = ((x.get("location") or {}).get("name") or "") + (" / " + offices if offices else "")
        desc = strip_html(html.unescape(x.get("content") or ""))[:6000]
        posted = parse_date(x.get("first_published") or x.get("updated_at"))
        out.append(rec("greenhouse", f"{c['slug']}:{x.get('id')}", c["name"], x.get("title"), x.get("absolute_url"),
                       loc, posted, None, "", None, desc, "", c.get("industry", ""), "greenhouse"))
    return out


def lever(c):
    host = "api.eu.lever.co" if c.get("eu") else "api.lever.co"
    code, j = get_json(f"https://{host}/v0/postings/{c['slug']}?mode=json")
    if j is None or not isinstance(j, list):
        raise RuntimeError(f"HTTP {code}")
    out = []
    for x in j:
        cat = x.get("categories") or {}
        loc = " / ".join([cat.get("location") or ""] + (cat.get("allLocations") or []))
        remote = (x.get("workplaceType") or "").lower() == "remote"
        sr = x.get("salaryRange") or {}
        smax = sr.get("max") if (sr.get("currency") in ("EUR", None)) else None
        sal = f"{sr.get('min')}–{sr.get('max')} {sr.get('currency','')}" if sr else ""
        out.append(rec("lever", f"{c['slug']}:{x.get('id')}", c["name"], x.get("text"), x.get("hostedUrl"), loc,
                       parse_date(x.get("createdAt")), remote, sal, smax,
                       (x.get("descriptionPlain") or "")[:6000], cat.get("commitment") or "",
                       c.get("industry", ""), "lever"))
    return out


def personio(c):
    last = None
    for tld in ([c["tld"]] if c.get("tld") else ["de", "com"]):
        code, body = http(f"https://{c['slug']}.jobs.personio.{tld}/xml?language=en")
        if code != 200 or not body.strip().startswith(b"<"):
            last = f"HTTP {code}"
            continue
        root = ET.fromstring(body)
        out = []
        for p in root.iter("position"):
            g = lambda k: (p.findtext(k) or "").strip()
            pid = g("id")
            offices = [g("office")] + [o.text or "" for o in p.iter("additionalOffice")]
            desc = " ".join(strip_html(v.findtext("value") or "") for v in p.iter("jobDescription"))[:6000]
            sched = g("schedule")
            out.append(rec("personio", f"{c['slug']}:{pid}", c["name"], g("name"),
                           f"https://{c['slug']}.jobs.personio.{tld}/job/{pid}?language=en",
                           " / ".join(o for o in offices if o), parse_date(g("createdAt")), None, "", None,
                           desc, (g("employmentType") + " " + sched).strip(), c.get("industry", ""), "personio"))
        return out
    raise RuntimeError(last or "no feed")


def recruitee(c):
    base = c.get("base") or f"https://{c['slug']}.recruitee.com"
    code, j = get_json(f"{base}/api/offers/")
    if j is None:
        raise RuntimeError(f"HTTP {code}")
    out = []
    for x in j.get("offers", []):
        loc = " / ".join(filter(None, [x.get("location") or "", x.get("city") or "", x.get("country") or ""]))
        remote = bool(x.get("remote")) or "remote" in (x.get("location") or "").lower()
        desc = strip_html((x.get("description") or "") + " " + (x.get("requirements") or ""))[:6000]
        out.append(rec("recruitee", f"{c['slug']}:{x.get('id')}", c["name"], x.get("title"), x.get("careers_url"), loc,
                       parse_date(x.get("published_at") or x.get("created_at")), remote, "", None, desc,
                       x.get("employment_type_code") or "", c.get("industry", ""), "recruitee"))
    return out


def workable(c):
    code, j = get_json(f"https://apply.workable.com/api/v1/widget/accounts/{c['slug']}?details=true")
    if j is None:
        raise RuntimeError(f"HTTP {code}")
    out = []
    for x in j.get("jobs", []):
        loc = " / ".join(filter(None, [x.get("city"), x.get("state"), x.get("country")]))
        remote = bool(x.get("telecommuting"))
        out.append(rec("workable", f"{c['slug']}:{x.get('shortcode')}", c["name"], x.get("title"),
                       x.get("url") or x.get("shortlink"), loc, parse_date(x.get("published_on") or x.get("created_at")),
                       remote, "", None, strip_html(x.get("description") or "")[:6000], x.get("employment_type") or "",
                       c.get("industry", ""), "workable"))
    return out


def smartrecruiters(c):
    out, offset = [], 0
    while offset < 500:
        code, j = get_json(f"https://api.smartrecruiters.com/v1/companies/{c['slug']}/postings?limit=100&offset={offset}")
        if j is None:
            if offset == 0:
                raise RuntimeError(f"HTTP {code}")
            break
        items = j.get("content", [])
        for x in items:
            lo = x.get("location") or {}
            loc = " / ".join(filter(None, [lo.get("city"), lo.get("region"), lo.get("country")]))
            remote = bool(lo.get("remote"))
            out.append(rec("smartrecruiters", f"{c['slug']}:{x.get('id')}", c["name"], x.get("name"),
                           f"https://jobs.smartrecruiters.com/{c['slug']}/{x.get('id')}", loc,
                           parse_date(x.get("releasedDate")), remote, "", None, "",
                           ((x.get("typeOfEmployment") or {}).get("label") or ""), c.get("industry", ""),
                           "smartrecruiters"))
        if len(items) < 100:
            break
        offset += 100
    return out


FETCHERS = {"ashby": ashby, "greenhouse": greenhouse, "lever": lever, "personio": personio,
            "recruitee": recruitee, "workable": workable, "smartrecruiters": smartrecruiters}


# ---------------------------------------------------------------- job boards / public APIs
def arbeitsagentur():
    a = CFG["arbeitsagentur"]
    H = {"X-API-Key": "jobboerse-jobsuche",
         "User-Agent": "Jobsuche/2.9.2 (de.arbeitsagentur.jobboerse; build:1077; iOS 15.1.0) Alamofire/5.4.4"}
    base = None
    for cand in ("https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v4/app/jobs",
                 "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v4/jobs",
                 "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v6/jobs"):
        code, j = get_json(cand + "?was=React&size=1", H)
        if j is not None:
            base = cand
            break
        STATUS.setdefault("arbeitsagentur_tries", []).append(f"{cand.split('service/')[1]} -> {code}")
    if base is None:
        raise RuntimeError("all endpoints refused: " + "; ".join(STATUS.get("arbeitsagentur_tries", [])))
    out, calls = [], []
    for q in a["queries"]:
        common = {"was": q, "angebotsart": 1, "befristung": 2, "pav": "false", "zeitarbeit": "false",
                  "veroeffentlichtseit": a["veroeffentlichtseit"], "size": 100, "page": 1}
        calls.append(dict(common, wo=a["around"]["wo"], umkreis=a["around"]["umkreis"]))
        if a.get("nationwide_homeoffice"):
            calls.append(dict(common, arbeitszeit="ho"))
    for params in calls:
        code, j = get_json(base + "?" + urllib.parse.urlencode(params), H)
        if j is None:
            raise RuntimeError(f"HTTP {code}")
        ho = params.get("arbeitszeit") == "ho"
        for x in j.get("stellenangebote", []) or []:
            ao = x.get("arbeitsort") or {}
            loc = " / ".join(filter(None, [ao.get("ort"), ao.get("region"), "Deutschland"]))
            if ho:
                loc += " / Homeoffice"
            ref = x.get("refnr")
            url = x.get("externeUrl") or f"https://www.arbeitsagentur.de/jobsuche/jobdetail/{ref}"
            out.append(rec("arbeitsagentur", ref, x.get("arbeitgeber"), x.get("titel"), url, loc,
                           parse_date(x.get("aktuelleVeroeffentlichungsdatum") or x.get("modifikationsTimestamp")),
                           True if ho else None, "", None, (x.get("beruf") or ""), "unbefristet", "", "arbeitsagentur"))
        time.sleep(0.4)
    return out


def arbeitnow():
    out = []
    for page in range(1, CFG["boards"]["arbeitnow_pages"] + 1):
        code, j = get_json(f"https://www.arbeitnow.com/api/job-board-api?page={page}")
        if j is None:
            if page == 1:
                raise RuntimeError(f"HTTP {code}")
            break
        for x in j.get("data", []):
            out.append(rec("arbeitnow", x.get("slug"), x.get("company_name"), x.get("title"), x.get("url"),
                           x.get("location") or "",
                           parse_date(x.get("created_at")), bool(x.get("remote")), "", None,
                           strip_html(x.get("description") or "")[:6000], " ".join(x.get("job_types") or []),
                           "", "arbeitnow"))
        if not j.get("links", {}).get("next"):
            break
    return out


def remotive():
    out = []
    for s in CFG["boards"]["remotive_searches"]:
        code, j = get_json("https://remotive.com/api/remote-jobs?" + urllib.parse.urlencode({"search": s, "limit": 100}))
        if j is None:
            raise RuntimeError(f"HTTP {code}")
        for x in j.get("jobs", []):
            out.append(rec("remotive", x.get("id"), x.get("company_name"), x.get("title"), x.get("url"),
                           x.get("candidate_required_location") or "", parse_date(x.get("publication_date")), True,
                           x.get("salary") or "", None, strip_html(x.get("description") or "")[:6000],
                           x.get("job_type") or "", "", "remotive"))
    return out


def jobicy():
    out = []
    for tag in CFG["boards"]["jobicy_tags"]:
        for geo in ("germany", "europe", "emea"):
            code, j = get_json(f"https://jobicy.com/api/v2/remote-jobs?count=50&geo={geo}&tag={tag}")
            if j is None:
                continue
            for x in j.get("jobs", []):
                geo_txt = x.get("jobGeo") or ""
                out.append(rec("jobicy", x.get("id"), x.get("companyName"), x.get("jobTitle"), x.get("url"),
                               geo_txt, parse_date(x.get("pubDate")), True,
                               f"{x.get('annualSalaryMin','')}–{x.get('annualSalaryMax','')} {x.get('salaryCurrency','')}".strip("– ")
                               if x.get("annualSalaryMax") else "",
                               x.get("annualSalaryMax") if x.get("salaryCurrency") == "EUR" else None,
                               strip_html(x.get("jobDescription") or x.get("jobExcerpt") or "")[:6000],
                               " ".join(x.get("jobType") or []) if isinstance(x.get("jobType"), list) else (x.get("jobType") or ""),
                               "", "jobicy"))
    if not out:
        raise RuntimeError("no results")
    return out


def remoteok():
    code, j = get_json("https://remoteok.com/api")
    if not isinstance(j, list):
        raise RuntimeError(f"HTTP {code}")
    out = []
    for x in j[1:]:
        out.append(rec("remoteok", x.get("id"), x.get("company"), x.get("position"), x.get("url"),
                       x.get("location") or "", parse_date(x.get("date")), True,
                       f"{x.get('salary_min','')}–{x.get('salary_max','')} USD" if x.get("salary_max") else "", None,
                       strip_html(x.get("description") or "")[:6000] + " " + " ".join(x.get("tags") or []), "",
                       "", "remoteok"))
    return out


def himalayas():
    out = []
    for page in range(CFG["boards"]["himalayas_pages"]):
        code, j = get_json(f"https://himalayas.app/jobs/api?limit=20&offset={page*20}")
        if j is None:
            if page == 0:
                raise RuntimeError(f"HTTP {code}")
            break
        jobs = j.get("jobs", [])
        for x in jobs:
            locs = x.get("locationRestrictions") or []
            loc = " / ".join(locs) if locs else "Worldwide"
            out.append(rec("himalayas", x.get("guid") or x.get("applicationLink"), x.get("companyName"), x.get("title"),
                           x.get("applicationLink") or x.get("guid"), loc, parse_date(x.get("pubDate")), True,
                           "", None, strip_html(x.get("description") or x.get("excerpt") or "")[:6000],
                           x.get("employmentType") or "", "", "himalayas"))
        if len(jobs) < 20:
            break
    return out


def adzuna():
    """Adzuna aggregates many German job sites. Needs free keys (GitHub secrets ADZUNA_APP_ID / ADZUNA_APP_KEY)."""
    aid, akey = os.environ.get("ADZUNA_APP_ID"), os.environ.get("ADZUNA_APP_KEY")
    if not aid or not akey:
        raise RuntimeError("no API key set (optional)")
    out = []
    searches = [(q, w) for q in CFG.get("adzuna_queries", ["react", "react native", "frontend", "fullstack typescript", "next.js"])
                for w in ("Essen", "")]
    for what, where in searches:
        for page in (1, 2):
            params = {"app_id": aid, "app_key": akey, "what": what, "results_per_page": 50, "max_days_old": 30,
                      "content-type": "application/json", "sort_by": "date"}
            if where:
                params.update(where=where, distance=60)
            else:
                params["what_or"] = "remote homeoffice"
            code, j = get_json(f"https://api.adzuna.com/v1/api/jobs/de/search/{page}?" + urllib.parse.urlencode(params))
            if j is None:
                if not out:
                    raise RuntimeError(f"HTTP {code}")
                break
            res = j.get("results", [])
            for x in res:
                loc = ((x.get("location") or {}).get("display_name") or "") + " / Deutschland"
                desc = strip_html(x.get("description") or "")
                if re.search(r"remote|home ?office|mobiles arbeiten", desc, re.I):
                    loc += " / Homeoffice"
                out.append(rec("adzuna", x.get("id"), (x.get("company") or {}).get("display_name"), x.get("title"),
                               x.get("redirect_url"), loc, parse_date(x.get("created")), None,
                               (f"{int(x['salary_min'])}–{int(x['salary_max'])} EUR" if x.get("salary_max") else ""),
                               x.get("salary_max") if x.get("salary_is_predicted") in ("0", 0) else None,
                               desc[:3000], (x.get("contract_type") or "") + " " + (x.get("contract_time") or ""),
                               "", "adzuna"))
            if len(res) < 50:
                break
            time.sleep(0.3)
    return out


BOARDS = {"adzuna": adzuna, "arbeitsagentur": arbeitsagentur, "arbeitnow": arbeitnow, "remotive": remotive,
          "jobicy": jobicy, "remoteok": remoteok, "himalayas": himalayas}


# ---------------------------------------------------------------- probing (auto-discovery of company feeds)
def slug_variants(name):
    n = name.lower().replace("&", "and").replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
    n = re.sub(r"[^a-z0-9 .\-]", "", n).strip()
    base = {re.sub(r"[ .]", "", n), re.sub(r"[ .]+", "-", n), re.sub(r"[ .\-]", "", n)}
    extra = set()
    for b in base:
        extra |= {b + "gmbh", b + "-gmbh", b + "hq", b + "-se", b + "ag"}
    return [s for s in list(base) + list(extra) if s]


def probe_one(name, industry):
    for slug in slug_variants(name)[:3]:
        tries = [
            ("ashby", {"slug": slug}),
            ("greenhouse", {"slug": slug}),
            ("greenhouse", {"slug": slug, "eu": True}),
            ("lever", {"slug": slug}),
            ("lever", {"slug": slug, "eu": True}),
            ("recruitee", {"slug": slug}),
            ("workable", {"slug": slug}),
        ]
        for ats, extra in tries:
            c = dict(name=name, industry=industry, ats=ats, **extra)
            try:
                jobs = FETCHERS[ats](c)
            except Exception:
                continue
            if jobs and plausible(jobs):
                return c
    for slug in slug_variants(name):
        c = dict(name=name, industry=industry, ats="personio", slug=slug)
        try:
            jobs = personio(c)
            if jobs and plausible(jobs):
                return c
        except Exception:
            continue
    return None


def plausible(jobs):
    """Guard against slug collisions: the board must have at least one job in Germany/Europe/remote."""
    for j in jobs:
        l = (j.get("location") or "").lower()
        if RE_DE.search(l) or RE_NRW.search(l) or RE_OK_REGION.search(l) or j.get("remote"):
            return True
    return False


PROBE_LIMIT = int(os.environ.get("PROBE_LIMIT", "120"))
GENERIC_EMPLOYER = re.compile(r"jobgether|lemon\.io|toptal|turing|andela|crossover|hays|randstad|adecco|manpower|gulp|"
                              r"personal|recruit|consult|staffing|talent|headhunt|vermittlung|zeitarbeit|jobs? ?gmbh", re.I)


def run_probe(companies, harvested):
    """Find job feeds for new companies: names from candidates.txt + employers seen on job boards."""
    have = {c["name"].lower() for c in companies}
    misses = load("data/probe_misses.json", [])
    recent_miss = {m["name"].lower() for m in misses
                   if (NOW - (parse_date(m.get("date")) or NOW)).days < 30}
    todo = []
    for line in open(os.path.join(ROOT, "candidates.txt"), encoding="utf-8"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        name, _, ind = (p.strip() for p in line.partition("|"))
        if name.lower() not in have and name.lower() not in recent_miss:
            todo.append((name, ind))
            have.add(name.lower())
    for name, cnt in sorted(harvested.items(), key=lambda kv: -kv[1]):
        n = name.strip()
        if len(todo) >= PROBE_LIMIT:
            break
        if not n or len(n) > 60 or GENERIC_EMPLOYER.search(n) or n.lower() in have or n.lower() in recent_miss:
            continue
        todo.append((n, "from job boards"))
        have.add(n.lower())
    todo = todo[:PROBE_LIMIT]
    found = []
    misses = [m for m in misses if m["name"].lower() not in {t[0].lower() for t in todo}]
    with cf.ThreadPoolExecutor(16) as ex:
        for (name, ind), res in zip(todo, ex.map(lambda t: probe_one(*t), todo)):
            if res:
                res["addedBy"] = f"probe {TODAY}"
                found.append(res)
            else:
                misses.append({"name": name, "date": TODAY})
    save("data/probe_misses.json", misses[-5000:])
    STATUS["probe"] = {"tried": len(todo), "found": [f"{c['name']} ({c['ats']}:{c['slug']})" for c in found]}
    return found


def harvest_employers(raw):
    """Employers that post developer jobs in Germany/remote on the boards -> candidates for feed discovery."""
    counts = {}
    for r in raw:
        if r["source"] in FETCHERS:
            continue
        if not title_ok(r["title"], r["_desc"])[0]:
            continue
        l = (r["location"] or "").lower()
        if not (RE_DE.search(l) or RE_NRW.search(l) or r.get("remote")):
            continue
        name = re.sub(r"\s+", " ", (r["company"] or "")).strip()
        if name:
            counts[name] = counts.get(name, 0) + 1
    return counts


# ---------------------------------------------------------------- io
def load(rel, default):
    p = os.path.join(ROOT, rel)
    try:
        return json.load(open(p, encoding="utf-8"))
    except Exception:
        return default


def save(rel, obj):
    p = os.path.join(ROOT, rel)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=1)


def norm_key(r):
    t = (r["title"] or "").lower()
    t = re.sub(r"\s*[|–-]\s*(germany|deutschland|remote|ireland|spain|sweden|uk|united kingdom|netherlands|france|poland|portugal|emea|europe|eu)\b.*$", "", t)
    t = re.sub(r"\((m|w|d|f|x|a|all genders?|gn|mwd|m/w/d|f/m/d|m/f/d|w/m/d|f/m/x|f/m/div)[^)]*\)", "", t)
    c = re.sub(r"\b(gmbh|se|ag|inc|ltd|llc|co|kg)\b", "", (r["company"] or "").lower())
    return re.sub(r"\W+", " ", f"{c}|{t}").strip()


# ---------------------------------------------------------------- main
def main():
    companies = load("data/companies.json", [])
    raw, fails = [], {}

    for name, fn in BOARDS.items():
        try:
            jobs = fn()
            raw += jobs
            STATUS["sources"][name] = {"fetched": len(jobs)}
        except Exception as e:
            STATUS["sources"][name] = {"error": str(e)[:160]}

    if "--no-probe" not in sys.argv:
        companies += run_probe(companies, harvest_employers(raw))

    def fetch_company(c):
        return c, FETCHERS[c["ats"]](c)

    active = [c for c in companies if not c.get("disabled")]
    with cf.ThreadPoolExecutor(16) as ex:
        futs = {ex.submit(fetch_company, c): c for c in active}
        for f in cf.as_completed(futs):
            c = futs[f]
            try:
                _, jobs = f.result()
                raw += jobs
                c["lastOk"] = TODAY
                c.pop("failCount", None)
            except Exception as e:
                c["failCount"] = c.get("failCount", 0) + 1
                fails[c["name"]] = str(e)[:80]
                if c["failCount"] >= 5:
                    c["disabled"] = True
    STATUS["sources"]["companies"] = {"checked": len(active), "failed": len(fails)}
    STATUS["errors"]["companies"] = fails

    # filter
    matches, seen_keys = [], set()
    for r in raw:
        if not r.get("url") or not r.get("title"):
            continue
        a = accept(r)
        if not a:
            continue
        dedupe = norm_key(a)
        if dedupe in seen_keys:
            continue
        seen_keys.add(dedupe)
        a.pop("_desc", None)
        matches.append(a)

    seen = load("data/seen.json", {})
    new = []
    for m in matches:
        key = norm_key(m)
        first = seen.get(m["id"]) or seen.get("k:" + key)
        if not first:
            first = NOW.isoformat(timespec="seconds")
            seen[m["id"]] = first
            seen["k:" + key] = first
            new.append(m)
        m["firstSeen"] = first
    # prune memory older than 150 days
    cutoff = (NOW - dt.timedelta(days=150)).isoformat()
    seen = {k: v for k, v in seen.items() if v >= cutoff}

    matches.sort(key=lambda m: m["firstSeen"], reverse=True)
    save("out/latest.json", {"collectedAt": NOW.isoformat(timespec="seconds"), "count": len(matches), "jobs": matches})
    save("out/new.json", {"collectedAt": NOW.isoformat(timespec="seconds"), "count": len(new), "jobs": new})
    save("data/seen.json", seen)
    save("data/companies.json", companies)
    STATUS.update({"collectedAt": NOW.isoformat(timespec="seconds"), "rawJobs": len(raw), "matches": len(matches),
                   "new": len(new), "companies": len(companies), "activeCompanies": len(active)})
    save("out/status.json", STATUS)
    print(json.dumps({k: STATUS[k] for k in ("rawJobs", "matches", "new", "companies")}, indent=1))
    print(json.dumps(STATUS["sources"], indent=1))


if __name__ == "__main__":
    main()
