import math
from collections import defaultdict
from typing import Dict, List, Any, Tuple

import pandas as pd

from data_service import ShopfloorScheduler
from ishopfloor_api import ShopfloorAPI


WEIGHT_OFF = 1_000_000_000
WEIGHT_NOT_OWN_LINE = 10
WEIGHT_YTD_OT = 10_000_000
WEIGHT_MONTH_OT = 100


def _to_dict_records(task_data: List[Any]) -> List[Dict[str, Any]]:
    records = []
    for item in task_data:
        if isinstance(item, dict):
            records.append(item)
        elif hasattr(item, "model_dump"):
            records.append(item.model_dump())
        else:
            records.append(item.__dict__)
    return records


def _normalize_line_skill_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()
    out["RequiredCount"] = pd.to_numeric(out["RequiredCount"], errors="coerce").fillna(0.0)
    if "LineSkillPreferGender" not in out.columns:
        out["LineSkillPreferGender"] = "M&F"
    out["LineSkillPreferGender"] = (
        out["LineSkillPreferGender"]
        .fillna("M&F")
        .astype(str)
        .str.strip()
        .replace({"": "M&F", "Any": "M&F", "ANY": "M&F"})
    )

    return out


def _build_line_group_maps(all_group_line_df: pd.DataFrame, task_records: List[Dict[str, Any]]):
    line_to_group = {}
    line_name_to_group = {}

    if not all_group_line_df.empty:
        for _, r in all_group_line_df.iterrows():
            gid = str(r.get("LineGroup", ""))
            lid = str(r.get("LineID", ""))
            lname = str(r.get("LineName", ""))
            if lid:
                line_to_group[lid] = gid
            if lname:
                line_name_to_group[lname] = gid

    for r in task_records:
        gid = str(r.get("LineGroup", ""))
        lid = str(r.get("LineID", ""))
        lname = str(r.get("LineName", ""))
        if lid and gid:
            line_to_group[lid] = gid
        if lname and gid:
            line_name_to_group[lname] = gid

    return line_to_group, line_name_to_group


def _round_requirements(req_df: pd.DataFrame, line_to_group: Dict[str, str]) -> pd.DataFrame:
    if req_df.empty:
        return req_df

    df = req_df.copy()
    df["LineID"] = df["LineID"].astype(str)
    df["EmpType"] = df["EmpType"].fillna("regular").astype(str).str.lower()
    df["LineGroup"] = df["LineID"].map(line_to_group).fillna(df.get("LineGroup", ""))

    int_part = df["RequiredCount"].apply(math.floor)
    frac_part = df["RequiredCount"] - int_part
    df["NeedRounded"] = int_part.astype(int)
    df["_frac"] = frac_part

    keys = ["LineGroup", "DorN", "OJTCode", "EmpType"]
    for _, g in df.groupby(keys, dropna=False):
        frac_sum = g["_frac"].sum()
        add_cnt = int(math.ceil(frac_sum - 1e-9))
        if add_cnt <= 0:
            continue
        ordered_idx = g.sort_values("_frac", ascending=False).index.tolist()
        for idx in ordered_idx[:add_cnt]:
            df.loc[idx, "NeedRounded"] += 1

    return df.drop(columns=["_frac"])


def _gender_match_and_penalty(emp_gender: str, prefer_gender: str):
    g = str(emp_gender or "").strip().upper()
    p = str(prefer_gender or "").strip().upper()

    if p == "M":
        return g == "M", 0
    if p == "F":
        return g == "F", 0
    if p in {"M&F", "F&M", "MF", "FM", "M/F", "F/M"}:
        return g in {"M", "F"}, 0
    return True, 0


def _weight(emp: Dict[str, Any], slot: Dict[str, Any], is_off: bool) -> float:
    w = 0
    line = str(emp.get("Line", ""))
    target_line = str(slot["LineID"])
    gender = str(emp.get("Gender", "")).upper()
    prefer_gender = str(slot.get("PreferGender", "Any")).upper()

    is_match, gender_penalty = _gender_match_and_penalty(gender, prefer_gender)
    if not is_match:
        return float("inf")

    if is_off:
        w += WEIGHT_OFF

    if line not in {target_line, "All", "ALL", "all"}:
        w += WEIGHT_NOT_OWN_LINE

    w += gender_penalty
    w += float(emp.get("YTDOTAvgNew", 0) or 0) * WEIGHT_YTD_OT
    w += float(emp.get("MonthOTNew", 0) or 0) * WEIGHT_MONTH_OT
    return w


def _build_slots(req_df: pd.DataFrame, emp_type_scope: str) -> List[Dict[str, Any]]:
    slots = []
    for _, r in req_df.iterrows():
        etype = str(r.get("EmpType", "regular")).lower()
        if emp_type_scope == "regular_only" and etype != "regular":
            continue
        need = int(r.get("NeedRounded", 0) or 0)
        for i in range(need):
            slots.append({
                "slot_id": f"{r['DorN']}-{r['LineID']}-{r['OJTCode']}-{etype}-{i}",
                "DorN": r["DorN"],
                "LineID": str(r["LineID"]),
                "LineName": r.get("LineName", ""),
                "LineGroup": r.get("LineGroup", ""),
                "OJTCode": r["OJTCode"],
                "SkillName": r.get("SkillName", ""),
                "EmpTypeNeed": etype,
                "PreferGender": r.get("LineSkillPreferGender", "Any"),
            })
    return slots


def _pick_candidates(emp_df: pd.DataFrame, slot: Dict[str, Any], used: set,
                     line_to_group: Dict[str, str], line_name_to_group: Dict[str, str]) -> List[Dict[str, Any]]:
    if emp_df.empty:
        return []
    skill_df = emp_df[emp_df["OJTCode"].astype(str) == str(slot["OJTCode"])]
    if skill_df.empty:
        return []

    target_group = line_to_group.get(str(slot["LineID"]), slot.get("LineGroup", ""))

    rows = []
    for _, r in skill_df.iterrows():
        pid = str(r.get("PersonNo", ""))
        if not pid or pid in used:
            continue
        emp_line = str(r.get("Line", ""))
        if emp_line not in {"All", "ALL", "all", str(slot["LineID"])}:
            emp_group = line_to_group.get(emp_line) or line_name_to_group.get(emp_line)
            if target_group and emp_group and emp_group != target_group:
                continue
        rows.append(r.to_dict())
    return rows


def _assign_slots(slots: List[Dict[str, Any]], normal_df: pd.DataFrame, off_df: pd.DataFrame,
                  line_to_group: Dict[str, str], line_name_to_group: Dict[str, str],
                  allow_off: bool, used_initial: set = None) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], set]:
    used = set(used_initial or set())
    assignments = []
    unfilled = []

    for slot in slots:
        cands = []
        for c in _pick_candidates(normal_df, slot, used, line_to_group, line_name_to_group):
            w = _weight(c, slot, is_off=False)
            if math.isfinite(w):
                cands.append((w, c, False))

        if allow_off:
            for c in _pick_candidates(off_df, slot, used, line_to_group, line_name_to_group):
                w = _weight(c, slot, is_off=True)
                if math.isfinite(w):
                    cands.append((w, c, True))

        if not cands:
            unfilled.append(slot)
            continue

        cands.sort(key=lambda x: x[0])
        w, best, is_off = cands[0]
        used.add(str(best["PersonNo"]))
        assignments.append({
            "PersonNo": best["PersonNo"],
            "LineID": slot["LineID"],
            "LineName": slot["LineName"],
            "DorN": slot["DorN"],
            "OJTCode": slot["OJTCode"],
            "SkillName": slot["SkillName"],
            "EmpTypeNeed": slot["EmpTypeNeed"],
            "PreferGender": slot.get("PreferGender", ""),
            "LineGroup": slot.get("LineGroup", ""),
            "IsOT": is_off,
            "Weight": w,
        })

    return assignments, unfilled, used


def _group_shortage(unfilled: List[Dict[str, Any]]) -> Dict[tuple, int]:
    stat = defaultdict(int)
    for s in unfilled:
        key = (s["DorN"], s["LineID"], s["LineName"], s["OJTCode"], s["SkillName"])
        stat[key] += 1
    return stat


def _build_task_meta(records: List[Dict[str, Any]]):
    first = records[0] if records else {}
    line_meta = {}
    for r in records:
        key = (str(r.get("DorN", "")), str(r.get("LineID", "")))
        line_meta[key] = {
            "Year": str(r.get("Year", first.get("Year", ""))),
            "Month": str(r.get("Month", first.get("Month", ""))),
            "Date": str(r.get("Date", first.get("Date", ""))),
            "Dept": r.get("Dept", first.get("Dept", "")),
            "SubDept": r.get("SubDept", first.get("SubDept", "")),
            "LineGroup": r.get("LineGroup", ""),
            "ShiftCode": r.get("ShiftCode", ""),
            "LineName": r.get("LineName", ""),
        }
    return {
        "default": {
            "Year": str(first.get("Year", "")),
            "Month": str(first.get("Month", "")),
            "Date": str(first.get("Date", "")),
            "Dept": first.get("Dept", ""),
            "SubDept": first.get("SubDept", ""),
        },
        "line_meta": line_meta,
    }


def _meta_for_slot(meta: Dict[str, Any], dorn: str, line_id: str, line_name: str, line_group: str):
    m = {}
    m.update(meta.get("default", {}))
    pick = meta.get("line_meta", {}).get((str(dorn), str(line_id)), {})
    if pick:
        m.update(pick)
    m.setdefault("LineName", line_name)
    m.setdefault("LineGroup", line_group)
    m.setdefault("ShiftCode", "")
    return m


def _format_assigned(assignments: List[Dict[str, Any]], meta: Dict[str, Any]):
    out = []
    for a in assignments:
        m = _meta_for_slot(meta, a.get("DorN", ""), a.get("LineID", ""), a.get("LineName", ""), a.get("LineGroup", ""))
        out.append({
            "Year": m.get("Year", ""),
            "Month": m.get("Month", ""),
            "Date": m.get("Date", ""),
            "DorN": a.get("DorN", ""),
            "Dept": m.get("Dept", ""),
            "SubDept": m.get("SubDept", ""),
            "LineGroup": m.get("LineGroup", a.get("LineGroup", "")),
            "LineID": a.get("LineID", ""),
            "LineName": a.get("LineName", ""),
            "ShiftCode": m.get("ShiftCode", ""),
            "OJTCode": a.get("OJTCode", ""),
            "SkillName": a.get("SkillName", ""),
            "PreferGender": a.get("PreferGender", ""),
            "EmpNo": a.get("PersonNo", ""),
        })
    return out


def _format_shortage(shortage_map: Dict[tuple, int], slot_meta_map: Dict[str, Dict[str, Any]], meta: Dict[str, Any]):
    out = []
    for k, v in shortage_map.items():
        if v <= 0:
            continue
        dorn, line_id, line_name, ojt, skill = k
        slot_m = slot_meta_map.get(f"{dorn}|{line_id}|{ojt}", {})
        m = _meta_for_slot(meta, dorn, line_id, line_name, slot_m.get("LineGroup", ""))
        out.append({
            "Year": m.get("Year", ""),
            "Month": m.get("Month", ""),
            "Date": m.get("Date", ""),
            "DorN": dorn,
            "Dept": m.get("Dept", ""),
            "SubDept": m.get("SubDept", ""),
            "LineGroup": m.get("LineGroup", slot_m.get("LineGroup", "")),
            "LineID": line_id,
            "LineName": line_name,
            "ShiftCode": m.get("ShiftCode", ""),
            "OJTCode": ojt,
            "SkillName": skill,
            "PreferGender": slot_m.get("PreferGender", ""),
            "MissingCount": v,
        })
    return out


def _format_outsourcing(req_df: pd.DataFrame, meta: Dict[str, Any]):
    out = []
    out_req = req_df[req_df["EmpType"].str.lower() == "outsourcing"] if not req_df.empty else req_df
    for _, r in out_req.iterrows():
        dorn = r.get("DorN", "")
        line_id = str(r.get("LineID", ""))
        line_name = r.get("LineName", "")
        m = _meta_for_slot(meta, dorn, line_id, line_name, r.get("LineGroup", ""))
        out.append({
            "Year": m.get("Year", ""),
            "Month": m.get("Month", ""),
            "Date": m.get("Date", ""),
            "DorN": dorn,
            "Dept": m.get("Dept", ""),
            "SubDept": m.get("SubDept", ""),
            "LineGroup": m.get("LineGroup", r.get("LineGroup", "")),
            "LineID": line_id,
            "LineName": line_name,
            "ShiftCode": m.get("ShiftCode", ""),
            "OJTCode": r.get("OJTCode", ""),
            "SkillName": r.get("SkillName", ""),
            "PreferGender": r.get("LineSkillPreferGender", ""),
            "OutSourcingCount": int(r.get("NeedRounded", 0) or 0),
        })
    return out

def _build_result(assignments: List[Dict[str, Any]], shortage_map: Dict[tuple, int], meta: Dict[str, Any], slot_meta_map: Dict[str, Dict[str, Any]], extra=None):
    out = {
        "assigned": _format_assigned(assignments, meta),
        "shortage": _format_shortage(shortage_map, slot_meta_map, meta),
    }
    if extra is not None:
        out.update(extra)
    return out


def _prepare_context(task_data):
    records = _to_dict_records(task_data)
    if not records:
        return None
    first = records[0]

    api = ShopfloorAPI()
    scheduler = ShopfloorScheduler(
        api_instance=api,
        dept=first["Dept"],
        sub_dept=first["SubDept"],
        year=first["Year"],
        month=first["Month"],
        day=first["Date"],
    )

    all_group_line_df, day_req, night_req = scheduler.get_line_requirements(records)
    req_df = pd.concat([day_req, night_req], ignore_index=True)
    req_df = _normalize_line_skill_df(req_df)

    line_to_group, line_name_to_group = _build_line_group_maps(all_group_line_df, records)
    req_df = _round_requirements(req_df, line_to_group)

    s121, s122, day_off, night_only_off = scheduler.prepare_emp_resources()

    task_meta = _build_task_meta(records)
    slot_meta_map = {}
    for _, r in req_df.iterrows():
        key = f"{r.get('DorN', '')}|{str(r.get('LineID', ''))}|{r.get('OJTCode', '')}"
        slot_meta_map[key] = {
            "PreferGender": r.get("LineSkillPreferGender", ""),
            "LineGroup": r.get("LineGroup", ""),
        }

    return req_df, s121, s122, day_off, night_only_off, line_to_group, line_name_to_group, task_meta, slot_meta_map


def _outsourcing_quota(req_df: pd.DataFrame, dorn: str) -> defaultdict:
    out_req = req_df[(req_df["DorN"] == dorn) & (req_df["EmpType"].str.lower() == "outsourcing")]
    quota = defaultdict(int)
    for _, r in out_req.iterrows():
        quota[(r["DorN"], str(r["LineID"]), r["OJTCode"])] += int(r["NeedRounded"])
    return quota


def _run_mode_for_shift(mode: str, req_df: pd.DataFrame, dorn: str,
                        normal_df: pd.DataFrame, off_df: pd.DataFrame,
                        line_to_group: Dict[str, str], line_name_to_group: Dict[str, str]):
    shift_req = req_df[req_df["DorN"] == dorn]

    if mode == "rule3":
        slots = _build_slots(shift_req, emp_type_scope="regular_only")
        assign, unfilled, _ = _assign_slots(slots, normal_df, off_df, line_to_group, line_name_to_group, allow_off=True)
        return assign, unfilled

    slots = _build_slots(shift_req, emp_type_scope="all")

    if mode == "rule1":
        assign1, unfilled1, _ = _assign_slots(slots, normal_df, off_df.iloc[0:0], line_to_group, line_name_to_group, allow_off=False)
        quota = _outsourcing_quota(req_df, dorn)
        remain = []
        for s in unfilled1:
            k = (s["DorN"], s["LineID"], s["OJTCode"])
            if quota[k] > 0:
                quota[k] -= 1
            else:
                remain.append(s)
        assign2, unfilled2, _ = _assign_slots(remain, normal_df.iloc[0:0], off_df, line_to_group, line_name_to_group, allow_off=True)
        return assign1 + assign2, unfilled2

    # mode == rule2
    assign, unfilled, _ = _assign_slots(slots, normal_df, off_df, line_to_group, line_name_to_group, allow_off=True)
    quota = _outsourcing_quota(req_df, dorn)
    remain = []
    for s in unfilled:
        k = (s["DorN"], s["LineID"], s["OJTCode"])
        if quota[k] > 0:
            quota[k] -= 1
        else:
            remain.append(s)
    return assign, remain


def _df_to_people(df: pd.DataFrame):
    if df is None or df.empty:
        return []
    cols = [c for c in ["PersonNo", "Gender", "Line", "OJTCode", "SkillName", "MonthOTNew", "YTDOTAvgNew", "SapShiftCode"] if c in df.columns]
    out = df[cols].drop_duplicates().copy()
    out = out.rename(columns={"PersonNo": "EmpNo"})
    return out.to_dict("records")


def _run_day_then_night(mode: str, ctx):
    req_df, s121, s122, day_off, night_only_off, line_to_group, line_name_to_group, task_meta, slot_meta_map = ctx

    # 1) 先排白班 D：normal=s121, off=day_and_night_off
    day_assign, day_unfilled = _run_mode_for_shift(mode, req_df, "D", s121, day_off, line_to_group, line_name_to_group)

    # D 班如果用了 day_off, N 班不能再用这些人
    day_off_ids = set(day_off["PersonNo"].astype(str).tolist()) if not day_off.empty else set()
    used_day_off_ids = {str(a["PersonNo"]) for a in day_assign if a.get("IsOT") and str(a.get("PersonNo")) in day_off_ids}
    remain_day_off = day_off[~day_off["PersonNo"].astype(str).isin(used_day_off_ids)] if not day_off.empty else day_off

    # 2) 再排夜班 N：normal=s122, off=remain_day_off + only_night_off
    n_off = pd.concat([remain_day_off, night_only_off], ignore_index=True)
    night_assign, night_unfilled = _run_mode_for_shift(mode, req_df, "N", s122, n_off, line_to_group, line_name_to_group)

    # 3) 返回每个班别剩余可用人员（normal + off）
    used_day_normal_ids = {str(a["PersonNo"]) for a in day_assign if not a.get("IsOT")}
    used_day_ot_ids = {str(a["PersonNo"]) for a in day_assign if a.get("IsOT")}

    day_normal_remain_df = s121[~s121["PersonNo"].astype(str).isin(used_day_normal_ids)] if not s121.empty else s121
    day_off_remain_df = day_off[~day_off["PersonNo"].astype(str).isin(used_day_ot_ids)] if not day_off.empty else day_off

    used_night_normal_ids = {str(a["PersonNo"]) for a in night_assign if not a.get("IsOT")}
    used_night_ot_ids = {str(a["PersonNo"]) for a in night_assign if a.get("IsOT")}

    night_normal_remain_df = s122[~s122["PersonNo"].astype(str).isin(used_night_normal_ids)] if not s122.empty else s122
    night_off_remain_df = n_off[~n_off["PersonNo"].astype(str).isin(used_night_ot_ids)] if not n_off.empty else n_off

    remain_payload = {
        "remainingAvailable": {
            "D_s121": _df_to_people(day_normal_remain_df),
            "N_s122": _df_to_people(night_normal_remain_df),
            "D_N_off": _df_to_people(day_off_remain_df),
            "only_N_off": _df_to_people(night_only_off if not night_only_off.empty else night_only_off),
        }
    }

    return day_assign + night_assign, day_unfilled + night_unfilled, remain_payload, task_meta, slot_meta_map


def inline_rule_one(task_data):
    """Regular>Outsourcing>OT"""
    ctx = _prepare_context(task_data)
    if not ctx:
        return {"assigned": [], "shortage": [], "outsourcingDemand": [], "remainingAvailable": {"D_s121": [], "N_s122": [], "D_N_off": [], "only_N_off": []}}

    assignments, unfilled, remain_payload, task_meta, slot_meta_map = _run_day_then_night("rule1", ctx)
    return _build_result(assignments, _group_shortage(unfilled), task_meta, slot_meta_map, {"outsourcingDemand": [], **remain_payload})


def inline_rule_two(task_data):
    """Regular>OT>Outsourcing"""
    ctx = _prepare_context(task_data)
    if not ctx:
        return {"assigned": [], "shortage": [], "outsourcingDemand": [], "remainingAvailable": {"D_s121": [], "N_s122": [], "D_N_off": [], "only_N_off": []}}

    assignments, unfilled, remain_payload, task_meta, slot_meta_map = _run_day_then_night("rule2", ctx)
    return _build_result(assignments, _group_shortage(unfilled), task_meta, slot_meta_map, {"outsourcingDemand": [], **remain_payload})


def inline_rule_three(task_data):
    """Outsourcing>Regular>OT"""
    ctx = _prepare_context(task_data)
    if not ctx:
        return {"assigned": [], "shortage": [], "outsourcingDemand": [], "remainingAvailable": {"D_s121": [], "N_s122": [], "D_N_off": [], "only_N_off": []}}

    assignments, unfilled, remain_payload, task_meta, slot_meta_map = _run_day_then_night("rule3", ctx)

    req_df = ctx[0]
    out_demand = _format_outsourcing(req_df, task_meta)

    extra = {"outsourcingDemand": out_demand}
    extra.update(remain_payload)
    return _build_result(assignments, _group_shortage(unfilled), task_meta, slot_meta_map, extra)

if __name__ == "__main__":
    task_data =   [
        {"Year": "2026", "Month": "03", "Date": "27", "DorN": "D", "Dept": "ME/MOE6-CN", "SubDept": "ME/MFO6.11-CN",
         "LineGroup": "Testing", "LineID": "11", "LineName": "BSTesting01", "ShiftCode": "S121"},
        {"Year": "2026", "Month": "03", "Date": "27", "DorN": "N", "Dept": "ME/MOE6-CN", "SubDept": "ME/MFO6.11-CN",
         "LineGroup": "Testing", "LineID": "11", "LineName": "BSTesting01", "ShiftCode": "S122"}
     ]
    output_three = inline_rule_three(task_data)
    print(output_three)
