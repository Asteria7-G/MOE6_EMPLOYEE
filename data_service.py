import bisect
import pandas as pd
from datetime import datetime, timedelta



class ShopfloorScheduler:
    def __init__(self, api_instance, dept, sub_dept, year, month, day):
        self.api = api_instance
        self.dept = dept
        self.sub_dept = sub_dept
        self.year = str(year)
        self.month = str(month)
        self.day = str(day)
        self.today = datetime(int(year), int(month), int(day)).date()

        # 内部缓存
        self.leave_codes = set()
        self.emp_status_df = pd.DataFrame()
        self.person_off_dict = {}

    @staticmethod
    def _normalize_int_str(value):
        """把 '03' / 3 / '3.0' 统一成 '3'，用于日期字段比对。"""
        if pd.isna(value):
            return ""
        s = str(value).strip()
        if not s:
            return ""
        try:
            return str(int(float(s)))
        except ValueError:
            return s

    def _fetch_base_data(self):
        """内部方法：统一拉取所有必要的基础数据"""
        # 获取休假代码
        l_df = self.api.get_sap_leave_code_df()
        self.leave_codes = {str(i).strip().lower() for i in l_df['SapShiftCode'].unique()}

        # 获取员工状态
        self.emp_status_df = self.api.get_employee_working_status_df(
            self.dept, self.sub_dept, ['Shopfloor leader', 'OP', 'MH', 'VI']
        )
        self.emp_status_df['Date'] = pd.to_datetime(
            self.emp_status_df[['Year', 'Month', 'Day']]
        ).dt.date

        # 构建离岗字典
        off_records = self.emp_status_df[
            self.emp_status_df['SapShiftCode'].str.strip().str.lower().isin(self.leave_codes)
        ]
        self.person_off_dict = off_records.groupby('PersonNo')['Date'].apply(
            lambda x: sorted(list(x))
        ).to_dict()

    def _check_consecutive_work(self, person_id, max_consecutive=6):
        """校验连续上班天数是否合规"""
        off_days = self.person_off_dict.get(person_id, [])
        temp_off_days = [d for d in off_days if d != self.today]

        if not temp_off_days:
            return False

        idx = bisect.bisect_left(temp_off_days, self.today)
        prev_off = temp_off_days[idx - 1] if idx > 0 else None
        next_off = temp_off_days[idx] if idx < len(temp_off_days) else None

        if prev_off and next_off:
            return (next_off - prev_off).days - 1 <= max_consecutive
        elif prev_off:
            return (self.today - prev_off).days <= max_consecutive
        elif next_off:
            return (next_off - self.today).days <= max_consecutive
        return False

    def get_line_requirements(self, inline_data: list):
        """处理线体需求数据"""
        task_df = pd.DataFrame(inline_data)
        if task_df.empty: return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        task_df["LineID"] = task_df["LineID"].astype(str)

        # 批量获取线体信息和技能
        line_groups = task_df['LineGroup'].unique()
        all_group_line_df = self.api.get_line_by_dept_and_subdept_df(self.dept, self.sub_dept)

        line_ids = task_df['LineID'].unique()
        all_line_skill_df = pd.concat([
        self.api.get_line_skill_df(lid)
        for lid in line_ids if str(lid).strip()
        ], ignore_index=True)

        merged_task = pd.merge(

            task_df,

            all_line_skill_df[['LineID', 'OJTCode', 'SkillName', 'EmpType', 'RequiredCount']],

            on='LineID', how='left'

        ).rename(columns={'Date': 'Day'})

        return (

            all_group_line_df,

            merged_task[merged_task['DorN'] == 'D'].reset_index(drop=True),

            merged_task[merged_task['DorN'] == 'N'].reset_index(drop=True)

        )

    def prepare_emp_resources(self):
        """准备可用的员工资源池"""
        self._fetch_base_data()

        # 1. 获取 OT 和 技能数据
        emp_ot_df = self.api.get_employee_ot_df(self.dept, self.sub_dept)
        emp_skill_df = self.api.get_employee_skill_df(self.dept, self.sub_dept)

        # 2. 筛选当日状态
        year_v = self._normalize_int_str(self.year)
        month_v = self._normalize_int_str(self.month)
        day_v = self._normalize_int_str(self.day)

        year_col = self.emp_status_df['Year'].map(self._normalize_int_str)
        month_col = self.emp_status_df['Month'].map(self._normalize_int_str)
        day_col = self.emp_status_df['Day'].map(self._normalize_int_str)
        today_mask = (year_col == year_v) & (month_col == month_v) & (day_col == day_v)

        df = self.emp_status_df[today_mask].copy()

        # 3. 标记 OFF 并过滤
        df.loc[df['SapShiftCode'].str.strip().str.lower().isin(self.leave_codes), 'SapShiftCode'] = 'OFF'
        df = df[df['SapShiftCode'].isin(['S121', 'S122', 'OFF'])]

        # 4. 关联 OT/技能 并按阈值过滤
        df = df.merge(emp_ot_df[['PersonNo', 'MonthOTNew', 'YTDOTAvgNew']], on='PersonNo', how='left') \
            .merge(emp_skill_df[['PersonNo', 'OJTCode', 'SkillName']], on='PersonNo', how='left')

        df = df[(df['YTDOTAvgNew'] <= 35) & (df['MonthOTNew'] <= 90)]

        # 5. 分类导出
        s121 = df[df['SapShiftCode'] == 'S121'].reset_index(drop=True)
        s122 = df[df['SapShiftCode'] == 'S122'].reset_index(drop=True)

        # 6. 处理 OFF 班次细分
        off_raw = df[df['SapShiftCode'] == 'OFF'].copy()
        valid_off_ids = [pid for pid in off_raw['PersonNo'].unique() if self._check_consecutive_work(pid)]
        off_df = off_raw[off_raw['PersonNo'].isin(valid_off_ids)]

        yesterday = self.today - timedelta(days=1)
        yest_shift = self.emp_status_df[self.emp_status_df['Date'] == yesterday][['PersonNo', 'Shift']]

        off_merged = off_df.merge(yest_shift, on='PersonNo', how='left', suffixes=('', '_Y'))

        only_night_off = off_merged[off_merged['Shift_Y'] == 'N'].reset_index(drop=True)
        day_night_off = off_merged[off_merged['Shift_Y'] != 'N'].reset_index(drop=True)

        return s121, s122, day_night_off, only_night_off


# --- 使用示例 ---
# scheduler = ShopfloorScheduler(api, "ME/MOE6-CN", "ME/MFO6.5-CN", 2026, 3, 17)
# s121, s122, day_night_off, only_night_off = scheduler.prepare_emp_resources()

if __name__ == "__main__":
    from ishopfloor_api import ShopfloorAPI
    api = ShopfloorAPI()
    scheduler = ShopfloorScheduler(api, "ME/MOE6-CN", "ME/MFO6.11-CN","2026","03","27")
    s121, s122, day_night_off, only_night_off = scheduler.prepare_emp_resources()
    s121.to_excel("./data_preprocessing/s121_emp_df_class.xlsx", index=False)
    s122.to_excel("./data_preprocessing/s122_emp_df_class.xlsx", index=False)
    day_night_off.to_excel("./data_preprocessing/day_night_off_emp_df_class.xlsx", index=False)
    only_night_off.to_excel(
        "./data_preprocessing/only_night_off_emp_df_class.xlsx",index=False)
    inline_data = [
            {"Year": "2026", "Month": "03", "Date": "27", "DorN": "D", "Dept": "ME/MOE6-CN", "SubDept": "ME/MFO6.11-CN",
             "LineGroup": "Testing", "LineID": "11", "LineName": "BSTesting01", "ShiftCode": "S121"},
            {"Year": "2026", "Month": "03", "Date": "27", "DorN": "N", "Dept": "ME/MOE6-CN", "SubDept": "ME/MFO6.11-CN",
             "LineGroup": "Testing", "LineID": "11", "LineName": "BSTesting01", "ShiftCode": "S122"}]

    all_group_line_df, day_inline_task_with_skill_df, night_inline_task_with_skill_df = scheduler.get_line_requirements(inline_data)
    all_group_line_df.to_excel(
        "./data_preprocessing/all_group_line_df_class.xlsx", index=False)
    day_inline_task_with_skill_df.to_excel(
        "./data_preprocessing/day_inline_task_with_skill_df_class.xlsx", index=False)
    night_inline_task_with_skill_df.to_excel(
        "./data_preprocessing/night_inline_task_with_skill_df_class.xlsx", index=False)
