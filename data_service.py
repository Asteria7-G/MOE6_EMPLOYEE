# import bisect
# import pandas as pd
# from datetime import datetime, timedelta
# from collections import defaultdict
# from ishopfloor_api import ShopfloorAPI
#
# api = ShopfloorAPI()
#
#
# # 线体数据处理
# def inline_task_df(inline_data:list) -> pd.DataFrame:
#     """
#     {"mode":"Regular>Outsourcing>OT",
#     "type":"InLineTask",
#     "data":[{"Year":"2026","Month":"3","Date":"17","DorN":"D","Dept":"ME/MOE6-CN","SubDept":"ME/MFO6.5-CN",
#     "LineGroup":"FA","LineID":"","LineName":"BSFA13","ShiftCode":"NORM"},
#     {"Year":"2026","Month":"3","Date":"17","DorN":"N","Dept":"ME/MOE6-CN","SubDept":"ME/MFO6.5-CN",
#     "LineGroup":"FA","LineID":"","LineName":"BSFA13","ShiftCode":"S082"}]}
#     """
#     inline_task_df = pd.DataFrame(inline_data)
#     # 收集开班的group
#     line_group_list = inline_task_df['LineGroup'].unique().tolist()
#     all_group_line_df_list = []
#     # 用大部门和小部门+group来获取 线体信息
#     for line_group in line_group_list:
#         dept = inline_task_df['Dept'][0]
#         sub_dept = inline_task_df['SubDept'][0]
#         group_line_df = api.get_line_by_dept_and_group_df(dept,sub_dept,line_group)
#         all_group_line_df_list.append(group_line_df)
#     all_group_line_df = pd.concat(all_group_line_df_list, ignore_index=True)
#
#     line_id_list = inline_task_df['LineID'].unique().tolist()
#
#     all_line_skill_df_list = []  # 用来存每个 line_id 的 df
#
#     for line_id in line_id_list:
#         line_skill_df = api.get_line_skill_df(line_id)  # 获取每条线的 df
#         all_line_skill_df_list.append(line_skill_df)
#     # 所有开办线体所需的技能人数
#     all_line_skill_df = pd.concat(all_line_skill_df_list, ignore_index=True)
#     # 拼接形成大的技能需求表
#     inline_task_with_skill_df = pd.merge(
#         inline_task_df,
#         all_line_skill_df[['LineID', 'OJTCode', 'SkillName','EmpType','RequiredCount']],
#         on='LineID',
#         how='left'
#     )
#
#     print(inline_task_with_skill_df.shape)
#     print(inline_task_with_skill_df.head())
#     day_inline_task_with_skill_df = inline_task_with_skill_df[
#         inline_task_with_skill_df['DorN'] == 'D'
#         ].reset_index(drop=True)
#     day_inline_task_with_skill_df = day_inline_task_with_skill_df.rename(columns={'Date':'Day'})
#     night_inline_task_with_skill_df = inline_task_with_skill_df[
#         inline_task_with_skill_df['DorN'] == 'N'
#         ].reset_index(drop=True)
#     night_inline_task_with_skill_df = night_inline_task_with_skill_df.rename(columns={'Date': 'Day'})
#
#     return all_group_line_df, day_inline_task_with_skill_df, night_inline_task_with_skill_df
#
# # 收集员工off的日期存成dict
# def build_person_off_dict(emp_status_df, leave_code_list):
#
#     leave_code_set = {str(i).strip().lower() for i in leave_code_list}
#
#     person_off_dict = defaultdict(list)
#
#     for row in emp_status_df.itertuples():
#         code = str(row.SapShiftCode).strip().lower()
#         if code in leave_code_set:
#             person_off_dict[row.PersonNo].append(row.Date)
#
#     # 排序（非常关键）
#     for person in person_off_dict:
#         person_off_dict[person].sort()
#
#     return person_off_dict
#
# # 二分法找到前一个休息日和后一个休息日 算连续上班的日子
# def can_change_off_to_work(person, today, person_off_dict, max_consecutive=6):
#
#     off_days = person_off_dict.get(person, [])
#
#     if today in off_days:
#         temp_off_days = off_days.copy()
#         temp_off_days.remove(today)
#     else:
#         temp_off_days = off_days
#
#     if not temp_off_days:
#         # 没有任何休息记录 → 理论上无限连续
#         return False
#
#     # 二分查找 today 在休息列表中的位置
#     idx = bisect.bisect_left(temp_off_days, today)
#
#     # 找前一个休息日
#     prev_off = temp_off_days[idx - 1] if idx > 0 else None
#
#     # 找后一个休息日
#     next_off = temp_off_days[idx] if idx < len(temp_off_days) else None
#
#     # 计算连续工作天数
#     if prev_off and next_off:
#         consecutive_days = (next_off - prev_off).days - 1
#     elif prev_off:
#         consecutive_days = (today - prev_off).days
#     elif next_off:
#         consecutive_days = (next_off - today).days
#     else:
#         return False
#
#     return consecutive_days <= max_consecutive
#
#
# #人员数据处理
# def inline_task_emp_df(year,month,day,dept,sub_dept):
#     # 基础数据1
#     # sap leave code
#     leave_code_list = api.get_sap_leave_code_df()['SapShiftCode'].unique().tolist()
#
#     # # subdept下参与排班的员工
#     # emp_df = api.get_employee_df(dept, sub_dept, ['Shopfloor leader', 'OP', 'MH', 'VI'])
#     # print(emp_df.shape)
#     # print(emp_df.head())
#
#     # 基础数据2
#     # subdept下员工的OT
#     emp_ot_df = api.get_employee_ot_df(dept, sub_dept)
#     print(emp_ot_df.shape)
#     print(emp_ot_df.head())
#
#     # 基础数据3
#     # subdept下员工的skill
#     emp_skill_df = api.get_employee_skill_df(dept, sub_dept)
#     print(emp_skill_df.shape)
#     print(emp_skill_df.head())
#
#     # 基础数据4
#     # subdept下员工的workstatus 已经包括 emp gender/line/position
#     emp_status_df = api.get_employee_working_status_df(dept, sub_dept,['Shopfloor leader', 'OP', 'MH', 'VI'])
#     emp_status_df['Date'] = pd.to_datetime(
#         emp_status_df[['Year', 'Month', 'Day']]
#     ).dt.date
#
#     # subdept下员工排班当天的workstatus
#     today_emp_status_df = emp_status_df[(emp_status_df['Year'] == year) & (emp_status_df['Month'] == month) & (emp_status_df['Day'] == day)]
#     print(today_emp_status_df.shape)
#     print(today_emp_status_df.head())
#     # 用sap_leave_code_df来匹配today_emp_status_df中 SapShiftCode，如果匹配到就都转成OFF
#     today_emp_status_df.loc[
#         today_emp_status_df['SapShiftCode'].isin(leave_code_list),
#         'SapShiftCode'
#     ] = 'OFF'
#
#     #然后再排除 today_emp_status_df中sapshiftcode 不为S121\S122\OFF这三大类的人
#     today_emp_status_df = today_emp_status_df[
#         today_emp_status_df['SapShiftCode'].isin(['S121', 'S122', 'OFF'])
#     ]
#
#     print("过滤后人数:", today_emp_status_df.shape)
#
#     # 配上OT信息
#     # 将员工信息表(emp_df) 和员工OT工时表(emp_ot_df) 合并
#     today_emp_status_df_with_ot_df = pd.merge(
#         today_emp_status_df,
#         emp_ot_df[['PersonNo', 'MonthOTNew', 'YTDOTAvgNew']],
#         on='PersonNo',
#         how='left'
#     )
#
#     # 用OT筛可用排班员工
#     today_emp_status_df_with_ot_df = today_emp_status_df_with_ot_df[
#         (today_emp_status_df_with_ot_df['YTDOTAvgNew'] <= 35) &
#         (today_emp_status_df_with_ot_df['MonthOTNew'] <= 90)
#         ]
#     print(today_emp_status_df_with_ot_df.head())
#
#     # mapping skill OJTCODE
#     today_emp_status_df_with_ot_skill_df = pd.merge(
#         today_emp_status_df_with_ot_df,
#         emp_skill_df[['PersonNo', 'OJTCode', 'SkillName']],
#         on='PersonNo',
#         how='left'
#     )
#     print(today_emp_status_df_with_ot_skill_df)
#
#     #分成S121 和 S122 和off班的人 3张df
#     s121_emp_df = today_emp_status_df_with_ot_skill_df[
#         today_emp_status_df_with_ot_skill_df['SapShiftCode'] == 'S121'
#         ].reset_index(drop=True)
#
#     s122_emp_df = today_emp_status_df_with_ot_skill_df[
#         today_emp_status_df_with_ot_skill_df['SapShiftCode'] == 'S122'
#         ].reset_index(drop=True)
#
#     off_emp_df = today_emp_status_df_with_ot_skill_df[
#         today_emp_status_df_with_ot_skill_df['SapShiftCode'] == 'OFF'
#         ].reset_index(drop=True)
#
#     print("S121人数:", s121_emp_df.shape)
#     print("S122人数:", s122_emp_df.shape)
#     print("OFF人数:", off_emp_df.shape)
#
#     #off班里的人还需要剔除连续7天上班的 用emp_status
#     person_off_dict = build_person_off_dict(emp_status_df, leave_code_list)
#     today = datetime(int(year),int(month),int(day)).date()
#
#     invalid_person_list = []
#
#     for person_id in off_emp_df['PersonNo'].unique():
#         if not can_change_off_to_work(person_id, today, person_off_dict):
#             invalid_person_list.append(person_id)
#
#     off_emp_df = off_emp_df[
#         ~off_emp_df['PersonNo'].isin(invalid_person_list)
#     ]
#
#     #再通过查看前一天晚班是否上班来 区分 白班晚班都可用的off人员 以及 只有晚班才可以用的off人员
#     # 昨天日期
#     yesterday = today - timedelta(days=1)
#
#     yesterday_shift_df = emp_status_df[
#         emp_status_df['Date'] == yesterday
#         ][['PersonNo', 'Shift']]
#     print(yesterday_shift_df)
#     yesterday_shift_df.columns = ['PersonNo', 'ShiftofY']
#
#     merged_df = off_emp_df.merge(
#         yesterday_shift_df,
#         on='PersonNo',
#         how='left'
#     )
#     print(merged_df)
#     only_night_off_emp_df = merged_df[
#         merged_df['ShiftofY'] == 'N'
#         ]
#
#     day_and_night_off_emp_df = merged_df[
#         merged_df['ShiftofY'] != 'N'
#         ]
#
#
#     return s121_emp_df, s122_emp_df, day_and_night_off_emp_df, only_night_off_emp_df


import bisect
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict


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

        # 批量获取线体信息和技能
        line_groups = task_df['LineGroup'].unique()
        all_group_line_df = pd.concat([
            self.api.get_line_by_dept_and_group_df(self.dept, self.sub_dept, lg)
            for lg in line_groups
        ], ignore_index=True)

        line_ids = task_df['LineID'].unique()
        all_line_skill_df = pd.concat([
            self.api.get_line_skill_df(lid) for lid in line_ids if lid
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
        today_mask = (self.emp_status_df['Year'] == self.year) & \
                     (self.emp_status_df['Month'] == self.month) & \
                     (self.emp_status_df['Day'] == self.day)

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
    scheduler = ShopfloorScheduler(api, "ME/MOE6-CN", "ME/MFO6.5-CN","2026","03","13")
    s121, s122, day_night_off, only_night_off = scheduler.prepare_emp_resources()
    s121.to_excel("./data_preprocessing/s121_emp_df_class.xlsx", index=False)
    s122.to_excel("./data_preprocessing/s122_emp_df_class.xlsx", index=False)
    day_night_off.to_excel("./data_preprocessing/day_night_off_emp_df_class.xlsx", index=False)
    only_night_off.to_excel(
        "./data_preprocessing/only_night_off_emp_df_class.xlsx",index=False)
    inline_data = [
        {"Year": "2026", "Month": "3", "Date": "13", "DorN": "D", "Dept": "ME/MOE6-CN", "SubDept": "ME/MFO6.5-CN",
         "LineGroup": "FA", "LineID": "31", "LineName": "BSFA12", "ShiftCode": "S121"},
        {"Year": "2026", "Month": "3", "Date": "13", "DorN": "D", "Dept": "ME/MOE6-CN", "SubDept": "ME/MFO6.5-CN",
         "LineGroup": "FA", "LineID": "32", "LineName": "BSFA13", "ShiftCode": "S121"},
        {"Year": "2026", "Month": "3", "Date": "13", "DorN": "N", "Dept": "ME/MOE6-CN", "SubDept": "ME/MFO6.5-CN",
         "LineGroup": "FA", "LineID": "32", "LineName": "BSFA13", "ShiftCode": "S122"}
                       ]
    all_group_line_df, day_inline_task_with_skill_df, night_inline_task_with_skill_df = scheduler.get_line_requirements(inline_data)
    all_group_line_df.to_excel(
        "./data_preprocessing/all_group_line_df_class.xlsx", index=False)
    day_inline_task_with_skill_df.to_excel(
        "./data_preprocessing/day_inline_task_with_skill_df_class.xlsx", index=False)
    night_inline_task_with_skill_df.to_excel(
        "./data_preprocessing/night_inline_task_with_skill_df_class.xlsx", index=False)

    # all_group_line_df, day_inline_task_with_skill_df, night_inline_task_with_skill_df = inline_task_df(inline_data)
    # print(all_group_line_df)
    # all_group_line_df.to_excel(
    #     "./data_preprocessing/all_group_line_df.xlsx", index=False)
    # print(day_inline_task_with_skill_df)
    # day_inline_task_with_skill_df.to_excel(
    #     "./data_preprocessing/day_inline_task_with_skill_df.xlsx", index=False)
    # print(night_inline_task_with_skill_df)
    # night_inline_task_with_skill_df.to_excel(
    #     "./data_preprocessing/night_inline_task_with_skill_df.xlsx", index=False)
    #
    # s121_emp_df, s122_emp_df, day_and_night_off_emp_df, only_night_off_emp_df = inline_task_emp_df("2026","03","13","ME/MOE6-CN", "ME/MFO6.5-CN")
    # print(s121_emp_df)
    # s121_emp_df.to_excel("./data_preprocessing/s121_emp_df.xlsx",index=False)
    # print(s122_emp_df)
    # s122_emp_df.to_excel("./data_preprocessing/s122_emp_df.xlsx", index=False)
    # print(day_and_night_off_emp_df)
    # day_and_night_off_emp_df.to_excel("./data_preprocessing/day_and_night_off_emp_df.xlsx", index=False)
    # print(only_night_off_emp_df)
    # only_night_off_emp_df.to_excel("./data_preprocessing/only_night_off_emp_df.xlsx", index=False)
    #
    #
    #
    #
