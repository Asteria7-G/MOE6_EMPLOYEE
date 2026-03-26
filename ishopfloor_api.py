import requests
import urllib3
import pandas as pd
from typing import Dict, Any, Optional

urllib3.disable_warnings()


class ShopfloorAPI:
    """
    Shopfloor API 客户端封装
    """

    def __init__(self,
                 base_url: str = "https://szh-ishopfloor.apac.bosch.com:9005/api/DataQuery",
                 timeout: int = 15):

        self.base_url = base_url
        self.timeout = timeout
        self.session = requests.Session()

        self.headers = {
            "Content-Type": "application/json"
        }

    # =========================
    # 通用POST方法
    # =========================
    def _post(self,
              endpoint: str,
              json_data: Optional[Dict[str, Any]] = None,
              params: Optional[Dict[str, Any]] = None):

        url = f"{self.base_url}/{endpoint}"

        response = self.session.post(
            url,
            json=json_data,
            params=params,
            headers=self.headers,
            verify=False,
            timeout=self.timeout
        )

        if response.status_code != 200:
            raise Exception(f"[{endpoint}] HTTP请求失败: {response.status_code}")

        result = response.json()

        return result.get("value", [])

    # =========================
    # 员工列表
    # =========================
    # ['Shopfloor leader', 'OP', 'MH', 'VI']
    # def get_employee_df(self,
    #                     dept_name: str,
    #                     sub_dept_name: str = "",
    #                     position_list: list = None) -> pd.DataFrame:
    #
    #     data = self._post(
    #         "getemplist",
    #         json_data={
    #             "deptName": dept_name,
    #             "subDeptName": sub_dept_name
    #         }
    #     )
    #
    #     if not data:
    #         return pd.DataFrame()
    #
    #     df = pd.DataFrame(data)
    #
    #     # 清洗
    #     df = df[df["Line"].notna()]
    #     df = df[df["Line"] != ""]
    #
    #     if position_list:
    #         df = df[df["POSITION"].isin(position_list)]
    #     df.reset_index(drop=True)
    #     df.columns = ['Dept','SubDept','PersonNo','Gender','Line', 'Poistion']
    #
    #     return df

    # =========================
    # 员工执勤记录
    # =========================
    def get_employee_working_status_df(self,
                                       dept_name: str,
                                       sub_dept_name: str = "",
                                       position_list: list = None) -> pd.DataFrame:

        data = self._post(
            "getempshiftlist",
            json_data={
                "deptName": dept_name,
                "subDeptName": sub_dept_name
            }
        )
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        if position_list:
            df = df[df["POSITION"].isin(position_list)]
            df.reset_index(drop=True)
        df.columns = ['PersonNo', 'Year', 'Month', 'Day', 'Shift', 'SapShiftCode',
       'Dept', 'SubDept', 'Gender', 'Line', 'LineID', 'Position']


        return df


    # =========================
    # OT信息
    # =========================

    def get_employee_ot_df(self,
                           dept_name: str,
                           sub_dept_name: str = "") -> pd.DataFrame:

        data = self._post(
            "getotlist",
            json_data={
                "deptName": dept_name,
                "subDeptName": sub_dept_name
            }
        )
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df.columns = ['Dept', 'SubDept', 'PersonNo', 'Year', 'Month', 'Day', 'Date',
       'MonthOTNew', 'YTDOTAvgNew']


        return df

    # =========================
    # 员工Skill
    # =========================
    def get_employee_skill_df(self,
                              dept_name: str,
                              sub_dept_name: str = "") -> pd.DataFrame:

        data = self._post(
            "getempskill",
            json_data={
                "deptName": dept_name,
                "subDeptName": sub_dept_name
            }
        )
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df.columns = ['Dept','SubDept','PersonNo','OJTCode','SkillName']

        return df

    # =========================
    # SAP Leave Code
    # =========================
    def get_sap_leave_code_df(self) -> pd.DataFrame:

        data = self._post("getsapleavecode")

        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df.columns = ['SapShiftCode']

        return df

    # =========================
    # Shift Group Mapping
    # =========================
    def get_shift_group_mapping_df(self) -> dict:

        data = self._post("getshiftgroup")

        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        return df

    # =========================
    # Line Skill
    # =========================
    def get_line_skill_df(self, line_id: str) -> pd.DataFrame:

        data = self._post(
            "getlineskill",
            params={"lineId": line_id}
        )

        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df.columns = ['LineName','OJTCode','SkillName','EmpType','PreferGender','RequiredCount']
        df.insert(0, "LineID", line_id)


        return df

    # =========================
    # 部门线体group
    # =========================
    def get_line_by_dept_and_subdept_df(self,
                                      dept_name: str,
                                      sub_dept_name: str = "") -> pd.DataFrame:

        data = self._post(
            "getdeptlinegroup",
            json_data={
                "deptName": dept_name,
                "subDept": sub_dept_name
            }
        )
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df.columns = ["LineGroup","LineName","LineId"]
        df.insert(0, "Dept", dept_name)
        df.insert(1, "SubDept", sub_dept_name)

        return df

if __name__ == "__main__":

    api = ShopfloorAPI()
    #subdept 下有所属线体且postion在这个list里面的员工
    # emp_df = api.get_employee_df("ME/MOE6-CN","ME/MFO6.5-CN",['Shopfloor leader', 'OP', 'MH', 'VI'])
    # print(emp_df.shape)
    # print(emp_df.head())
    # print(list(emp_df['Line']))
    # print(emp_df[emp_df['PersonNo'] == '88840982'])
    # # # subdept下的员工工作状态记录
    # # # 当日是regular还是OT
    # # # 加班是否导致持续7天工作(劳动法）
    # # # 白天加班是否和前一天晚上上班冲突
    emp_working_status_df = api.get_employee_working_status_df("ME/MOE6-CN","ME/MFO6.5-CN",['Shopfloor leader', 'OP', 'MH', 'VI'])
    today_emp_status_df = emp_working_status_df[
        (emp_working_status_df['Year'] == "2026") & (emp_working_status_df['Month'] == "03") & (emp_working_status_df['Day'] == "12")]
    print(emp_working_status_df.shape)
    print(emp_working_status_df.head())
    print(emp_working_status_df.columns)
    print(emp_working_status_df['Shift'])
    print(today_emp_status_df["SapShiftCode"])
    print(emp_working_status_df[emp_working_status_df['PersonNo'] == '88840982'])
    # #subdept下员工目前的加班时长
    # emp_ot_df = api.get_employee_ot_df(
    #     "ME/MOE6-CN","ME/MFO6.5-CN")
    # print(emp_ot_df.shape)
    # print(emp_ot_df.head())
    # print(emp_ot_df.columns)
    #subdept下员工的技能
    # emp_skill_df = api.get_employee_skill_df("ME/MOE6-CN","ME/MFO6.5-CN")
    # print(emp_skill_df.shape)
    # print(emp_skill_df.head())
    # print(emp_skill_df.columns)
    # # # sap的休班休假code
    # sap_leave_code_df = api.get_sap_leave_code_df()
    # print(sap_leave_code_df.shape)
    # print(sap_leave_code_df.head())
    # print(sap_leave_code_df['SapShiftCode'])
    # # 大班和小班的包含关系
    # shift_group_mapping_df = api.get_shift_group_mapping_df()
    # print(shift_group_mapping_df.shape)
    # print(shift_group_mapping_df.head())
    # print(shift_group_mapping_df.columns)
    # print(shift_group_mapping_df['smallshiftcodelist'][0][0])
    #线体所需技能人数 人员分2种 regular和outsourcing
    line_skill_df = api.get_line_skill_df("33")
    print(line_skill_df.shape)
    print(line_skill_df.head())
    print(line_skill_df.columns)
    # subdept下线体是否多个group分组 导致人员互换有限制  本线体员工最优 其次优先级一样
    line_by_dept_and_subdept_df = api.get_line_by_dept_and_subdept_df("ME/MOE6-CN","ME/MFO6.5-CN")
    print(line_by_dept_and_subdept_df.shape)
    print(line_by_dept_and_subdept_df.head())



