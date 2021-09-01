import calendar
import datetime
import os

import pandas as pd
import yaml


def strip_in_data(df):
    """
    去掉不可见字符（¥）等信息

    :param df:
    :return:
    """
    df = df.rename(columns={column_name: column_name.strip() for column_name in df.columns})
    df = df.applymap(lambda x: x.strip().strip("¥") if isinstance(x, str) else x)
    return df


def read_wechat_data(path):
    """
    读取 WeChat 账单转换为 DataFrame

    :param path:
    :return:
    """
    wechat_df = pd.read_csv(path, header=16, skipfooter=0, encoding="utf-8", engine="python")
    wechat_df = wechat_df.iloc[:, [0, 4, 7, 1, 2, 3, 5]]
    wechat_df = strip_in_data(wechat_df)
    wechat_df.iloc[:, 0] = wechat_df.iloc[:, 0].astype("datetime64")
    wechat_df.iloc[:, 6] = wechat_df.iloc[:, 6].astype("float64")
    wechat_df = wechat_df.drop(wechat_df[wechat_df["收/支"] == "/"].index)
    wechat_df.rename(columns={"交易时间": "时间", "当前状态": "支付状态", "金额(元)": "金额", "商品": "备注", "收/支": "类型"}, inplace=True)
    wechat_df = wechat_df.drop(wechat_df[wechat_df["金额"] == 0].index)
    wechat_df.insert(1, "账户1", "微信", allow_duplicates=True)
    print(f"成功读取「微信」账单数据 {len(wechat_df)} 条")
    return wechat_df


def read_alipay_data(path):
    """
    读取 AliPay 账单转换为 DataFrame

    :param path:
    :return:
    """
    alipay_df = pd.read_csv(path, header=4, skipfooter=7, encoding="gbk", engine="python")
    alipay_df = alipay_df.iloc[:, [2, 10, 11, 6, 7, 8, 9]]
    alipay_df = strip_in_data(alipay_df)
    alipay_df.iloc[:, 0] = alipay_df.iloc[:, 0].astype("datetime64")
    alipay_df.iloc[:, 6] = alipay_df.iloc[:, 6].astype("float64")
    alipay_df = alipay_df.drop(alipay_df[alipay_df["收/支"] == ""].index)
    alipay_df.rename(
        columns={"交易创建时间": "时间", "交易状态": "支付状态", "商品名称": "备注", "金额（元）": "金额", "类型": "交易类型", "收/支": "类型"}, inplace=True
    )
    alipay_df = alipay_df.drop(alipay_df[alipay_df["金额"] == 0].index)
    alipay_df.insert(1, "账户1", "支付宝", allow_duplicates=True)
    print(f"成功读取「支付宝」账单数据 {len(alipay_df)} 条")
    return alipay_df


def add_category(data):
    """
    根据分类配置，修改默认分类

    :param data:
    :return:
    """
    with open("category.yaml", 'r', encoding="utf-8") as f:
        category_map = yaml.load(f, Loader=yaml.FullLoader)
    data.insert(8, "分类", "其他", allow_duplicates=True)
    for index in range(len(data.iloc[:, 8])):
        pay_type2 = data.iloc[index, 4]
        pay_user = data.iloc[index, 5]
        shop = data.iloc[index, 6]
        pay_type = data.iloc[index, 2]
        pay_map = category_map.get(pay_type)
        for key, value in pay_map.items():
            if (
                any(one in shop for one in value.get("备注"))
                or any(two in pay_user for two in value.get("交易对方"))
                or any(three in pay_type2 for three in value.get("交易类型", []))
            ):
                data.iloc[index, 8] = key
    return data


def main():
    """
    根据文件目录获取账单 csv 文件，合并账单生成钱迹模版

    :return:
    """

    path_wx = [os.path.join("data/wechat", one) for one in os.listdir("data/wechat") if one.endswith(".csv")]
    path_zfb = [os.path.join("data/alipay", one) for one in os.listdir("data/alipay") if one.endswith(".csv")]
    df_list = [pd.DataFrame(), pd.DataFrame()]
    print("------------ 开始读取账单 ------------\n")
    for wx_path in path_wx:
        df_list.append(read_wechat_data(wx_path))
    for alipay in path_zfb:
        df_list.append(read_alipay_data(alipay))

    data_merge = pd.concat(df_list, axis=0)
    print("\n------------ 开始账单去重 ------------\n")
    old_count = len(data_merge)
    data_merge = data_merge.drop_duplicates()
    print(f"去重前条数为: {old_count}\n去重后条数为: {len(data_merge)}\n去重掉条数为: {old_count - len(data_merge)}")
    data_merge = add_category(data_merge)
    data_merge["时间"] = pd.to_datetime(data_merge["时间"]).dt.date
    end_day = data_merge["时间"].max()
    start_day = data_merge["时间"].min()
    months = (end_day.year - start_day.year) * 12 + end_day.month - start_day.month
    month_range = [
        (
            datetime.date(year=start_day.year + month // 12, month=month % 12 + 1, day=1),
            datetime.date(
                year=start_day.year + month // 12,
                month=month % 12 + 1,
                day=calendar.monthrange(start_day.year + month // 12, month % 12 + 1)[-1],
            ),
        )
        for month in range(start_day.month - 1, start_day.month + months)
    ]
    print("\n------------ 开始拆分账单 ------------\n")
    for month_info in month_range:
        start_time, end_time = month_info
        month_data = data_merge[(data_merge["时间"] >= start_time) & (data_merge["时间"] <= end_time)]
        save_path = f"data/result/{start_time}~{end_time}.xlsx"
        print(f"已合并 {start_time}~{end_time} 数据 {len(month_data)} 条, 并存储到: {save_path}")
        month_data.to_excel(save_path, index=False)


if __name__ == "__main__":
    main()
