import pandas as pd
import requests
import json
import os
import time
import random
from pathlib import Path
from collections import defaultdict

# -------------------------- 从环境变量读取配置（适配GitHub Actions） --------------------------
WECHAT_WEBHOOK = os.getenv("WECHAT_WEBHOOK")  # 从GitHub Secrets读取
EXCEL_FILE_PATH = os.getenv("EXCEL_FILE_PATH", "共享无偿资料库.xlsx")  # 默认值
TARGET_COL_TYPE = os.getenv("TARGET_COL_TYPE", "资源类型")
TARGET_COL_NAME = os.getenv("TARGET_COL_NAME", "资源名称")
TARGET_COL_LINK = os.getenv("TARGET_COL_LINK", "资源链接")
SEND_LINKS_PER_TYPE = int(os.getenv("SEND_LINKS_PER_TYPE", 5))
SEND_INTERVAL = int(os.getenv("SEND_INTERVAL", 2))
NO_fenlei_MODE = os.getenv("NO_fenlei_MODE", "true").lower() == "true"  # 启用随机模式
RANDOM_COUNT = int(os.getenv("RANDOM_COUNT", 10))  # 随机选取的资源数量

# 指定要优先推送的标签
SPECIFIED_TAGS = ["学科资料", "年级学段", "教材版本", "考试试卷", "学习教辅"]



def read_excel_and_classify(file_path, col_type, col_name, col_link):
    """改用pandas读取Excel按资源类型分类"""
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Excel文件不存在：{str(file_path.absolute())}")

    try:
        df = pd.read_excel(
            file_path,
            dtype=str,
            keep_default_na=False,
            na_filter=False
        )
    except Exception as e:
        raise Exception(f"读取Excel失败：{str(e)}")

    # 清洗列名 + 校验必填列
    df.columns = [str(col).strip() for col in df.columns]
    required_cols = [col_type.strip(), col_name.strip(), col_link.strip()]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise Exception(f"Excel缺少指定列：{missing_cols}，当前表头：{list(df.columns)}")

    # 重命名列 + 数据清洗
    df = df.rename(columns={
        col_type.strip(): "type",
        col_name.strip(): "name",
        col_link.strip(): "link"
    })
    df["type"] = df["type"].str.strip()
    df["name"] = df["name"].str.strip().replace("", "无名称")
    df["link"] = df["link"].str.strip()

    # 过滤有效数据
    filter_condition = (
            df["type"].notna() &
            df["link"].notna() &
            df["link"].str.startswith(("http://", "https://"), na=False)
    )
    df_clean = df[filter_condition].copy()

    # 按类型分组
    type_res_dict = defaultdict(list)
    for _, row in df_clean.iterrows():
        type_res_dict[row["type"]].append((row["name"], row["link"]))

    return type_res_dict


def get_random_resources_by_tags(file_path, col_type, col_name, col_link, count, specified_tags=None, exclude_tags=False):
    """根据标签筛选后随机选取指定数量的资源"""
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Excel文件不存在：{str(file_path.absolute())}")

    try:
        df = pd.read_excel(
            file_path,
            dtype=str,
            keep_default_na=False,
            na_filter=False
        )
    except Exception as e:
        raise Exception(f"读取Excel失败：{str(e)}")

    # 清洗列名 + 校验必填列
    df.columns = [str(col).strip() for col in df.columns]
    required_cols = [col_type.strip(), col_name.strip(), col_link.strip()]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise Exception(f"Excel缺少指定列：{missing_cols}，当前表头：{list(df.columns)}")

    # 重命名列 + 数据清洗
    df = df.rename(columns={
        col_type.strip(): "type",
        col_name.strip(): "name",
        col_link.strip(): "link"
    })
    df["type"] = df["type"].str.strip()
    df["name"] = df["name"].str.strip().replace("", "无名称")
    df["link"] = df["link"].str.strip()

    # 过滤有效数据
    filter_condition = (
            df["type"].notna() &
            df["link"].notna() &
            df["link"].str.startswith(("http://", "https://"), na=False)
    )
    df_clean = df[filter_condition].copy()

    if len(df_clean) == 0:
        raise Exception("Excel中没有有效的资源数据")

    # 根据标签筛选
    if specified_tags:
        if exclude_tags:
            # 排除指定标签
            df_filtered = df_clean[~df_clean["type"].isin(specified_tags)]
        else:
            # 只包含指定标签
            df_filtered = df_clean[df_clean["type"].isin(specified_tags)]

        if len(df_filtered) == 0:
            raise Exception(f"没有找到{'非' if exclude_tags else ''}指定标签的资源数据")
    else:
        df_filtered = df_clean

    # 随机选取指定数量的资源
    sample_count = min(count, len(df_filtered))
    df_sample = df_filtered.sample(n=sample_count, random_state=random.randint(0, 10000))

    return [(row["name"], row["link"], row["type"]) for _, row in df_sample.iterrows()]


def format_single_type_message(res_type, res_list, max_num=5):
    """格式化单个类别的消息，改为随机抽取max_num条"""
    if not res_list:
        return f"⚠️ 【{res_type}】无有效资源数据"

    # 核心修改：随机抽取max_num条（不足则取全部）
    sample_count = min(max_num, len(res_list))  # 避免资源数不足时报错
    random_res = random.sample(res_list, sample_count)  # 随机抽样，不重复

    # 格式化随机抽取的内容
    res_str = "\n".join([f"📚{i + 1}. {name}：\n{link}" for i, (name, link) in enumerate(random_res)])

    # 构造单类别消息（删除了原第一行"📚 共享资源推送"）
    msg_parts = [
        f"{res_type}共{len(res_list)}条，随机抽取{sample_count}条）：\n{res_str}\n",
        "💡 需要其他资源可联系我，更多资料可在该网站搜索：https://dcn8qexvg13r.feishu.cn/wiki/OAS1wpySSiedCDkgnjycCza8nFf?table=tblgsMxc3clOlIc5&view=vewQ1AKJ0D"
    ]
    final_msg = "\n".join(msg_parts)
    return final_msg[:4000]  # 预留空间，避免超企业微信字符限制


def format_random_resources_message(resources):
    """格式化随机选取的资源消息"""
    if not resources:
        return "⚠️ 没有有效的资源数据"

    # 格式化随机选取的内容
    res_str = "\n".join([f"📚{i + 1}. {name}（{res_type}）：\n{link}" for i, (name, link, res_type) in enumerate(resources)])

    # 构造消息
    msg_parts = [
        f"🎲 随机推送 {len(resources)} 个资源：\n{res_str}\n",
        "💡 需要其他资源可联系我，更多资料可在该网站搜索：https://dcn8qexvg13r.feishu.cn/wiki/OAS1wpySSiedCDkgnjycCza8nFf?table=tblgsMxc3clOlIc5&view=vewQ1AKJ0D"
    ]
    final_msg = "\n".join(msg_parts)
    return final_msg[:4000]  # 预留空间，避免超企业微信字符限制


def send_to_wechat_bot(webhook, content, res_type):
    """推送企业微信"""
    if not webhook or not content:
        raise ValueError("Webhook地址或推送内容不能为空")

    payload = {
        "msgtype": "text",
        "text": {
            "content": content,
            "mentioned_list": [],
            "mentioned_mobile_list": []
        }
    }

    try:
        headers = {"Content-Type": "application/json; charset=utf-8"}
        response = requests.post(
            url=webhook,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers=headers,
            timeout=20,
            verify=False  # GitHub ActionsUbuntu环境SSL兼容
        )
        response.encoding = "utf-8"
        result = response.json()
        if result.get("errcode") != 0:
            raise Exception(f"推送失败：{result.get('errmsg')}（错误码：{result.get('errcode')}）")
        print(f"✅ 【{res_type}】推送成功！")
        return True
    except Exception as e:
        raise Exception(f"推送失败：{str(e)}")


if __name__ == "__main__":
    """主入口"""
    try:
        # 校验Webhook是否配置
        if not WECHAT_WEBHOOK:
            raise Exception("❌ 未配置企业微信Webhook（请检查GitHub Secrets）")

        if NO_fenlei_MODE:
            # 第一次推送：从指定标签中选取资源
            print(f"🎲 第一次推送：从指定标签{SPECIFIED_TAGS}中随机选取 {RANDOM_COUNT} 个资源...")
            try:
                first_batch_resources = get_random_resources_by_tags(
                    EXCEL_FILE_PATH,
                    TARGET_COL_TYPE,
                    TARGET_COL_NAME,
                    TARGET_COL_LINK,
                    RANDOM_COUNT,
                    specified_tags=SPECIFIED_TAGS,
                    exclude_tags=False
                )

                if not first_batch_resources:
                    print("❌ 指定标签中没有有效的资源数据")
                else:
                    print(f"✅ 成功从指定标签中选取 {len(first_batch_resources)} 个资源")
                    msg_content = format_random_resources_message(first_batch_resources)
                    print(f"📝 待推送内容：\n{msg_content}")

                    try:
                        send_to_wechat_bot(WECHAT_WEBHOOK, msg_content, "指定标签资源")
                        print("✅ 第一次推送完成")
                    except Exception as e:
                        print(f"❌ 第一次推送失败：{str(e)}")
                        exit(1)

                    # 等待2秒间隔
                    print("⏳ 等待2秒后进行第二次推送...")
                    time.sleep(2)

            except Exception as e:
                print(f"❌ 第一次推送失败：{str(e)}")
                exit(1)

            # 第二次推送：从非指定标签中选取资源
            print(f"🎲 第二次推送：从非指定标签中随机选取 {RANDOM_COUNT} 个资源...")
            try:
                second_batch_resources = get_random_resources_by_tags(
                    EXCEL_FILE_PATH,
                    TARGET_COL_TYPE,
                    TARGET_COL_NAME,
                    TARGET_COL_LINK,
                    RANDOM_COUNT,
                    specified_tags=SPECIFIED_TAGS,
                    exclude_tags=True
                )

                if not second_batch_resources:
                    print("❌ 非指定标签中没有有效的资源数据")
                    exit(0)

                print(f"✅ 成功从非指定标签中选取 {len(second_batch_resources)} 个资源")
                msg_content = format_random_resources_message(second_batch_resources)
                print(f"📝 待推送内容：\n{msg_content}")

                try:
                    send_to_wechat_bot(WECHAT_WEBHOOK, msg_content, "非指定标签资源")
                except Exception as e:
                    print(f"❌ 第二次推送失败：{str(e)}")
                    exit(1)

            except Exception as e:
                print(f"❌ 第二次推送失败：{str(e)}")
                exit(1)

            print("\n🎉 两次资源推送完成！")
        else:
            # 分类模式：按资源类型分类推送
            print("📌 分类模式：开始读取Excel文件...")
            type_res = read_excel_and_classify(
                EXCEL_FILE_PATH,
                TARGET_COL_TYPE,
                TARGET_COL_NAME,
                TARGET_COL_LINK
            )
            total_types = len(type_res)
            print(f"✅ 读取完成，共识别到 {total_types} 种资源类型")

            if total_types == 0:
                print("❌ 未识别到任何有效资源类型，终止推送")
                exit(0)

            print(f"📌 开始分{total_types}次推送（间隔{SEND_INTERVAL}秒/次）...")
            for idx, (res_type, res_list) in enumerate(type_res.items(), start=1):
                # 核心修改：资源类型下的资源数量少于5个则跳过推送
                if len(res_list) < 5:
                    print(f"⏭️ 【{res_type}】资源数量不足5个（当前{len(res_list)}个），跳过推送")
                    continue

                print(f"\n🔹 推送第{idx}/{total_types}类：{res_type}")
                msg_content = format_single_type_message(res_type, res_list, SEND_LINKS_PER_TYPE)
                print(f"📝 待推送内容：\n{msg_content}")

                try:
                    send_to_wechat_bot(WECHAT_WEBHOOK, msg_content, res_type)
                except Exception as e:
                    print(f"❌ 【{res_type}】推送失败：{str(e)}")

                if idx < total_types:
                    print(f"⏳ 等待{SEND_INTERVAL}秒...")
                    time.sleep(SEND_INTERVAL)

            print("\n🎉 所有符合条件的类别推送完成！")

    except Exception as e:
        print(f"❌ 执行失败：{str(e)}")
        exit(1)  # 非0退出码，GitHub Actions会标记为失败
