import json
import os
import pandas as pd
import re
import sys
import logging
from datetime import datetime

# 模拟器VP前缀字典
simulator_vp_prefix_dict = {
    'RS': 'rsspm_erpt.RSSPM_ERPT_Varpool.',
    'RH': 'rhrede.RhredeVarpool.',
    'WH': 'whvpa.WhvpaVarpool.',
    'FLS': 'fls.FlsVarPool.',
    'WA': 'wa.WaVarPool.',
    'IS': 'is.IS_Varpool.',
    'RA': 'ra.RA_Varpool.',
    'WS': 'wsetc.WsetcVarpool.',
    'DC': 'dc.DcVarpool.',
    'SD': 'avis.AVISVarpool.',
    'WSPM': 'wsetc.WsetcVarpool.',
    'RSPM': 'rsspm_erpt.RSSPM_ERPT_Varpool.',
    'RM': 'rm_erpt.RM_ERPT_Varpool.'
}

def setup_logging(config):
    """设置日志记录 - 只保存到文件"""
    log_dir = config.get('output_dir', '.')
    log_filename = f"fault_matching_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    log_path = os.path.join(log_dir, log_filename)

    # 创建logger
    logger = logging.getLogger('FaultMatching')
    logger.setLevel(logging.INFO)

    # 只使用文件handler，移除控制台handler
    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    file_handler.setLevel(logging.INFO)

    # 格式化
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    return logger, log_path

def load_config(config_path):
    """加载配置文件"""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"错误: 读取配置文件失败: {e}")
        sys.exit(1)

def get_board_config(board_cfg, pg_name):
    """获取单个板卡配置"""
    parts = board_cfg['board_fold_name'].split('_')
    board_id = board_cfg['boardId']
    module_code = parts[1].upper()
    return board_id, module_code

def get_vp_prefix(pg_name):
    """根据PG名称获取VP前缀"""
    vp_prefix = simulator_vp_prefix_dict.get(pg_name)
    if not vp_prefix:
        print(f"警告: 未找到PG '{pg_name}' 对应的VP前缀，使用默认值")
        vp_prefix = 'wsetc.WsetcVarpool.'  # 默认值
    return vp_prefix

def read_input_files_for_board(config, board_cfg, logger):
    """读取单个板卡的输入文件"""
    # 读取故障处理表单
    erpt_file = os.path.join(board_cfg['fold_path'], board_cfg['errEntries_file_name'])
    print(f"读取故障处理表单: {erpt_file}")
    print(f"Sheet页: {board_cfg['errEntries_file_sheet']}")
    logger.info(f"读取故障处理表单: {erpt_file}")
    logger.info(f"Sheet页: {board_cfg['errEntries_file_sheet']}")

    df_err_rpt = pd.read_excel(erpt_file, sheet_name=board_cfg['errEntries_file_sheet'],
                               header=2, skiprows=[3])

    # 读取故障码对照表（所有板卡共用同一个）
    # 使用AG列（故障码）和L列（描述（英文））
    df_helf_input = pd.read_excel(config['helf_input_file'], sheet_name=config['subsys_sheet'],
                                  header=2, usecols='L, AG')
    df_helf_input.columns = ['故障描述', '故障码']
    print(f"读取完成: 故障表{len(df_err_rpt)}行")
    logger.info(f"读取完成: 故障表{len(df_err_rpt)}行, 对照表{len(df_helf_input)}行")
    return df_err_rpt, df_helf_input

def prepare_output_for_board(df_err_rpt, config, board_cfg, logger):
    """为单个板卡准备输出结构"""
    pg_name = config['pgName']
    board_id, module_code = get_board_config(board_cfg, pg_name)
    vp_prefix = get_vp_prefix(pg_name)

    print(f"板卡配置: {board_cfg['board_fold_name']}, boardId={board_id}, 模块代码={module_code}")
    print(f"VP前缀: {vp_prefix}")
    logger.info(f"板卡配置: board_fold_name='{board_cfg['board_fold_name']}', boardId={board_id}, 模块代码='{module_code}'")
    logger.info(f"VP前缀: {vp_prefix}")

    df_selected = df_err_rpt[['成员名称', '器件', '器件编号']].copy()
    df_selected['成员名称'] = df_selected['成员名称'].astype(str).str.upper()

    # 生成TDK固化名
    df_selected[f'{pg_name}_ERPT_ACTIVE.{module_code}_成员名称_器件_器件编号'] = (
            f'{pg_name.upper()}_ERPT_ACTIVE.{module_code}_' +
            df_selected['成员名称'] + '_' +
            df_selected['器件'].astype(str) + '_' +
            df_selected['器件编号'].astype(str)
    )

    # 生成VP变量
    byte_addr = df_err_rpt['Byte地址'].fillna(0).astype(int).astype(str)
    bit_addr = df_err_rpt['bit位'].fillna(0).astype(int).astype(str)
    df_selected[f'{vp_prefix}errRptInject[x][x][x]'] = (
            f'{vp_prefix}errRptInject[{board_id}][' + byte_addr + '][' + bit_addr + ']')
    df_selected['故障说明'] = df_err_rpt['故障说明'].astype(str).str.replace(r'[\r\n]+', ' ', regex=True)
    df_selected['故障说明（英文）'] = df_err_rpt['故障说明（英文）'].astype(str)
    df_selected['故障码'] = ''  # 初始化为空字符串

    # 添加板卡标识列，便于区分数据来源
    df_selected['板卡标识'] = board_cfg['board_fold_name']

    print(f"板卡结构准备完成: {len(df_selected)}条记录")
    logger.info(f"板卡结构准备完成: {len(df_selected)}条记录")

    return df_selected

def add_fault_codes_for_board(df_output, df_err_rpt, df_helf_input, board_cfg, logger):
    """
    在基础输出结构上添加故障码
    """
    print("开始故障码匹配...")
    logger.info("开始故障码匹配...")

    # 准备故障码对照表：L列"描述（英文）"和AG列"事件码"
    fault_mappings = []
    for eng_desc, event_code in zip(df_helf_input['故障描述'].astype(str), df_helf_input['故障码'].astype(str)):
        # 清洗英文描述：去除标点符号并转换为小写
        clean_helf_desc = re.sub(r'[^\w\s]', '', str(eng_desc).strip()).lower()
        fault_mappings.append((clean_helf_desc, event_code))

    print(f"故障码对照表条目数: {len(fault_mappings)}")
    logger.info(f"故障码对照表条目数: {len(fault_mappings)}")

    matched_count = 0
    not_matched_count = 0

    # 遍历故障处理表单的"故障说明（英文）"列
    for idx, fault_desc in df_err_rpt['故障说明（英文）'].items():
        if pd.isna(fault_desc):
            continue

        # 清洗故障说明（英文）：去除标点符号并转换为小写
        clean_fault_desc = re.sub(r'[^\w\s]', '', str(fault_desc).strip()).lower()

        found = False
        matched_event_code = ""

        # 在故障码对照表中查找匹配
        for clean_helf_desc, event_code in fault_mappings:  # 修正变量名
            # 检查是否相同或包含关系（不区分大小写，不考虑标点符号）
            if (clean_fault_desc == clean_helf_desc) or (clean_fault_desc in clean_helf_desc):
                df_output.at[idx, '故障码'] = event_code
                matched_count += 1
                found = True
                matched_event_code = event_code
                break

        if found:
            logger.info(f"匹配成功 - 板卡:{board_cfg['board_fold_name']} - 故障描述:'{fault_desc}' -> 故障码:'{matched_event_code}'")
        else:
            not_matched_count += 1
            logger.warning(f"匹配失败 - 板卡:{board_cfg['board_fold_name']} - 故障描述:'{fault_desc}'")

    print(f"故障码匹配完成: 成功 {matched_count}, 失败 {not_matched_count}")
    logger.info(f"故障码匹配完成: 成功 {matched_count}, 失败 {not_matched_count}")

    return df_output

def process_all_boards(config, logger):
    """处理所有板卡配置"""
    pg_name = config['pgName']
    print("=" * 60)
    print(f"处理PG: {pg_name}")
    print(f"共{len(config['paraInfoList'])}个板卡配置")
    print("=" * 60)
    logger.info(f"开始处理PG: {pg_name}")
    logger.info(f"共{len(config['paraInfoList'])}个板卡配置")

    all_results = []
    total_processed = 0

    for i, board_cfg in enumerate(config['paraInfoList']):
        print("-" * 50)
        print(f"处理第{i+1}个板卡: {board_cfg['board_fold_name']}")
        print("-" * 50)
        logger.info(f"处理第{i+1}个板卡: {board_cfg['board_fold_name']}")

        try:
            # 读取当前板卡的文件
            df_err_rpt, df_helf_input = read_input_files_for_board(config, board_cfg, logger)

            # 准备输出结构
            df_output = prepare_output_for_board(df_err_rpt, config, board_cfg, logger)

            # 添加故障码
            df_final = add_fault_codes_for_board(df_output, df_err_rpt, df_helf_input, board_cfg, logger)

            # 添加到总结果
            all_results.append(df_final)
            total_processed += len(df_final)

            print(f"板卡处理完成: {len(df_final)}条记录")
            logger.info(f"板卡处理完成: {len(df_final)}条记录")

        except Exception as e:
            error_msg = f"✗ 处理板卡 {board_cfg['board_fold_name']} 时出错: {e}"
            print(error_msg)
            logger.error(error_msg)
            continue

    # 合并所有板卡的结果
    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)
        print("=" * 60)
        print(f"所有板卡处理完成，共合并{len(final_df)}条记录")
        print("=" * 60)
        logger.info(f"所有板卡处理完成，共合并{len(final_df)}条记录")
        return final_df
    else:
        error_msg = "没有成功处理任何板卡"
        print("=" * 60)
        print(error_msg)
        print("=" * 60)
        logger.error(error_msg)
        sys.exit(1)

def save_results(df, config, logger):
    """保存合并后的结果"""
    output_path = os.path.join(config['output_dir'], config['output_filename'])
    os.makedirs(config['output_dir'], exist_ok=True)

    pg_name = config['pgName']
    vp_prefix = get_vp_prefix(pg_name)

    # 动态生成输出列名（不包含板卡标识列）
    output_cols = [
        f'{pg_name}_ERPT_ACTIVE.*_成员名称_器件_器件编号',
        f'{vp_prefix}errRptInject[x][x][x]',
        '故障说明',
        '故障码',
        '故障说明（英文）'
    ]

    # 实际使用的列名（从DataFrame中筛选）
    actual_cols = [col for col in df.columns if any(pattern.replace('*', '') in col for pattern in output_cols)]
    actual_cols.extend(['故障说明', '故障码', '故障说明（英文）'])

    # 去重并保持顺序
    actual_cols = list(dict.fromkeys(actual_cols))

    df.to_csv(output_path, index=False, columns=actual_cols, encoding='utf-8-sig')

    # 生成统计
    total = len(df)
    with_fault_code = len(df[df['故障码'] != ''])

    print(f"结果已保存: {output_path}")
    print(f"生成统计: 总记录{total}条, 含故障码{with_fault_code}条, 缺失{total - with_fault_code}条")
    logger.info(f"结果已保存: {output_path}")
    logger.info(f"生成统计: 总记录{total}条, 含故障码{with_fault_code}条, 缺失{total - with_fault_code}条")

    # 显示各板卡记录数
    if '板卡标识' in df.columns:
        logger.info("各板卡记录分布:")
        board_stats = df['板卡标识'].value_counts()
        for board, count in board_stats.items():
            logger.info(f"  {board}: {count}条")

def main():
    """主函数"""
    if len(sys.argv) < 2:
        print("用法: python script.py <配置文件路径>")
        sys.exit(1)

    config_path = sys.argv[1]
    print("=" * 60)
    print(f"配置文件: {config_path}")

    config = load_config(config_path)

    # 设置日志
    logger, log_path = setup_logging(config)
    logger.info(f"开始处理，配置文件: {config_path}")

    # 处理所有板卡
    final_df = process_all_boards(config, logger)

    # 保存合并结果
    save_results(final_df, config, logger)

    logger.info("所有板卡处理完成")
    print(f"\n处理完成！日志文件: {log_path}")

if __name__ == '__main__':
    main()