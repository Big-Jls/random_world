import os
import csv
import json
import random
import logging
from typing import List, Dict, Set, Union
from concurrent.futures import ThreadPoolExecutor  # 可选多线程
from nbtlib import load as nbt_load, File, Compound, List as NBTList, schema
from nbtlib.tag import String, Int
# import gzip
# import magic

# ====================
# 基础配置
# ====================
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(name)s - %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
# 可选黑名单
BLACKLIST = {
    'bedrock', 'trial_spawner','spawner', 'command_block', 'water', 'lava', 'white_candle', 'light_gray_candle', 'gray_candle',
    'black_candle', 'brown_candle', 'red_candle', 'orange_candle', 'yellow_candle', 'lime_candle',
    'green_candle', 'cyan_candle', 'light_blue_candle', 'blue_candle', 'purple_candle', 'magenta_candle',
    'pink_candle', 'white_bed', 'light_gray_bed', 'gray_bed',
    'black_bed', 'brown_bed', 'red_bed', 'orange_bed', 'yellow_bed', 'lime_bed',
    'green_bed', 'cyan_bed', 'light_blue_bed', 'blue_bed', 'purple_bed', 'magenta_bed',
    'pink_bed', 'white_carpet', 'light_gray_carpet', 'gray_carpet',
    'black_carpet', 'brown_carpet', 'red_carpet', 'orange_carpet', 'yellow_carpet', 'lime_carpet',
    'green_carpet', 'cyan_carpet', 'light_blue_carpet', 'blue_carpet', 'purple_carpet', 'magenta_carpet',
    'pink_carpet', 'white_banner', 'light_gray_banner', 'gray_banner',
    'black_banner', 'brown_banner', 'red_banner', 'orange_banner', 'yellow_banner', 'lime_banner',
    'green_banner', 'cyan_banner', 'light_blue_banner', 'blue_banner', 'purple_banner', 'magenta_banner',
    'pink_banner', 'iron_door', 'copper_door', 'exposed_copper_door', 'weathered_copper_door', 'waxed_copper_door',
    'oxidized_copper_door', 'waxed_exposed_door', 'waxed_weathered_copper_door',
    'waxed_oxidized_copper_door', 'oak_button', 'birch_button', 'spruce_button', 'jungle_button', 'acacia_button',
    'dark_oak_button', 'mangrove_button', 'cherry_button', 'pale_oak_button', 'bamboo_button', 'crimson_button',
    'warped_button', 'stone_button', 'polished_blackstone_button', 'oak_door', 'birch_door', 'spruce_door',
    'jungle_door', 'acacia_door', 'dark_oak_door', 'mangrove_door', 'cherry_door', 'pale_oak_door', 'bamboo_door',
    'crimson_door', 'warped_door', 'snow', 'moss_carpet', 'pale_moss_carpet', 'pale_hanging_moss', 'pointed_dripstone',
    'oak_sapling', 'birch_sapling', 'spruce_sapling', 'jungle_sapling', 'acacia_sapling','dark_oak_sapling',
    'mangrove_propagule', 'cherry_sapling', 'pale_oak_sapling', 'azalea', 'flowering_azalea', 'brown_mushroom',
    'red_mushroom', 'crimson_fungus', 'warped_fungus', 'short_grass', 'fern', 'dead_bush', 'dandelion', 'poppy',
    'blue_orchid', 'allium', 'azure_bluet', 'red_tulip', 'orange_tulip', 'white_tulip', 'pink_tulip', 'oxeye_daisy',
    'cornflower', 'lily_of_the_valley', 'torchflower', 'closed_eyeblossom', 'open_eyeblossom', 'wither_rose',
    'pink_petals','spore_blossom', 'bamboo', 'sugar_cane', 'cactus', 'crimson_roots', 'warped_roots', 'weeping_vines',
    'twisting_vines', 'vine', 'tall_grass', 'large_fern','sunflower', 'lilac', 'rose_bush', 'peony', 'pitcher_plant',
    'big_dripleaf', 'small_dripleaf', 'glow_lichen', 'hanging_roots','glow_berries', 'sweet_berries','nether_wart',
    'lily_pad', 'seagrass', 'sea_pickle', 'kelp', 'tube_coral', 'brain_coral', 'bubble_coral',
    'fire_coral', 'horn_coral', 'tube_coral_fan', 'brain_coral_fan','bubble_coral_fan',
    'fire_coral_fan', 'horn_coral_fan', 'dead_tube_coral', 'dead_brain_coral', 'dead_bubble_coral',
    'dead_fire_coral', 'dead_horn_coral', 'dead_tube_coral_fan', 'dead_brain_coral_fan','dead_bubble_coral_fan',
    'dead_fire_coral_fan', 'dead_horn_coral_fan', 'torch', 'soul_torch', 'redstone_torch', 'oak_sign', 'birch_sign',
    'spruce_sign','jungle_sign', 'acacia_sign', 'dark_oak_sign', 'mangrove_sign', 'cherry_sign', 'pale_oak_sign',
    'bamboo_sign', 'vault', 'redstone', 'repeater', 'comparator', 'lever', 'oak_pressure_plate', 'birch_pressure_plate',
    'spruce_pressure_plate','jungle_pressure_plate', 'acacia_pressure_plate', 'dark_oak_pressure_plate',
    'mangrove_pressure_plate', 'cherry_pressure_plate', 'pale_oak_pressure_plate', 'bamboo_pressure_plate',
    'light_weighted_pressure_plate', 'heavy_weighted_pressure_plate', 'tripwire_hook', 'rail', 'powered_rail',
    'detector_rail', 'activator_rail'
}
# 可选优先级权重
PRIORITY_BLOCKS = {'chest': 3}


# ====================
# 主处理类
# ====================
class MinecraftWorldRandomizer:
    def __init__(self, block_csv: str = 'block_id.csv'):
        self.block_ids = self._load_block_ids(block_csv)
        self.used_ids: Set[str] = set()
        self.reset_count = 0
        self.logger = logging.getLogger('MinecraftRandomizer')
        self.executor = ThreadPoolExecutor(max_workers=4)  # 可选多线程

    # ====================
    # 核心方法
    # ====================
    def randomize_worldgen(self, worldgen_path: str):
        """处理世界生成JSON文件"""
        self._process_directory(worldgen_path, processor=self._process_json_file)

    def randomize_structures(self, structures_path: str):
        """处理结构NBT文件"""
        self._process_directory(structures_path, processor=self._process_nbt_file)

    # ====================
    # 文件处理逻辑
    # ====================
    def _process_directory(self, path: str, processor: callable):
        """通用目录处理器"""
        if not os.path.exists(path):
            self.logger.error(f"路径不存在: {path}")
            return

        for root, _, files in os.walk(path):
            for file in files:
                file_path = os.path.join(root, file)
                # 可选：提交到线程池
                self.executor.submit(processor, file_path)
                # 或直接处理
                # processor(file_path)

    def _process_json_file(self, file_path: str):
        """处理JSON文件（生物群系/特性）"""
        if not file_path.endswith('.json'):
            return

        try:
            with open(file_path, 'r+', encoding='utf-8') as f:
                data = json.load(f)
                local_used = set()
                self._modify_json(data, local_used)
                f.seek(0)
                json.dump(data, f, indent=2, ensure_ascii=False)
                f.truncate()
                self.logger.info(f"JSON处理完成: {file_path}")

        except Exception as e:
            self.logger.error(f"JSON处理失败 [{file_path}]: {str(e)}")

    def _process_nbt_file(self, file_path: str):
        """改进后的NBT文件处理器"""
        if not file_path.endswith('.nbt'):
            return

        try:
            # 规范化路径处理
            normalized_path = os.path.normpath(file_path)
            if not os.path.exists(normalized_path):
                self.logger.error(f"文件不存在: {normalized_path}")
                return

            # 自动检测文件压缩格式
            is_gzipped = self._detect_gzip(normalized_path)
            self.logger.debug(f"文件压缩检测 [{normalized_path}]: {'gzip' if is_gzipped else '未压缩'}")

            # 加载NBT数据
            with nbt_load(normalized_path, gzipped=is_gzipped) as nbt_data:
                self.logger.debug(f"成功加载NBT文件结构: {nbt_data.keys()}")

                # 深度查找palette并记录路径
                palette, palette_path = self._find_palette_with_path(nbt_data)
                if palette:
                    self.logger.info(f"定位到palette列表 [路径: {palette_path}]")
                    self._modify_palette(palette, normalized_path)
                    nbt_data.save()
                else:
                    self.logger.warning(f"未找到有效palette列表: {normalized_path}")

        except Exception as e:
            self.logger.error(f"NBT处理深度失败 [{normalized_path}]\n{repr(e)}", exc_info=True)

    def _detect_gzip(self, file_path: str) -> bool:
        """精确检测GZIP压缩格式"""
        try:
            with open(file_path, 'rb') as f:
                return f.read(2) == b'\x1f\x8b'
        except Exception as e:
            self.logger.error(f"文件头检测失败 [{file_path}]: {str(e)}")
            return False

    def _find_palette_with_path(self, node: Compound, current_path: str = '') -> (Union[NBTList, None], str):
        """带路径追踪的palette定位器"""
        if isinstance(node, Compound):
            for key in node.keys():
                child_path = f"{current_path}/{key}"
                if key == 'palette' and isinstance(node[key], NBTList):
                    return node[key], child_path
                result, found_path = self._find_palette_with_path(node[key], child_path)
                if result:
                    return result, found_path
        elif isinstance(node, NBTList):
            for i, item in enumerate(node):
                result, found_path = self._find_palette_with_path(item, f"{current_path}[{i}]")
                if result:
                    return result, found_path
        return None, current_path

    def _modify_palette(self, palette: NBTList, file_path: str):
        """带类型校验的palette修改器"""
        local_used = set()
        modified_count = 0

        for idx, block_state in enumerate(palette):
            try:
                if not isinstance(block_state, Compound):
                    self.logger.warning(f"非法block_state类型 [{file_path} 索引{idx}]: {type(block_state)}")
                    continue

                if 'Name' not in block_state:
                    self.logger.debug(f"跳过无Name字段的block_state [{file_path} 索引{idx}]")
                    continue

                original_name = str(block_state['Name'])
                new_name = self._get_random_id(local_used)

                # 类型安全修改
                if not isinstance(block_state['Name'], String):
                    self.logger.warning(
                        f"强制转换Name字段类型 [{file_path} 索引{idx}] 原类型: {type(block_state['Name'])}")
                    block_state['Name'] = String(new_name)
                else:
                    block_state['Name'] = String(new_name)

                modified_count += 1
                self.logger.debug(f"替换记录 [{file_path}]: {original_name} → {new_name}")

            except Exception as e:
                self.logger.error(f"block_state处理失败 [{file_path} 索引{idx}]: {str(e)}")

        self.logger.info(f"成功修改 {modified_count}/{len(palette)} 个方块状态 [{file_path}]")

    # ====================
    # 数据处理逻辑
    # ====================
    def _modify_json(self, data: Union[dict, list], used_set: Set[str]):
        """递归修改JSON数据"""
        if isinstance(data, dict):
            for key in list(data.keys()):
                if key in ["Name", "name"]:
                    data[key] = self._get_random_id(used_set)
                else:
                    self._modify_json(data[key], used_set)
        elif isinstance(data, list):
            for item in data:
                self._modify_json(item, used_set)

    def _find_palette(self, nbt_data: Compound) -> Union[NBTList, None]:
        """定位结构文件中的palette列表"""
        # 直接访问常见路径
        if 'palette' in nbt_data:
            return nbt_data['palette']
        if 'structure' in nbt_data and 'palette' in nbt_data['structure']:
            return nbt_data['structure']['palette']

        # 深度搜索（兼容非标准结构）
        def recursive_search(node):
            if isinstance(node, Compound):
                for k, v in node.items():
                    if k == 'palette' and isinstance(v, NBTList):
                        return v
                    result = recursive_search(v)
                    if result:
                        return result
            elif isinstance(node, NBTList):
                for item in node:
                    result = recursive_search(item)
                    if result:
                        return result
            return None

        return recursive_search(nbt_data)

    # ====================
    # ID管理模块
    # ====================
    def _load_block_ids(self, csv_path: str) -> List[str]:
        """加载并预处理方块ID"""
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                ids = []
                for row in csv.DictReader(f):
                    if 'id' not in row:
                        continue
                    block_id = row['id']

                    # 应用黑名单过滤
                    if block_id in BLACKLIST:
                        continue

                    # 应用优先级权重（可选）
                    repeat = PRIORITY_BLOCKS.get(block_id, 1)
                    ids.extend([block_id] * repeat)

                random.shuffle(ids)
                return ids

        except FileNotFoundError:
            self.logger.critical(f"方块ID文件不存在: {csv_path}")
            return []

    def _get_random_id(self, used_set: Set[str]) -> str:
        """获取随机不重复ID"""
        available = [id for id in self.block_ids if id not in used_set and id not in self.used_ids]

        # ID池重置逻辑
        if not available:
            self.logger.warning("ID池耗尽，重置并重新洗牌")
            used_set.clear()
            self.used_ids.clear()
            random.shuffle(self.block_ids)
            available = self.block_ids.copy()
            self.reset_count += 1

        chosen = available.pop(0)
        used_set.add(chosen)
        self.used_ids.add(chosen)
        return f"minecraft:{chosen}"

    # ====================
    # 清理资源
    # ====================
    def __del__(self):
        self.executor.shutdown(wait=True)
        self.logger.info(f"任务完成 | 总重置次数: {self.reset_count}")


# ====================
# 使用示例
# ====================
if __name__ == '__main__':
    # 初始化处理器
    randomizer = MinecraftWorldRandomizer(block_csv='block_id.csv')

    # 处理世界生成配置（JSON）
    randomizer.randomize_worldgen(r'.\blocks')

    # 处理世界生成配置（JSON）
    randomizer.randomize_worldgen(r'.\nbt_dir\worldgen')

    # 处理结构文件（NBT）
    randomizer.randomize_structures(r'.\nbt_dir\structure')

    # 可选：处理其他自定义路径
    # randomizer.randomize_worldgen(r'.\datapacks\my_pack\data')
