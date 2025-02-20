import os
import csv
import random
import nbtlib
import json


# 获取所有方块id保存到csv，随机分配block
def save_id(path):
    files1 = os.listdir(path)
    block_id = []
    for file_name in files1:
        block_id.append({'id': file_name[0:-5]})

    with open('block_id.csv', 'a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['id'])
        writer.writeheader()
        for block in block_id:
            writer.writerow(block)

    copy_list1 = block_id.copy()
    for root, _, files2 in os.walk(path):
        for file in files2:
            file_path = os.path.join(root, file)
            with open(file_path,'r') as jsonfile1:
                json_data = json.load(jsonfile1)
                try:
                    if 'pools' in json_data:
                        for pool in json_data['pools']:
                            if 'entries' in pool:
                                for entry in pool['entries']:
                                    if 'name' in entry:
                                        block_id = entry['name']
                                        random_block_id = random_choice(copy_list1)
                                        entry['name'] = random_block_id
                                        with open(file_path, 'w') as jsonfile2:
                                            json.dump(json_data, jsonfile2, indent=4)
                                        print(f'===================')
                                        print(f'方块 {block_id} 掉落物已被更改为 {random_block_id}')
                except KeyError as e:
                    print(f'{block_id}Key Error', e)


# 随机抽取列表一个元素
def random_choice(lst: list):
    random.shuffle(lst)
    return lst.pop()['id']


def get_nbt_files(nbt_dir):
    nbt_file = nbtlib.load('nbt_dir')


if __name__ == '__main__':
    save_id('blocks')
    # get_nbt_files()
