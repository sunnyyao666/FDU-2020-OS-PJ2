// fs.cpp: File System

#include "sfs/fs.h"

#include <algorithm>

#include <cmath>
#include <cstdio>
#include <cstring>

// Debug file system -----------------------------------------------------------

void FileSystem::debug(Disk *disk) {
    Block block{};

    // Read superblock
    disk->read(0, block.Data);

    printf("SuperBlock:\n");

    // 验证魔数正确性
    if (block.Super.MagicNumber == MAGIC_NUMBER) {
        printf("    magic number is valid\n");
    } else {
        printf("    magic number is invalid\n");
        printf("    exiting...\n");
        return;
    }

    printf("    %u blocks\n", block.Super.Blocks);
    printf("    %u inode blocks\n", block.Super.InodeBlocks);
    printf("    %u inodes\n", block.Super.Inodes);

    // inode编号
    uint32_t n = -1;

    // Read Inode blocks
    uint32_t num_inode_blocks = block.Super.InodeBlocks;
    for (uint32_t i = 1; i <= num_inode_blocks; i++) {
        disk->read(i, block.Data); // array of inodes
        // 遍历block中的所有可能inode
        for (auto &Inode : block.Inodes) {
            n++;
            if (!Inode.Valid) {
                continue;
            }
            printf("Inode %u:\n", n);
            printf("    size: %u bytes\n", Inode.Size);

            // 处理直接索引块
            printf("    direct blocks:");
            for (uint32_t k : Inode.Direct) {
                if (k) {
                    printf(" %u", k);
                }
            }
            printf("\n");

            // 处理间接索引块
            if (!Inode.Indirect) {
                continue;
            }
            printf("    indirect block: %u\n    indirect data blocks:", Inode.Indirect);
            // 读入间接索引块内容
            Block IndirectBlock{};
            disk->read(Inode.Indirect, IndirectBlock.Data);
            // 遍历所有可能的间接索引块
            for (uint32_t Pointer : IndirectBlock.Pointers) {
                if (Pointer) {
                    printf(" %u", Pointer);
                }
            }
            printf("\n");

        }
    }
}

// Format file system ----------------------------------------------------------

bool FileSystem::format(Disk *disk) {
    // 若已挂载则不处理
    if (disk->mounted()) {
        return false;
    }

    // Write superblock
    Block block{};
    memset(&block, 0, sizeof(Block));

    block.Super.MagicNumber = FileSystem::MAGIC_NUMBER;
    block.Super.Blocks = (uint32_t) (disk->size());
    // 分配给inode的block数，十分之一向上取整
    block.Super.InodeBlocks = (uint32_t) std::ceil((block.Super.Blocks * 1.00) / 10);
    block.Super.Inodes = block.Super.InodeBlocks * (FileSystem::INODES_PER_BLOCK);
    disk->write(0, block.Data);

    // Clear all other blocks
    for (uint32_t i = 1; i < block.Super.Blocks; i++) {
        Block Empty_block{};
        memset(&Empty_block, 0, sizeof(Empty_block));
        disk->write(i, Empty_block.Data);
    }
    return true;
}

// Mount file system -----------------------------------------------------------

bool FileSystem::mount(Disk *disk) {
    // 若已挂载则不处理
    if (disk->mounted()) {
        return false;
    }

    // Read superblock
    Block block{};
    disk->read(0, block.Data);
    if (block.Super.MagicNumber != MAGIC_NUMBER) {
        return false;
    }
    if (block.Super.InodeBlocks != std::ceil((block.Super.Blocks * 1.00) / 10)) {
        return false;
    }
    if (block.Super.Inodes != (block.Super.InodeBlocks * INODES_PER_BLOCK)) {
        return false;
    }


    // Set device and mount
    disk->mount();
    cur_disk = disk;

    // Copy metadata
    MetaData = block.Super;

    // Allocate free block bitmap
    free_block_bitmap.resize(MetaData.Blocks, false);
    // 超级块已使用
    free_block_bitmap[0] = true;

    inode_counter.resize(MetaData.InodeBlocks, 0);

    // 遍历所有inode，找寻其中已经使用的block
    for (uint32_t i = 1; i <= MetaData.InodeBlocks; i++) {
        disk->read(i, block.Data);

        // 遍历所有可能的inode节点
        for (auto &Inode : block.Inodes) {
            if (!Inode.Valid) {
                continue;
            }
            inode_counter[i - 1]++;
            // 本块已使用
            free_block_bitmap[i] = true;

            // 遍历所有可能的直接索引块，找已使用了的
            for (uint32_t k : Inode.Direct) {
                if (!k) {
                    continue;
                }
                // 防溢出
                if (k >= MetaData.Blocks) {
                    return false;
                }
                // 本直接索引块已使用
                free_block_bitmap[k] = true;
            }

            // 处理间接索引
            if (!Inode.Indirect) {
                continue;
            }
            // 防溢出
            if (Inode.Indirect >= MetaData.Blocks) {
                return false;
            }
            // 间接索引块已使用
            free_block_bitmap[Inode.Indirect] = true;
            Block indirect{};
            cur_disk->read(Inode.Indirect, indirect.Data);
            for (uint32_t Pointer : indirect.Pointers) {
                // 防溢出
                if (Pointer >= MetaData.Blocks) {
                    return false;
                }
                // 间接索引块指向的目标已使用
                free_block_bitmap[Pointer] = true;
            }
        }
    }
    return true;
}

// Create inode ----------------------------------------------------------------

ssize_t FileSystem::create() {
    // 不允许未挂载就操作
    if (!cur_disk || !cur_disk->mounted()) {
        return -1;
    }

    Block block{};
    cur_disk->read(0, block.Data);

    // Locate free inode in inode table
    for (uint32_t i = 1; i <= MetaData.InodeBlocks; i++) {
        // 这个inode块中是否存在未分配inode
        if (inode_counter[i - 1] == INODES_PER_BLOCK) {
            continue;
        }

        // 这个inode块中必有空闲inode存在
        cur_disk->read(i, block.Data);

        // 遍历找到第一个
        for (uint32_t j = 0; j < INODES_PER_BLOCK; j++) {
            if (block.Inodes[j].Valid) {
                continue;
            }
            block.Inodes[j].Valid = true;
            block.Inodes[j].Size = 0;
            block.Inodes[j].Indirect = 0;
            for (uint32_t &k : block.Inodes[j].Direct) {
                k = 0;
            }
            free_block_bitmap[i] = true;
            inode_counter[i - 1]++;

            // 将更新后的数据写回磁盘
            cur_disk->write(i, block.Data);

            // Record inode if found
            return (((i - 1) * INODES_PER_BLOCK) + j);
        }
    }
    return -1;
}

// Load inode -----------------------------------------------------------------
bool FileSystem::load_inode(size_t inumber, Inode *inode) {
    // 不允许未挂载就操作
    if (!cur_disk || !cur_disk->mounted()) {
        return false;
    }
    Block block{};

    // 在第i+1块inode块的第j个位置
    int i = (int) (inumber / INODES_PER_BLOCK);
    int j = (int) (inumber % INODES_PER_BLOCK);

    // 载入对应位置的inode
    if (inode_counter[i]) {
        cur_disk->read(i + 1, block.Data);
        if (block.Inodes[j].Valid) {
            *inode = block.Inodes[j];
            return true;
        }
    }
    return false;
}

// Remove inode ----------------------------------------------------------------
bool FileSystem::remove(size_t inumber) {
    // 不允许未挂载就操作
    if (!cur_disk || !cur_disk->mounted()) {
        return -1;
    }

    // Load inode information
    Block block{};
    Inode inode{};
    if (!load_inode(inumber, &inode)) {
        return false;
    }

    inode.Valid = false;
    inode.Size = 0;

    int i = (int) (inumber / INODES_PER_BLOCK);
    int j = (int) (inumber % INODES_PER_BLOCK);

    // 如果这个inode是本块中最后一个inode，则将块状态修改为未使用
    if (--inode_counter[i] == 0) {
        free_block_bitmap[i + 1] = false;
    }

    // Free direct blocks
    for (uint32_t &k : inode.Direct) {
        free_block_bitmap[k] = false;
        k = 0;
    }

    // Free indirect blocks
    if (inode.Indirect) {
        cur_disk->read(inode.Indirect, block.Data);
        free_block_bitmap[inode.Indirect] = false;
        inode.Indirect = 0;

        for (uint32_t Pointer : block.Pointers) {
            if (Pointer) {
                free_block_bitmap[Pointer] = false;
            }
        }
    }

    // Clear inode in inode table
    cur_disk->read(i + 1, block.Data);
    block.Inodes[j] = inode;
    cur_disk->write(i + 1, block.Data);

    return true;
}

// Inode stat ------------------------------------------------------------------

ssize_t FileSystem::stat(size_t inumber) {
    if (!cur_disk || !cur_disk->mounted()) {
        return -1;
    }

    // Load inode information
    Inode inode{};

    if (load_inode(inumber, &inode)) {
        return inode.Size;
    }

    return -1;
}

// Read helper -----------------------------------------------------------------

void FileSystem::read_in_block(uint32_t blocknum, int offset, int *length, char **ptr) {
    Block block{};
    cur_disk->read(blocknum, block.Data);
    // 读取到的字节数
    uint32_t num_bytes = Disk::BLOCK_SIZE - offset;
    memcpy(*ptr, block.Data + offset, num_bytes);
    *ptr += num_bytes;
    *length -= num_bytes;
}

// Read from inode -------------------------------------------------------------

ssize_t FileSystem::read(size_t inumber, char *data, int length, size_t offset) {
    // 不允许未挂载就操作
    if (!cur_disk || !cur_disk->mounted()) {
        return -1;
    }

    // Load inode information
    ssize_t size_inode = stat(inumber);
    // 隐含了size_inode = -1的情况
    if ((int) offset >= size_inode) {
        return 0;
    } else if (length + (int) offset > size_inode) {
        length = size_inode - (int) offset;
    } // Adjust length

    Inode inode{};

    if (!load_inode(inumber, &inode)) {
        return -1;
    }

    // 下一数据保存位置
    char *ptr = data;

    // 总共需要读取的字节数
    size_t num_bytes = length;

    // Read block and copy to data
    // 起始位置在直接索引
    if (offset < POINTERS_PER_INODE * Disk::BLOCK_SIZE) {
        uint32_t direct_node = offset / Disk::BLOCK_SIZE;
        offset %= Disk::BLOCK_SIZE;

        // 无存储数据
        if (!inode.Direct[direct_node]) {
            return 0;
        }
        read_in_block(inode.Direct[direct_node], offset, &length, &ptr);
        direct_node++;
        while (length > 0 && direct_node < POINTERS_PER_INODE && inode.Direct[direct_node]) {
            read_in_block(inode.Direct[direct_node], 0, &length, &ptr);
            direct_node++;
        }

        // 已读取足够数据
        if (length <= 0) {
            return num_bytes;
        }

        // 读完了直接索引或没有间接索引
        if (direct_node != POINTERS_PER_INODE || !inode.Indirect) {
            return num_bytes - length;
        }

        // 读取间接索引中的剩余部分
        Block indirect{};
        cur_disk->read(inode.Indirect, indirect.Data);
        for (uint32_t &Pointer : indirect.Pointers) {
            if (!Pointer || length <= 0) {
                break;
            }
            read_in_block(Pointer, 0, &length, &ptr);
        }

        // 读到了足够的数据
        if (length <= 0) {
            return num_bytes;
        }

        // 间接索引也读完了
        return num_bytes - length;
    } else {
        // 起始位置在间接索引中
        if (!inode.Indirect) {
            return 0;
        }

        // 去掉直接索引的偏移量部分
        offset -= POINTERS_PER_INODE * Disk::BLOCK_SIZE;
        // 间接索引块下标
        uint32_t indirect_node = offset / Disk::BLOCK_SIZE;
        offset %= Disk::BLOCK_SIZE;

        Block indirect{};
        cur_disk->read(inode.Indirect, indirect.Data);

        // 第一块间接索引，从偏移量开始读
        if (indirect.Pointers[indirect_node] && length > 0) {
            read_in_block(indirect.Pointers[indirect_node++], offset, &length, &ptr);
        }

        // 之后的间接索引直接读取整块
        for (uint32_t i = indirect_node; i < POINTERS_PER_BLOCK; i++) {
            if (!indirect.Pointers[i] || length <= 0) {
                break;
            }
            read_in_block(indirect.Pointers[i], 0, &length, &ptr);
        }

        // 已读取足够数据
        if (length <= 0) {
            return num_bytes;
        } else {
            return num_bytes - length;
        } // 数据不足
    }
}

// Allocate a block ------------------------------------------------------------

bool FileSystem::allocate_block(uint32_t &blocknum) {
    // 不允许未挂载就操作
    if (!cur_disk || !cur_disk->mounted()) {
        return -1;
    }

    if (blocknum) {
        return true;
    }
    for (int i = (int) MetaData.InodeBlocks + 1; i < (int) MetaData.Blocks; i++) {
        if (!free_block_bitmap[i]) {
            free_block_bitmap[i] = true;
            blocknum = i;
            return true;
        }
    }
    return false;
}

// Write inode back to block ---------------------------------------------------

void FileSystem::write_inode_to_block(size_t inumber, Inode *inode) {
    // 不允许未挂载就操作
    if (!cur_disk || !cur_disk->mounted()) {
        return;
    }

    // 在第i+1块inode块的第j个位置
    int i = (int) (inumber / INODES_PER_BLOCK);
    int j = (int) (inumber % INODES_PER_BLOCK);

    Block block{};
    cur_disk->read(i + 1, block.Data);
    block.Inodes[j] = *inode;
    cur_disk->write(i + 1, block.Data);
}

// write real data to block ----------------------------------------------------

void FileSystem::write_data_to_block(int offset, int *num_bytes, int length, char *data, uint32_t blocknum) {
    // 不允许未挂载就操作
    if (!cur_disk || !cur_disk->mounted()) {
        return;
    }

    // 缓冲区用于先存储整个块大小的数据
    char *ptr = (char *) calloc(Disk::BLOCK_SIZE, sizeof(char));
    cur_disk->read(blocknum, ptr);

    // 从偏移量开始逐字节修改数据
    for (int i = offset; i < (int) Disk::BLOCK_SIZE && *num_bytes < length; i++) {
        ptr[i] = data[*num_bytes];
        *num_bytes = *num_bytes + 1;
    }
    cur_disk->write(blocknum, ptr);

    // 释放缓冲区
    free(ptr);
}


// Write to inode --------------------------------------------------------------

ssize_t FileSystem::write(size_t inumber, char *data, int length, size_t offset) {
    // 不允许未挂载就操作
    if (!cur_disk || !cur_disk->mounted()) {
        return -1;
    }

    // Load inode
    Inode inode{};
    Block indirect{};
    int num_bytes = 0;
    size_t old_offset = offset;
    size_t old_size = 0;

    int max_size = length + (int) offset;
    // 超过最大可能长度
    if (max_size > (int) ((POINTERS_PER_BLOCK + POINTERS_PER_INODE) * Disk::BLOCK_SIZE)) {
        return -1;
    }

    if (!load_inode(inumber, &inode)) {
        inode.Valid = true;
        inode.Size = max_size;
        for (uint32_t &i : inode.Direct) {
            i = 0;
        }
        inode.Indirect = 0;
        inode_counter[inumber / INODES_PER_BLOCK]++;
        free_block_bitmap[inumber / INODES_PER_BLOCK + 1] = true;
    } else {
        // 重设inode大小
        old_size = inode.Size;
        inode.Size = fmax((int) inode.Size, max_size);
    }

    // Write block and copy to data
    // 从直接索引开始写
    if (offset < POINTERS_PER_INODE * Disk::BLOCK_SIZE) {
        // 直接索引块下标
        uint32_t direct_node = offset / Disk::BLOCK_SIZE;
        offset %= Disk::BLOCK_SIZE;

        // 尝试为直接索引分配块，若磁盘已满则直接返回，下同
        if (!allocate_block(inode.Direct[direct_node])) {
            inode.Size = old_size;
            write_inode_to_block(inumber, &inode);
            return num_bytes;
        }
        // 真实写入数据
        write_data_to_block(offset, &num_bytes, length, data, inode.Direct[direct_node]);
        direct_node++;

        // 写入了足够数据
        if (num_bytes == length) {
            write_inode_to_block(inumber, &inode);
            return length;
        }

        for (int i = direct_node; i < (int) POINTERS_PER_INODE; i++) {
            // 之后的直接索引从0开始写
            if (!allocate_block(inode.Direct[direct_node])) {
                inode.Size = old_offset + num_bytes;
                write_inode_to_block(inumber, &inode);
                return num_bytes;
            }
            write_data_to_block(0, &num_bytes, length, data, inode.Direct[direct_node]);
            direct_node++;

            // 写入了足够数据
            if (num_bytes == length) {
                write_inode_to_block(inumber, &inode);
                return length;
            }
        }

        // 开始使用间接索引块
        if (inode.Indirect) {
            cur_disk->read(inode.Indirect, indirect.Data);
        } else {
            // 目前没有间接索引块，尝试分配
            if (!allocate_block(inode.Indirect)) {
                inode.Size = old_offset + num_bytes;
                write_inode_to_block(inumber, &inode);
                return num_bytes;
            }
            cur_disk->read(inode.Indirect, indirect.Data);

            // 新创建的间接索引块，先全部置0
            for (uint32_t &Pointer : indirect.Pointers) {
                Pointer = 0;
            }
        }

        for (uint32_t &Pointer : indirect.Pointers) {
            // 尝试分配间接索引块指向的数据块
            if (!allocate_block(Pointer)) {
                inode.Size = old_offset + num_bytes;
                cur_disk->write(inode.Indirect, indirect.Data);
                write_inode_to_block(inumber, &inode);
                return num_bytes;
            }
            write_data_to_block(0, &num_bytes, length, data, Pointer);


            // 写入了足够数据
            if (num_bytes == length) {
                cur_disk->write(inode.Indirect, indirect.Data);
                write_inode_to_block(inumber, &inode);
                return length;
            }
        }

        // 没有多余空间了
        cur_disk->write(inode.Indirect, indirect.Data);
        write_inode_to_block(inumber, &inode);
        return num_bytes;
    } else { // 从间接索引开始写
        // 先去掉直接索引块的偏移量部分
        offset -= POINTERS_PER_INODE * Disk::BLOCK_SIZE;
        // 计算间接索引块下标
        uint32_t indirect_node = offset / Disk::BLOCK_SIZE;
        offset %= Disk::BLOCK_SIZE;

        // 同上
        if (inode.Indirect) {
            cur_disk->read(inode.Indirect, indirect.Data);
        } else {
            // 目前没有间接索引块，尝试分配
            if (!allocate_block(inode.Indirect)) {
                inode.Size = old_size;
                write_inode_to_block(inumber, &inode);
                return num_bytes;
            }
            cur_disk->read(inode.Indirect, indirect.Data);

            // 新创建的间接索引块，先全部置0
            for (uint32_t &Pointer : indirect.Pointers) {
                Pointer = 0;
            }
        }

        if (!allocate_block(indirect.Pointers[indirect_node])) {
            inode.Size = old_size;
            cur_disk->write(inode.Indirect, indirect.Data);
            write_inode_to_block(inumber, &inode);
            return num_bytes;
        }
        write_data_to_block(offset, &num_bytes, length, data, indirect.Pointers[indirect_node]);
        indirect_node++;

        // 写入了足够数据
        if (num_bytes == length) {
            cur_disk->write(inode.Indirect, indirect.Data);
            write_inode_to_block(inumber, &inode);
            return length;
        }

        for (int i = indirect_node; i < (int) POINTERS_PER_BLOCK; i++) {
            // 尝试分配间接索引指向的块
            if (!allocate_block(indirect.Pointers[i])) {
                inode.Size = old_offset + num_bytes;
                cur_disk->write(inode.Indirect, indirect.Data);
                write_inode_to_block(inumber, &inode);
                return num_bytes;
            }
            write_data_to_block(0, &num_bytes, length, data, indirect.Pointers[i]);

            // 写入了足够数据
            if (num_bytes == length) {
                cur_disk->write(inode.Indirect, indirect.Data);
                write_inode_to_block(inumber, &inode);
                return length;
            }
        }

        // 没有多余空间了
        cur_disk->write(inode.Indirect, indirect.Data);
        write_inode_to_block(inumber, &inode);
        return num_bytes;
    }
}
