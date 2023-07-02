// fs.h: File System

#pragma once

#include "sfs/disk.h"

#include <cstdint>
#include <vector>
#include <sys/types.h>

class FileSystem {
public:
    const static uint32_t MAGIC_NUMBER = 0xf0f03410;
    const static uint32_t INODES_PER_BLOCK = 128;
    const static uint32_t POINTERS_PER_INODE = 5;
    const static uint32_t POINTERS_PER_BLOCK = 1024;

private:
    struct SuperBlock {        // Superblock structure
        uint32_t MagicNumber;    // File system magic number
        uint32_t Blocks;    // Number of blocks in file system
        uint32_t InodeBlocks;    // Number of blocks reserved for inodes
        uint32_t Inodes;    // Number of inodes in file system
    };

    struct Inode {
        uint32_t Valid;        // Whether or not inode is valid
        uint32_t Size;        // Size of file
        uint32_t Direct[POINTERS_PER_INODE]; // Direct pointers
        uint32_t Indirect;    // Indirect pointer
    };

    union Block {
        SuperBlock Super;                // Superblock
        Inode Inodes[INODES_PER_BLOCK];        // Inode block
        uint32_t Pointers[POINTERS_PER_BLOCK];   // Pointer block
        char Data[Disk::BLOCK_SIZE];        // Data block
    };

    // Internal helper functions
    bool load_inode(size_t inumber, Inode *inode);

    void read_in_block(uint32_t blocknum, int offset, int *length, char **ptr);

    bool allocate_block(uint32_t &blocknum);

    void write_inode_to_block(size_t inumber, Inode *inode);

    void write_data_to_block(int offset, int *num_bytes, int length, char *data, uint32_t blocknum);

    // Internal member variables
    Disk *cur_disk; // 当前选定磁盘
    struct SuperBlock MetaData; // 超级块信息
    std::vector<bool> free_block_bitmap; // 空闲块列表
    std::vector<int> inode_counter; // 记录每个inode块中已使用的inode数量

public:
    static void debug(Disk *disk);

    static bool format(Disk *disk);

    bool mount(Disk *disk);

    ssize_t create();

    bool remove(size_t inumber);

    ssize_t stat(size_t inumber);

    ssize_t read(size_t inumber, char *data, int length, size_t offset);

    ssize_t write(size_t inumber, char *data, int length, size_t offset);
};
